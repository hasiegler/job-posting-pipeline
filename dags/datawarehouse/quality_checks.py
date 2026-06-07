"""
Warn-only post-load data-quality checks — the final DAG step.

Runs after the jobs table, ``company_run_metrics`` and ``pipeline_runs`` have
been loaded for the current run.  Executes a battery of absolute, regression
and soft checks, folds every finding into a SINGLE Telegram summary via
``send_alert``, and always sends exactly one message per successful run:

  * a "[QC WARNINGS]" summary when ≥1 check tripped, or
  * a "[QC PASS]" confirmation when everything is clean.

This task NEVER blocks and NEVER crashes the DAG: each individual check is
wrapped (a failing check is logged and skipped) and the whole task is wrapped,
so a QC bug can't fail the pipeline.

Baseline semantics mirror the Ashby completeness guard: "successful / non-poison"
runs are those with ``scraped_jobs > 0`` (company level) or
``status = 'success' AND total_scraped > 0`` (run level), always excluding the
current run.  If there are 0 prior runs the regression checks are skipped.
"""

import logging

try:
    from airflow.decorators import task
    from airflow.operators.python import get_current_context
except ImportError:
    def task(func):
        func.function = func
        return func

    def get_current_context():
        return {}

from alerting import send_alert
from datawarehouse.data_utils import run_with_db

logger = logging.getLogger(__name__)

# -- Thresholds -------------------------------------------------------------
NULL_RATE_THRESHOLD = 0.01          # >1% null/empty on a never-null field
FILL_RATE_DROP_FACTOR = 0.5         # alert if current < 50% of trailing avg
COUNT_REGRESSION_FACTOR = 0.6       # alert if current < 60% of avg (>40% drop)
PER_COMPANY_DROP_FACTOR = 0.5       # alert if company < 50% of its avg (>50% drop)
MIN_BASELINE_FOR_PCT = 10           # small-sample guard (mirrors Ashby check)
BASELINE_RUNS = 7                   # trailing window
SALARY_MIN_PLAUSIBLE = 1000         # plausible annual floor
SALARY_MAX_PLAUSIBLE = 10_000_000   # plausible annual ceiling
RECENT_POSTING_DAYS = 4
MAX_COMPANY_LINES = 15              # cap per-company anomalies in the message
NULL_RATE_TOP_N = 3                 # top-N companies shown per tripping null-rate field

# Fields that should essentially never be null/empty.  `location` treats the
# Greenhouse "Unknown" default as missing; `departments` treats an empty array
# as missing (this is the warn-only surfacing of the departments check).
NEVER_NULL_FIELDS = (
    "title", "location", "source_job_id",
    "source_url", "description_text", "departments",
)

# Salary-sanity predicate.  The plausible-range bounds apply ONLY to yearly
# salaries — an hourly rate (e.g. $50/hr) is legitimately < 1000.
_SALARY_BAD_SQL = """
    is_active = TRUE AND (
          (salary_min IS NOT NULL AND salary_min <= 0)
       OR (salary_min IS NOT NULL AND salary_max IS NOT NULL AND salary_max < salary_min)
       OR (salary_period = 'yearly' AND salary_min IS NOT NULL
           AND (salary_min < %(lo)s OR salary_min > %(hi)s))
       OR (salary_period = 'yearly' AND salary_max IS NOT NULL
           AND (salary_max < %(lo)s OR salary_max > %(hi)s))
    )
"""


# ---------------------------------------------------------------------------
# Absolute checks
# ---------------------------------------------------------------------------
def _check_salary_sanity(cur) -> list[str]:
    params = {"lo": SALARY_MIN_PLAUSIBLE, "hi": SALARY_MAX_PLAUSIBLE}
    cur.execute(f"SELECT COUNT(*) AS n FROM jobs WHERE {_SALARY_BAD_SQL}", params)
    n = cur.fetchone()["n"]
    if not n:
        return []
    # Per-company breakdown, descending by count, capped at 10 rows.
    cur.execute(
        f"""
        SELECT c.company_name, COUNT(*) AS cnt
        FROM jobs j
        JOIN companies c ON c.company_id = j.company_id
        WHERE {_SALARY_BAD_SQL}
        GROUP BY c.company_name
        ORDER BY cnt DESC
        LIMIT 10
        """,
        params,
    )
    rows = cur.fetchall()
    lines = [f"Salary sanity: {n} implausible row(s) by company:"]
    for r in rows:
        lines.append(f"  {r['company_name']}: {r['cnt']}")
    shown = sum(r["cnt"] for r in rows)
    if shown < n:
        lines.append(f"  … +{n - shown} more")
    return ["\n".join(lines)]


def _check_duplicates(cur) -> list[str]:
    # NOTE: jobs has UNIQUE(company_id, source_job_id), so this is effectively a
    # guard against that constraint being dropped — normally always 0.
    cur.execute(
        """
        SELECT c.company_name, j.source_job_id, COUNT(*) AS cnt
        FROM jobs j
        JOIN companies c ON c.company_id = j.company_id
        GROUP BY c.company_name, j.source_job_id
        HAVING COUNT(*) > 1
        LIMIT 10
        """
    )
    dups = cur.fetchall()
    if not dups:
        return []
    sample = [f"{d['company_name']} / {d['source_job_id']}" for d in dups[:3]]
    return [f"Duplicate (company, source_job_id): {len(dups)}+ group(s) e.g. {sample}"]


def _check_null_rates(cur) -> list[str]:
    # Step 1: pipeline-wide totals.
    cur.execute(
        """
        SELECT
          COUNT(*) AS total,
          COUNT(*) FILTER (WHERE title IS NULL OR title = '')                              AS title,
          COUNT(*) FILTER (WHERE location IS NULL OR location = '' OR location = 'Unknown') AS location,
          COUNT(*) FILTER (WHERE source_job_id IS NULL OR source_job_id = '')              AS source_job_id,
          COUNT(*) FILTER (WHERE source_url IS NULL OR source_url = '')                    AS source_url,
          COUNT(*) FILTER (WHERE description_text IS NULL OR description_text = '')        AS description_text,
          COUNT(*) FILTER (WHERE departments IS NULL OR cardinality(departments) = 0)      AS departments
        FROM jobs
        WHERE is_active = TRUE
        """
    )
    row = cur.fetchone()
    total = row["total"] or 0
    if total == 0:
        return []

    tripping = {f: row[f] for f in NEVER_NULL_FIELDS if row[f] / total > NULL_RATE_THRESHOLD}
    if not tripping:
        return []

    # Step 2: per-company breakdown (one query, all fields).  Only run when
    # something trips — this is the slow path.
    cur.execute(
        """
        SELECT
          c.company_name,
          COUNT(*) FILTER (WHERE j.title IS NULL OR j.title = '')                              AS title,
          COUNT(*) FILTER (WHERE j.location IS NULL OR j.location = '' OR j.location = 'Unknown') AS location,
          COUNT(*) FILTER (WHERE j.source_job_id IS NULL OR j.source_job_id = '')              AS source_job_id,
          COUNT(*) FILTER (WHERE j.source_url IS NULL OR j.source_url = '')                    AS source_url,
          COUNT(*) FILTER (WHERE j.description_text IS NULL OR j.description_text = '')        AS description_text,
          COUNT(*) FILTER (WHERE j.departments IS NULL OR cardinality(j.departments) = 0)      AS departments
        FROM jobs j
        JOIN companies c ON c.company_id = j.company_id
        WHERE j.is_active = TRUE
        GROUP BY c.company_name
        """
    )
    company_rows = cur.fetchall()

    out = []
    for field, n in tripping.items():
        rate = n / total
        top = sorted(
            [(r["company_name"], r[field]) for r in company_rows if r[field] > 0],
            key=lambda x: x[1], reverse=True,
        )[:NULL_RATE_TOP_N]
        top_str = "    " + ", ".join(f"{name} ({cnt})" for name, cnt in top)
        out.append(f"Null-rate {field}: {rate * 100:.1f}% ({n}/{total})\n{top_str}")
    return out


# ---------------------------------------------------------------------------
# Soft check
# ---------------------------------------------------------------------------
def _check_recent_postings(cur) -> list[str]:
    cur.execute(
        "SELECT COUNT(*) AS n FROM jobs "
        "WHERE first_published_at >= NOW() - make_interval(days => %s)",
        (RECENT_POSTING_DAYS,),
    )
    if cur.fetchone()["n"] == 0:
        return [
            f"Recent postings: 0 jobs first_published in last "
            f"{RECENT_POSTING_DAYS} days (soft — maybe nothing fresh captured)"
        ]
    return []


# ---------------------------------------------------------------------------
# Run-level regression checks (baseline from pipeline_runs)
# ---------------------------------------------------------------------------
def _check_run_regressions(cur, dag_id: str, run_id: str) -> list[str]:
    out: list[str] = []

    cur.execute(
        """
        SELECT total_scraped,
               salary_coverage_pct, remote_coverage_pct, skills_coverage_pct
        FROM pipeline_runs WHERE dag_id = %s AND run_id = %s
        """,
        (dag_id, run_id),
    )
    cur_run = cur.fetchone()
    if not cur_run or not cur_run["total_scraped"]:
        return out

    cur.execute(
        """
        SELECT total_scraped,
               salary_coverage_pct, remote_coverage_pct, skills_coverage_pct
        FROM pipeline_runs
        WHERE dag_id = %s AND status = 'success'
          AND total_scraped > 0 AND run_id <> %s
        ORDER BY run_started_at DESC
        LIMIT %s
        """,
        (dag_id, run_id, BASELINE_RUNS),
    )
    base = cur.fetchall()
    if not base:
        return out

    # Count regression: total scraped this run vs trailing average.
    avg_scraped = sum(r["total_scraped"] for r in base) / len(base)
    if cur_run["total_scraped"] < avg_scraped * COUNT_REGRESSION_FACTOR:
        drop = (1 - cur_run["total_scraped"] / avg_scraped) * 100
        out.append(
            f"Count regression: {cur_run['total_scraped']} scraped vs avg "
            f"{avg_scraped:.0f} over {len(base)} run(s) (-{drop:.0f}%)"
        )

    # Coverage regressions: % of active jobs with field populated, vs trailing
    # average.  Uses stored coverage_pct columns (computed from jobs table at
    # finalize time) — not extractor hit counters, so Ashby API-sourced values
    # and Greenhouse metadata-sourced values are all counted correctly.
    # NULLs in prior runs (before the columns were added) are excluded from the
    # baseline so the first post-migration run doesn't false-positive.
    for field, label in (
        ("salary_coverage_pct", "salary"),
        ("remote_coverage_pct", "remote"),
        ("skills_coverage_pct", "skills"),
    ):
        cur_pct = cur_run[field]
        if cur_pct is None:
            continue
        base_vals = [r[field] for r in base if r[field] is not None]
        if not base_vals:
            continue
        base_avg = sum(base_vals) / len(base_vals)
        if base_avg > 0 and float(cur_pct) < float(base_avg) * FILL_RATE_DROP_FACTOR:
            drop = (1 - float(cur_pct) / float(base_avg)) * 100
            out.append(
                f"Coverage drop {label}: {float(cur_pct):.1f}% vs avg "
                f"{float(base_avg):.1f}% (-{drop:.0f}%)"
            )
    return out


# ---------------------------------------------------------------------------
# Per-company regression checks (baseline from company_run_metrics)
# ---------------------------------------------------------------------------
def _check_per_company(cur, dag_id: str, run_id: str) -> list[str]:
    cur.execute(
        """
        SELECT company_id, scraped_jobs
        FROM company_run_metrics WHERE dag_id = %s AND run_id = %s
        """,
        (dag_id, run_id),
    )
    current = {r["company_id"]: (r["scraped_jobs"] or 0) for r in cur.fetchall()}

    # Trailing per-company baseline: last N successful (scraped_jobs > 0) runs,
    # excluding the current run.  Any company appearing here had >0 in its most
    # recent successful run, which is exactly the "had jobs before" condition —
    # persistently-zero companies never appear, so they are never flagged.
    cur.execute(
        """
        SELECT company_id, company_name, AVG(scraped_jobs) AS avg_scraped, COUNT(*) AS n
        FROM (
            SELECT company_id, company_name, scraped_jobs,
                   ROW_NUMBER() OVER (
                       PARTITION BY company_id ORDER BY run_started_at DESC
                   ) AS rn
            FROM company_run_metrics
            WHERE dag_id = %s AND scraped_jobs > 0 AND run_id <> %s
        ) t
        WHERE rn <= %s
        GROUP BY company_id, company_name
        """,
        (dag_id, run_id, BASELINE_RUNS),
    )
    baselines = cur.fetchall()

    anomalies: list[str] = []
    for b in baselines:
        name = b["company_name"]
        avg = float(b["avg_scraped"])
        cur_scraped = current.get(b["company_id"], 0)

        # Zero-that-had-jobs (also catches companies skipped this run, which
        # simply have no current row → treated as 0).
        if cur_scraped == 0:
            anomalies.append(f"{name} 0 jobs (avg {avg:.0f})")
            continue

        # Count regression — skip tiny baselines (noise at low counts).
        if avg >= MIN_BASELINE_FOR_PCT and cur_scraped < avg * PER_COMPANY_DROP_FACTOR:
            drop = (1 - cur_scraped / avg) * 100
            anomalies.append(f"{name} dropped {drop:.0f}% ({cur_scraped} vs avg {avg:.0f})")

    return anomalies


# ---------------------------------------------------------------------------
# Health checks (stale jobs, enabled-but-empty companies)
# ---------------------------------------------------------------------------
def _check_stale_active_jobs(cur) -> list[str]:
    """Flag active jobs whose last_seen is >4 days old.

    Normally every scrape touches last_seen for all of a company's active jobs.
    A stale last_seen means that company's scrapes have been silently returning
    sentinel results (no staging insert) for 4+ days, so its jobs were never
    closed by the pipeline's close-jobs logic either.
    """
    cur.execute(
        """
        SELECT
            c.company_name,
            COUNT(*) AS cnt,
            EXTRACT(DAY FROM NOW() - MAX(j.last_seen))::INT AS days_since
        FROM jobs j
        JOIN companies c ON c.company_id = j.company_id
        WHERE j.is_active = TRUE
          AND j.last_seen < NOW() - INTERVAL '4 days'
        GROUP BY c.company_name
        ORDER BY cnt DESC
        LIMIT 10
        """
    )
    rows = cur.fetchall()
    if not rows:
        return []
    total = sum(r["cnt"] for r in rows)
    lines = [f"Stale active jobs: {total}+ active jobs not seen in >4 days:"]
    for r in rows:
        lines.append(
            f"    {r['company_name']}: {r['cnt']} jobs ({r['days_since']}d ago)"
        )
    return ["\n".join(lines)]


def _check_enabled_zero_jobs(cur) -> list[str]:
    """Flag enabled companies that have 0 active jobs but have had jobs before.

    Skips brand-new companies (those with no historical jobs at all) so a
    freshly added company that hasn't been scraped yet doesn't false-positive.
    """
    cur.execute(
        """
        SELECT c.company_name
        FROM companies c
        WHERE c.enabled = TRUE
          AND NOT EXISTS (
              SELECT 1 FROM jobs j
              WHERE j.company_id = c.company_id AND j.is_active = TRUE
          )
          AND EXISTS (
              SELECT 1 FROM jobs j
              WHERE j.company_id = c.company_id
          )
        ORDER BY c.company_name
        LIMIT 20
        """
    )
    rows = cur.fetchall()
    if not rows:
        return []
    names = [r["company_name"] for r in rows]
    lines = [f"Enabled companies with 0 active jobs ({len(names)}):"]
    lines.extend(f"    {n}" for n in names)
    return ["\n".join(lines)]


# ---------------------------------------------------------------------------
# Message assembly
# ---------------------------------------------------------------------------
def _build_message(run_id, pipeline_warnings: list[str], company_anomalies: list[str]) -> str:
    run_label = run_id or "(manual)"
    if not pipeline_warnings and not company_anomalies:
        return f"[QC PASS] {run_label}: all quality checks passed."

    lines = [f"[QC WARNINGS] {run_label}:"]

    if pipeline_warnings:
        lines.append("\nPipeline-wide:")
        for w in pipeline_warnings:
            # Warnings may be multi-line (e.g. salary sanity breakdown).
            # First line gets the "- " bullet; continuation lines are indented.
            first, *rest = w.split("\n")
            lines.append(f"- {first}")
            lines.extend(rest)  # already indented by the check function

    if company_anomalies:
        shown = company_anomalies[:MAX_COMPANY_LINES]
        extra = len(company_anomalies) - len(shown)
        lines.append(f"\nPer-company anomalies ({len(company_anomalies)}):")
        lines.extend(f"- {a}" for a in shown)
        if extra:
            lines.append(f"  (+{extra} more)")

    return "\n".join(lines)


@task
def run_quality_checks() -> dict:
    """Final warn-only QC pass.  Sends exactly one Telegram summary per run."""
    try:
        context = get_current_context()
        dag = context.get("dag")
        dag_run = context.get("dag_run")
        dag_id = getattr(dag, "dag_id", "company_json_scraper")
        run_id = getattr(dag_run, "run_id", None)

        pipeline_warnings: list[str] = []
        company_anomalies: list[str] = []

        # Each check runs on its own short-lived connection so a failure in one
        # (e.g. an aborted transaction) can never poison the others.
        def _safe(label, fn):
            try:
                return run_with_db(lambda conn, cur: fn(cur))
            except Exception as e:  # noqa: BLE001 — warn-only, never crash
                logger.warning("QC check %s failed: %s", label, e)
                return []

        pipeline_warnings += _safe("salary_sanity", _check_salary_sanity)
        pipeline_warnings += _safe("duplicates", _check_duplicates)
        pipeline_warnings += _safe("null_rates", _check_null_rates)
        pipeline_warnings += _safe("recent_postings", _check_recent_postings)
        pipeline_warnings += _safe("stale_active_jobs", _check_stale_active_jobs)
        pipeline_warnings += _safe("enabled_zero_jobs", _check_enabled_zero_jobs)

        if run_id:
            try:
                pipeline_warnings += run_with_db(
                    lambda conn, cur: _check_run_regressions(cur, dag_id, run_id)
                )
            except Exception as e:  # noqa: BLE001
                logger.warning("QC run-regression failed: %s", e)
            try:
                company_anomalies += run_with_db(
                    lambda conn, cur: _check_per_company(cur, dag_id, run_id)
                )
            except Exception as e:  # noqa: BLE001
                logger.warning("QC per-company failed: %s", e)

        message = _build_message(run_id, pipeline_warnings, company_anomalies)
        send_alert(message)
        print(f"  QC complete: {message}")
        return {
            "pipeline_warnings": len(pipeline_warnings),
            "company_anomalies": len(company_anomalies),
        }
    except Exception as e:  # noqa: BLE001 — QC must never fail the DAG
        logger.error(
            "run_quality_checks: swallowed fatal error (%s: %s)",
            e.__class__.__name__, e,
        )
        return {"error": f"{e.__class__.__name__}: {e}"}

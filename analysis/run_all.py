"""
run_all.py
----------

Run every analysis script in this folder, in a sensible order, and produce:

  - results/<date>/summary.md        — every per-query .md concatenated under
                                       headers, ready to paste into a Claude
                                       conversation.
  - results/<date>/run_log.txt       — start/finish timestamps + return codes
                                       + any tracebacks, so a broken script
                                       is never silently skipped.
  - results/history/                  — dated CSV snapshots of the metrics
                                       we want to diff over time. These are
                                       additive (one new file per run) and
                                       are the foundation for "first week
                                       of April vs fourth week of April"
                                       trajectory analysis later.

Each script is invoked as a separate Python subprocess so that a failure
in one query doesn't poison the others. We capture stdout/stderr per run.

The history archive runs *after* every script has finished, against the
already-populated `company_stats` / `jobs` / `job_history` tables. It is
read-only and intentionally minimal: just the columns we'll want to
backfill into a longitudinal series.
"""

from __future__ import annotations

import csv
import datetime as dt
import os
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent

RUN_DATE = dt.date.today().isoformat()
os.environ["JOBPULSE_ANALYSIS_DATE"] = RUN_DATE

RESULTS = HERE / "results" / RUN_DATE
RESULTS.mkdir(parents=True, exist_ok=True)

HISTORY_DIR = HERE / "results" / "history"
HISTORY_DIR.mkdir(parents=True, exist_ok=True)

SCRIPTS = [
    "ghost_jobs_by_inactivity.py",
    "job_age_distribution.py",
    "hiring_velocity_leaders.py",
    "freshest_vs_stalest_companies.py",
    "posting_cadence_over_time.py",
    "skills_demand_signals.py",
    "salary_outliers.py",
    "remote_hybrid_onsite_split.py",
    "close_reactivation_patterns.py",
]


def _now() -> str:
    return dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _write_history_snapshot(log_lines: list[str]) -> None:
    """Append today's per-company and overall metric snapshots to results/history/.

    Designed to never overwrite — each run produces two new dated files:

      - company_metrics_<RUN_DATE>.csv   one row per active company
      - overall_metrics_<RUN_DATE>.csv   one row, dataset-wide

    Failure here must not break the rest of the run; we log and continue.
    """
    sys.path.insert(0, str(HERE))
    try:
        from db import readonly_cursor
    except Exception as exc:
        log_lines.append(f"[FAIL] history snapshot import: {type(exc).__name__}: {exc}")
        return

    company_path = HISTORY_DIR / f"company_metrics_{RUN_DATE}.csv"
    overall_path = HISTORY_DIR / f"overall_metrics_{RUN_DATE}.csv"

    company_fields = [
        "snapshot_date", "company_name", "active_jobs",
        "posted_7d", "posted_30d", "closed_7d", "closed_30d",
        "net_7d", "net_30d", "median_job_age_days", "top_3_skills",
    ]
    overall_fields = [
        "snapshot_date", "tracked_companies", "active_jobs",
        "posted_7d", "posted_30d", "closed_7d", "closed_30d",
        "net_7d", "net_30d", "overall_median_job_age_days",
        "pipeline_history_age_days",
    ]

    try:
        with readonly_cursor() as cur:
            cur.execute(
                """
                WITH per_company AS (
                    SELECT
                        c.company_id,
                        c.company_name,
                        COUNT(*) FILTER (WHERE j.is_active)                                            AS active_jobs,
                        COUNT(*) FILTER (WHERE j.first_published_at >= NOW() - INTERVAL '7 days')     AS posted_7d,
                        COUNT(*) FILTER (WHERE j.first_published_at >= NOW() - INTERVAL '30 days')    AS posted_30d,
                        PERCENTILE_CONT(0.5) WITHIN GROUP (
                            ORDER BY EXTRACT(EPOCH FROM (NOW() - j.first_published_at)) / 86400.0
                        ) FILTER (WHERE j.is_active AND j.first_published_at IS NOT NULL) AS median_job_age_days
                    FROM companies c
                    JOIN jobs j ON j.company_id = c.company_id
                    GROUP BY c.company_id, c.company_name
                ),
                closes AS (
                    SELECT
                        company_id,
                        COUNT(*) FILTER (WHERE recorded_at >= NOW() - INTERVAL '7 days')   AS closed_7d,
                        COUNT(*) FILTER (WHERE recorded_at >= NOW() - INTERVAL '30 days')  AS closed_30d
                    FROM job_history
                    WHERE change_type = 'closed'
                    GROUP BY company_id
                ),
                top_skills AS (
                    SELECT
                        j.company_id,
                        ARRAY_AGG(skill ORDER BY cnt DESC) AS skill_list
                    FROM (
                        SELECT j2.company_id, s.skill, COUNT(*) AS cnt
                        FROM jobs j2,
                             LATERAL UNNEST(COALESCE(j2.skills, ARRAY[]::TEXT[])) AS s(skill)
                        WHERE j2.is_active
                        GROUP BY j2.company_id, s.skill
                    ) j
                    GROUP BY j.company_id
                )
                SELECT
                    p.company_name,
                    p.active_jobs,
                    p.posted_7d,
                    p.posted_30d,
                    COALESCE(cl.closed_7d, 0)   AS closed_7d,
                    COALESCE(cl.closed_30d, 0)  AS closed_30d,
                    p.posted_7d  - COALESCE(cl.closed_7d, 0)  AS net_7d,
                    p.posted_30d - COALESCE(cl.closed_30d, 0) AS net_30d,
                    ROUND(p.median_job_age_days::NUMERIC, 1)  AS median_job_age_days,
                    (
                        SELECT skill_list[1:3]
                        FROM top_skills ts
                        WHERE ts.company_id = p.company_id
                    ) AS top_3_skills
                FROM per_company p
                LEFT JOIN closes cl ON cl.company_id = p.company_id
                WHERE p.active_jobs > 0
                ORDER BY p.active_jobs DESC
                """
            )
            company_rows = cur.fetchall()

            cur.execute(
                """
                SELECT
                    COUNT(DISTINCT j.company_id) FILTER (WHERE j.is_active)
                        AS tracked_companies,
                    COUNT(*) FILTER (WHERE j.is_active) AS active_jobs,
                    COUNT(*) FILTER (WHERE j.first_published_at >= NOW() - INTERVAL '7 days')
                        AS posted_7d,
                    COUNT(*) FILTER (WHERE j.first_published_at >= NOW() - INTERVAL '30 days')
                        AS posted_30d,
                    PERCENTILE_CONT(0.5) WITHIN GROUP (
                        ORDER BY EXTRACT(EPOCH FROM (NOW() - j.first_published_at)) / 86400.0
                    ) FILTER (WHERE j.is_active AND j.first_published_at IS NOT NULL)
                        AS overall_median_job_age_days
                FROM jobs j
                """
            )
            overall = cur.fetchone()

            cur.execute(
                """
                SELECT
                    COUNT(*) FILTER (WHERE change_type = 'closed' AND recorded_at >= NOW() - INTERVAL '7 days')  AS closed_7d,
                    COUNT(*) FILTER (WHERE change_type = 'closed' AND recorded_at >= NOW() - INTERVAL '30 days') AS closed_30d,
                    (CURRENT_DATE - MIN(recorded_at)::DATE)::INT AS pipeline_history_age_days
                FROM job_history
                """
            )
            jh = cur.fetchone()

        with company_path.open("w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=company_fields, extrasaction="ignore")
            w.writeheader()
            for r in company_rows:
                w.writerow({
                    "snapshot_date": RUN_DATE,
                    "company_name": r["company_name"],
                    "active_jobs": r["active_jobs"],
                    "posted_7d": r["posted_7d"],
                    "posted_30d": r["posted_30d"],
                    "closed_7d": r["closed_7d"],
                    "closed_30d": r["closed_30d"],
                    "net_7d": r["net_7d"],
                    "net_30d": r["net_30d"],
                    "median_job_age_days": r["median_job_age_days"],
                    "top_3_skills": "|".join(r["top_3_skills"] or []),
                })

        closed_7d = (jh["closed_7d"] or 0) if jh else 0
        closed_30d = (jh["closed_30d"] or 0) if jh else 0
        with overall_path.open("w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=overall_fields, extrasaction="ignore")
            w.writeheader()
            w.writerow({
                "snapshot_date": RUN_DATE,
                "tracked_companies": overall["tracked_companies"] if overall else 0,
                "active_jobs": overall["active_jobs"] if overall else 0,
                "posted_7d": overall["posted_7d"] if overall else 0,
                "posted_30d": overall["posted_30d"] if overall else 0,
                "closed_7d": closed_7d,
                "closed_30d": closed_30d,
                "net_7d": (overall["posted_7d"] or 0) - closed_7d if overall else 0,
                "net_30d": (overall["posted_30d"] or 0) - closed_30d if overall else 0,
                "overall_median_job_age_days": (
                    round(float(overall["overall_median_job_age_days"]), 1)
                    if overall and overall["overall_median_job_age_days"] is not None
                    else None
                ),
                "pipeline_history_age_days": (
                    jh["pipeline_history_age_days"] if jh else None
                ),
            })

        log_lines.append(
            f"[ OK ] history snapshot — {len(company_rows)} companies → "
            f"{company_path.name}, overall → {overall_path.name}"
        )
    except Exception as exc:
        log_lines.append(
            f"[FAIL] history snapshot: {type(exc).__name__}: {exc}"
        )


def main() -> int:
    log_path = RESULTS / "run_log.txt"
    summary_path = RESULTS / "summary.md"
    log_lines: list[str] = [
        f"== run_all.py started at {_now()} ==",
        f"Results directory: {RESULTS}",
        f"History directory: {HISTORY_DIR}",
    ]
    failures: list[tuple[str, int]] = []

    for script in SCRIPTS:
        script_path = HERE / script
        if not script_path.exists():
            log_lines.append(f"[SKIP] {script}: file not found")
            failures.append((script, -1))
            continue

        log_lines.append(f"\n[RUN ] {script}  (started {_now()})")
        print(f"\n>>> {script}")
        try:
            proc = subprocess.run(
                [sys.executable, str(script_path)],
                cwd=HERE,
                capture_output=True,
                text=True,
                timeout=300,
            )
        except subprocess.TimeoutExpired:
            log_lines.append(f"[FAIL] {script}: timed out after 300s")
            failures.append((script, -2))
            continue
        except Exception as exc:
            log_lines.append(f"[FAIL] {script}: {type(exc).__name__}: {exc}")
            failures.append((script, -3))
            continue

        if proc.stdout:
            print(proc.stdout, end="")
        if proc.returncode != 0:
            log_lines.append(f"[FAIL] {script}: exit {proc.returncode}")
            log_lines.append("--- stderr ---")
            log_lines.append(proc.stderr.rstrip() or "(no stderr)")
            log_lines.append("--- end stderr ---")
            failures.append((script, proc.returncode))
            print(proc.stderr, file=sys.stderr)
        else:
            log_lines.append(f"[ OK ] {script} (finished {_now()})")

    log_lines.append(f"\n[RUN ] history snapshot  (started {_now()})")
    _write_history_snapshot(log_lines)

    summary_lines: list[str] = [
        f"# JobPulse — analysis summary ({RUN_DATE})",
        "",
        f"_Generated {_now()} by `analysis/run_all.py`._",
        "",
    ]
    for script in SCRIPTS:
        md_path = RESULTS / (script.replace(".py", "") + ".md")
        section_title = script.replace(".py", "").replace("_", " ").title()
        summary_lines.append(f"## {section_title}")
        summary_lines.append("")
        if md_path.exists():
            body = md_path.read_text().strip()
            body_lines = body.splitlines()
            if body_lines and body_lines[0].startswith("# "):
                body_lines = body_lines[1:]
                while body_lines and not body_lines[0].strip():
                    body_lines.pop(0)
            summary_lines.append("\n".join(body_lines))
        else:
            summary_lines.append(f"_No markdown produced (script may have failed). See `run_log.txt`._")
        summary_lines.append("")

    summary_lines.append("---")
    summary_lines.append("")
    summary_lines.append("## Longitudinal archive")
    summary_lines.append("")
    summary_lines.append(
        f"This run also wrote dated CSV snapshots to `analysis/results/history/` "
        f"(gitignored). These accumulate across runs and are the source of truth "
        f"for cross-snapshot diffs (\"first week of the month vs last week\")."
    )
    summary_lines.append("")
    summary_lines.append(
        f"- `company_metrics_{RUN_DATE}.csv` — per-company snapshot "
        f"(active_jobs, posted_7d, posted_30d, closed_7d, closed_30d, net_7d, "
        f"net_30d, median_job_age_days, top_3_skills)."
    )
    summary_lines.append(
        f"- `overall_metrics_{RUN_DATE}.csv` — single-row dataset-wide snapshot."
    )
    summary_lines.append("")

    if failures:
        summary_lines.append("---")
        summary_lines.append("")
        summary_lines.append("## Failures in this run")
        summary_lines.append("")
        for name, code in failures:
            summary_lines.append(f"- `{name}` (exit {code})")
        summary_lines.append("")

    summary_path.write_text("\n".join(summary_lines))
    log_lines.append(f"\n== run_all.py finished at {_now()} ==")
    if failures:
        log_lines.append(f"Failures: {len(failures)}")
        for name, code in failures:
            log_lines.append(f"  - {name} (exit {code})")
    log_path.write_text("\n".join(log_lines))

    print(f"\nSummary written to {summary_path}")
    print(f"Run log written to {log_path}")
    if failures:
        print(f"\n{len(failures)} script(s) failed — see {log_path.name}.")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())

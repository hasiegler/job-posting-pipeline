"""
Scrapes job postings for each enabled company defined in companies.yaml.
Outputs results to a JSON file per company in the ./output/ directory.
"""

import json
import logging
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse

import requests
import yaml

from alerting import send_alert
from api.board_guards import CONSECUTIVE_CONFIRMATION_DAYS, AshbyGuardTrip, next_consecutive_counts
from datawarehouse.data_utils import run_with_db

try:
    from airflow.decorators import task
except ImportError:
    def task(func):
        func.function = func
        return func

logger = logging.getLogger(__name__)

COMPANIES_FILE = "companies.yaml"



@task
def load_companies(path: str, scraper_type: str = None) -> list[dict]:
    """Load and return enabled companies from the YAML config, optionally filtered by scraper_type."""
    with open(path, "r") as f:
        data = yaml.safe_load(f)

    companies = data.get("companies", [])

    enabled = [c for c in companies if c.get("enabled", False)]

    if scraper_type:
        enabled = [c for c in enabled if c.get("scraper_type") == scraper_type]

    if not enabled:
        print("No enabled companies found in config.")
    return enabled


def extract_board_token(url: str) -> str:
    """Extract the board token (last path segment) from a Greenhouse URL.

    e.g. https://boards.greenhouse.io/anthropic -> 'anthropic'
    """
    path = urlparse(url).path.rstrip("/")
    return path.split("/")[-1]


def scrape_greenhouse_jobs(company: dict) -> dict:
    """Scrape all jobs from a Greenhouse job board using their public JSON API.

    Plain function (not an Airflow task) so it can be invoked from the
    `scrape_all_companies` dispatcher alongside other ATS handlers.
    """
    board_token = extract_board_token(company["url"])
    api_url = f"https://boards-api.greenhouse.io/v1/boards/{board_token}/jobs"

    print(f"  Fetching job list from {api_url} ...")
    resp = requests.get(api_url, params={"content": "true"}, timeout=60)
    resp.raise_for_status()

    body = resp.json()
    raw_jobs = body.get("jobs", [])
    # Greenhouse's /jobs endpoint is unpaginated and returns meta.total. If the
    # response we got has fewer jobs than meta.total, treat it as a partial
    # response and fail loud — silently accepting it would mass-close the
    # missing jobs downstream and pollute job_history with phantom
    # close+reactivate cycles.
    meta_total = body.get("meta", {}).get("total")
    if meta_total is not None and meta_total != len(raw_jobs):
        send_alert(
            f"[Greenhouse] {company['name']}: partial response — "
            f"meta.total={meta_total}, received={len(raw_jobs)}. "
            f"Skipping this run."
        )
        raise RuntimeError(
            f"{company['name']}: Greenhouse reported meta.total={meta_total} "
            f"but returned {len(raw_jobs)} jobs — refusing partial response."
        )

    print(f"  Found {len(raw_jobs)} jobs (meta.total={meta_total}).")

    # Store raw API objects exactly as received.  Only filter out jobs with no
    # title — those are unpublished drafts the API occasionally leaks and are
    # useless for any downstream consumer.  All other fields (absolute_url,
    # location object, departments/offices objects, metadata, content HTML,
    # internal_job_id, requisition_id, data_compliance, etc.) are preserved
    # verbatim so normalize_greenhouse can work from the pristine source.
    jobs = [raw for raw in raw_jobs if (raw.get("title") or "").strip()]

    return {"company_name": company["name"], "jobs": jobs, "meta_total": meta_total}


# Per-scraper required field maps.  Maps logical jobs-table column name
# (used in alert text) to the raw scrape-dict key for that ATS.
#
# Greenhouse jobs stored in S3 are now RAW API objects, so field names differ
# from Ashby's pre-shaped dicts:
#   - source_url  : raw has "absolute_url", Ashby has "url"
#   - description : raw has "content" (HTML), Ashby has "content_text" (plain)
#   - location    : raw has a nested object {"name": ...}; handled separately
#
# `departments` is deliberately absent from both maps — some boards
# legitimately never populate it.  departments quality is surfaced warn-only
# by the final QC task instead.
_REQUIRED_FIELDS_GREENHOUSE = {
    "source_job_id":  "id",
    "source_url":     "absolute_url",
    "title":          "title",
    # location checked separately (nested object — see check_completeness)
    "description_text": "content",
}

_REQUIRED_FIELDS_ASHBY = {
    "source_job_id":    "id",
    "source_url":       "jobUrl",         # raw Ashby field (was "url" in shaped dict)
    "title":            "title",
    "location":         "location",       # primary location — flat string in raw Ashby
    "description_text": "descriptionPlain",  # raw Ashby field (was "content_text")
}

_REQUIRED_FIELDS_BY_SCRAPER: dict[str, dict[str, str]] = {
    "greenhouse": _REQUIRED_FIELDS_GREENHOUSE,
    "ashby":      _REQUIRED_FIELDS_ASHBY,
}


def _is_missing(value) -> bool:
    """True if a scraped field is null or blank (empty/whitespace-only string)."""
    if value is None:
        return True
    if isinstance(value, str) and not value.strip():
        return True
    return False


def check_completeness(company_name: str, jobs: list[dict], scraper_type: str = "ashby") -> None:
    """Blocking per-company completeness guard.

    Runs after a company's jobs are scraped but BEFORE the S3 write / staging
    insert.  If ANY job is missing a required field this alerts and raises
    ``RuntimeError`` so the surrounding `scrape_all_companies` handler converts
    it into the SAME "transient" sentinel the Ashby/Greenhouse guards use: no
    S3 write, no staging insert, existing jobs untouched, board not
    auto-disabled, and the run still succeeds.

    The required field map is per-scraper because Greenhouse jobs are now raw
    API objects (different key names than Ashby's pre-shaped dicts).  Location
    for Greenhouse is a nested object; we check location.name specifically.

    The fields salary / remote_policy / skills / language / offices /
    departments are intentionally NOT required — they're null by design on many
    postings.
    """
    if not jobs:
        return

    required = _REQUIRED_FIELDS_BY_SCRAPER.get(scraper_type, _REQUIRED_FIELDS_ASHBY)
    is_greenhouse = scraper_type == "greenhouse"

    missing_counts: dict[str, int] = {}
    for job in jobs:
        for field, raw_key in required.items():
            if _is_missing(job.get(raw_key)):
                missing_counts[field] = missing_counts.get(field, 0) + 1

        # Greenhouse location is a nested object {"name": ...}.  Check the
        # name value directly; a missing or blank name is treated as missing.
        if is_greenhouse:
            loc_name = (job.get("location") or {}).get("name")
            if _is_missing(loc_name):
                missing_counts["location"] = missing_counts.get("location", 0) + 1

    if missing_counts:
        detail = ", ".join(
            f"{field} ({n})" for field, n in sorted(missing_counts.items())
        )
        send_alert(
            f"[COMPLETENESS BLOCK] {company_name}: skipping this run — "
            f"of {len(jobs)} scraped jobs, missing required field(s): {detail}. "
            f"Likely a source schema change."
        )
        raise RuntimeError(
            f"{company_name}: completeness guard tripped — missing {detail}."
        )


def _sentinel_result(
    company: dict,
    error_type: str,
    error: str,
    skip_reason: str | None = None,
) -> dict:
    """Return a result shape that downstream tasks treat as a skip.

    `error_type` is either:
      - "permanent"  → counts toward the 7-day 404 latch in `disable_dead_boards`
      - "transient"  → skip this run only; next run retries

    `skip_reason` distinguishes zero-floor skips (count toward the empty-board
    latch) from other transients (which reset both counters).

    Shape mirrors the success-path summary (see `scrape_all_companies`) so
    downstream mapped tasks can read the same keys for every map index.
    `s3_path` is `None` so `update_staging_jobs` no-ops for skipped companies.
    """
    return {
        "company_name": company.get("name", "unknown"),
        "scraper_type": company.get("scraper_type"),
        "s3_path": None,
        "total_jobs": 0,
        "meta_total": None,
        "skipped": True,
        "error_type": error_type,
        "error": error,
        "skip_reason": skip_reason,
    }


def _write_result_to_s3(result: dict) -> str:
    """Write a scrape result envelope to S3 as JSON and return the s3:// URI.

    Mirrors the layout the old `save_results` task wrote so the existing
    `load_s3_json` / `insert_staging_jobs` consumers don't need to change.
    Called inline from `scrape_all_companies` so the full jobs payload never
    has to round-trip through Airflow's XCom backend.
    """
    import boto3

    company_name = result["company_name"]
    jobs = result["jobs"]

    now = datetime.now(timezone.utc)
    timestamp = now.strftime("%Y%m%d_%H%M%S")
    filename = f"{company_name}_{timestamp}.json"
    s3_key = f"{company_name}/{now.year}/{now.month:02d}/{now.day:02d}/{filename}"

    output = {
        "company": company_name,
        "scraped_at": now.isoformat(),
        "total_jobs": len(jobs),
        "meta_total": result.get("meta_total"),
        "jobs": jobs,
    }

    json_data = json.dumps(output, indent=2, ensure_ascii=False)

    bucket = os.environ["S3_BUCKET_NAME"]
    s3_client = boto3.client("s3")
    s3_client.put_object(
        Bucket=bucket,
        Key=s3_key,
        Body=json_data.encode("utf-8"),
        ContentType="application/json",
    )

    s3_path = f"s3://{bucket}/{s3_key}"
    print(f"  Saved {len(jobs)} jobs -> {s3_path}")
    return s3_path


@task
def scrape_all_companies(company: dict) -> dict:
    """Dispatch a single company's scrape to the appropriate ATS handler,
    write the full result to S3, and return a lightweight summary.

    One mapped task in the DAG handles every ATS. Adding a new ATS (Lever, etc.)
    only requires registering a new handler below — no new DAG task needed.

    The full jobs payload (potentially tens of MB of HTML per company) is
    written directly to S3 here rather than returned to Airflow. Only a small
    summary dict — the S3 path plus counts — is pushed to XCom, so the
    metadata DB stays small and the worker subprocess can't get OOM-killed
    serializing a giant return value.

    Any failure is captured here and returned as a sentinel result rather than
    raised, so one bad company doesn't fail the mapped task and cascade into
    the rest of the pipeline.     Permanent failures (HTTP 404 / 401 / 403) count toward a 7-day latch;
    `disable_dead_boards` flips YAML + DB off only after that streak.
    Transient failures (network, 5xx, partial responses, schema breaks, and
    S3 write errors) reset the latch and retry next run.
    """
    name = company.get("name", "unknown")
    scraper_type = company.get("scraper_type")
    try:
        if scraper_type == "greenhouse":
            result = scrape_greenhouse_jobs(company)
        elif scraper_type == "ashby":
            from api.scrape_ashby import scrape_ashby_jobs
            result = scrape_ashby_jobs(company)
        else:
            # Unknown scraper_type is a config bug — surface via sentinel
            # rather than raising, so the rest of the run still completes.
            logger.error(f"{name}: unknown scraper_type '{scraper_type}'")
            return _sentinel_result(
                company, "transient", f"unknown scraper_type '{scraper_type}'",
                skip_reason="unknown_scraper",
            )

        # Blocking completeness guard: raises RuntimeError (→ transient
        # sentinel below) if any scraped job is missing a required field, so a
        # silent source schema change can't load incomplete rows.
        check_completeness(result["company_name"], result["jobs"], scraper_type=scraper_type)

        s3_path = _write_result_to_s3(result)
        return {
            "company_name": result["company_name"],
            "scraper_type": scraper_type,
            "s3_path": s3_path,
            "total_jobs": len(result["jobs"]),
            "meta_total": result.get("meta_total"),
            "skipped": False,
            "error_type": None,
            "error": None,
            "skip_reason": None,
        }
    except requests.HTTPError as e:
        status = e.response.status_code if e.response is not None else None
        if status in (404, 401, 403):
            logger.error(
                f"{name}: permanent failure HTTP {status} — counts toward "
                f"the {CONSECUTIVE_CONFIRMATION_DAYS}-day disable latch."
            )
            return _sentinel_result(
                company, "permanent", f"HTTP {status}", skip_reason="permanent_http"
            )
        logger.warning(
            f"{name}: transient HTTP {status}, skipping this run — will retry next run."
        )
        return _sentinel_result(
            company, "transient", f"HTTP {status}", skip_reason="transient_http"
        )
    except (requests.ConnectionError, requests.Timeout) as e:
        logger.warning(
            f"{name}: transient network failure, skipping this run: "
            f"{e.__class__.__name__}"
        )
        return _sentinel_result(
            company, "transient", f"network: {e.__class__.__name__}",
            skip_reason="network",
        )
    except AshbyGuardTrip as e:
        logger.warning(f"{name}: Ashby guard trip, skipping this run: {e}")
        return _sentinel_result(
            company, "transient", str(e), skip_reason=e.skip_reason
        )
    except RuntimeError as e:
        # Includes the Greenhouse `meta.total` partial-response guard.
        # The board is fine — the response was truncated — so don't auto-disable.
        logger.warning(f"{name}: runtime failure, skipping this run: {e}")
        return _sentinel_result(
            company, "transient", str(e), skip_reason="runtime"
        )
    except (ValueError, KeyError, json.JSONDecodeError) as e:
        # Schema break — log loudly because this likely affects every company
        # on the same ATS. Do NOT auto-disable; one of these would silently
        # take out the entire fleet.
        logger.error(
            f"{name}: SCHEMA BREAK ({e.__class__.__name__}: {e}) — "
            f"the ATS may have changed its API, investigate!"
        )
        return _sentinel_result(
            company, "transient", f"schema break: {e}", skip_reason="schema_break"
        )
    except Exception as e:  # noqa: BLE001 — last-resort catch-all
        logger.error(
            f"{name}: unexpected error ({e.__class__.__name__}: {e}), "
            f"skipping this run."
        )
        return _sentinel_result(
            company, "transient", f"{e.__class__.__name__}: {e}",
            skip_reason="unexpected",
        )


def _disable_in_yaml(yaml_path: str, company_names: list[str]) -> list[str]:
    """Flip `enabled: true` → `enabled: false` for the given company names.

    Edits the YAML file in place via a line-by-line walk rather than
    yaml.safe_load + yaml.dump so the diff is surgical: only the `enabled:`
    value changes; comments, blank lines, and key ordering are preserved.

    Returns the list of company names whose `enabled` flag was actually
    flipped. Names that weren't found, or were already `enabled: false`,
    are silently omitted from the return value.
    """
    name_set = set(company_names)
    text = Path(yaml_path).read_text(encoding="utf-8")
    lines = text.splitlines(keepends=True)

    name_re = re.compile(r"^\s*-\s+name:\s*([\w_-]+)\s*$")
    enabled_re = re.compile(r"^(\s*enabled:\s*)true\s*$")

    flipped: list[str] = []
    current_target: str | None = None

    for i, line in enumerate(lines):
        m_name = name_re.match(line.rstrip("\n"))
        if m_name:
            n = m_name.group(1)
            current_target = n if n in name_set else None
            continue

        if current_target is None:
            continue

        m_en = enabled_re.match(line.rstrip("\n"))
        if m_en:
            lines[i] = f"{m_en.group(1)}false\n"
            flipped.append(current_target)
            current_target = None  # done with this company

    if flipped:
        Path(yaml_path).write_text("".join(lines), encoding="utf-8")

    return flipped


def _ensure_latch_columns(cur) -> None:
    cur.execute(
        "ALTER TABLE companies ADD COLUMN IF NOT EXISTS "
        "consecutive_zero_scrapes INTEGER NOT NULL DEFAULT 0"
    )
    cur.execute(
        "ALTER TABLE companies ADD COLUMN IF NOT EXISTS "
        "consecutive_permanent_failures INTEGER NOT NULL DEFAULT 0"
    )


def _apply_consecutive_counters(results: list[dict]) -> list[str]:
    """Bump/reset latch counters from this run's scrape summaries.

    Returns company names whose ``consecutive_permanent_failures`` is now
    at or past ``CONSECUTIVE_CONFIRMATION_DAYS``.
    """
    def _fn(conn, cur):
        _ensure_latch_columns(cur)
        names = [r["company_name"] for r in results if r.get("company_name")]
        if not names:
            conn.commit()
            return []
        cur.execute(
            """
            SELECT company_name,
                   consecutive_zero_scrapes,
                   consecutive_permanent_failures
            FROM companies
            WHERE company_name = ANY(%s)
            """,
            (names,),
        )
        current = {row["company_name"]: dict(row) for row in cur.fetchall()}
        latched: list[str] = []
        for result in results:
            name = result.get("company_name")
            row = current.get(name)
            if not row:
                continue
            zero, perm = next_consecutive_counts(
                int(row["consecutive_zero_scrapes"] or 0),
                int(row["consecutive_permanent_failures"] or 0),
                result,
            )
            cur.execute(
                """
                UPDATE companies
                SET consecutive_zero_scrapes = %s,
                    consecutive_permanent_failures = %s,
                    updated_at = NOW()
                WHERE company_name = %s
                """,
                (zero, perm, name),
            )
            row["consecutive_zero_scrapes"] = zero
            row["consecutive_permanent_failures"] = perm
            if perm >= CONSECUTIVE_CONFIRMATION_DAYS:
                latched.append(name)
        conn.commit()
        return latched

    try:
        return run_with_db(_fn)
    except Exception as e:  # noqa: BLE001 — never fail the pipeline on counters
        logger.warning("Could not update consecutive latch counters: %s", e)
        return []


def _disable_in_db(company_names: list[str]) -> list[str]:
    """Set ``enabled = false`` in the companies table. Returns names updated."""
    if not company_names:
        return []

    def _fn(conn, cur):
        cur.execute(
            """
            UPDATE companies
            SET enabled = FALSE, updated_at = NOW()
            WHERE company_name = ANY(%s)
            RETURNING company_name
            """,
            (company_names,),
        )
        rows = cur.fetchall()
        conn.commit()
        return [r["company_name"] for r in rows]

    try:
        return run_with_db(_fn)
    except Exception as e:  # noqa: BLE001 — YAML disable can still proceed
        logger.warning("Could not disable companies in DB: %s", e)
        return []


@task
def disable_dead_boards(
    scrape_results: list[dict],
    yaml_path: str = COMPANIES_FILE,
) -> dict:
    """Update consecutive-run latches and disable boards after 7 days of 404s.

    Every scrape summary bumps or resets ``consecutive_zero_scrapes`` /
    ``consecutive_permanent_failures`` on ``companies``. Only boards whose
    permanent-failure streak has reached ``CONSECUTIVE_CONFIRMATION_DAYS``
    are flipped to ``enabled: false`` — in companies.yaml *and* in the DB.

    If the YAML is not writable, the DB is still disabled (so warehouse
    views stop treating the board as live) and a MANUAL ACTION REQUIRED
    alert lists the YAML edits. The task never fails the pipeline.

    Transient failures reset both counters and are not disabled.
    """
    results = [r for r in (scrape_results or []) if isinstance(r, dict)]
    latched_names = _apply_consecutive_counters(results)
    latched_set = set(latched_names)

    permanent = [
        r for r in results
        if r.get("skipped") and r.get("error_type") == "permanent"
    ]
    to_disable = [r for r in permanent if r.get("company_name") in latched_set]
    names = [r["company_name"] for r in to_disable]

    if permanent:
        print(f"  {len(permanent)} permanent failure(s) this run:")
        for r in permanent:
            print(f"    - {r['company_name']}: {r.get('error', '(no detail)')}")

    if not names:
        print(
            "  No boards past the "
            f"{CONSECUTIVE_CONFIRMATION_DAYS}-day 404 latch — nothing to disable."
        )
        return {
            "disabled": [],
            "permanent_failures": [
                {"company_name": r["company_name"], "error": r.get("error")}
                for r in permanent
            ],
            "yaml_write_ok": True,
        }

    print(
        f"  {len(names)} board(s) failed HTTP 404/401/403 for "
        f"{CONSECUTIVE_CONFIRMATION_DAYS} consecutive runs — disabling:"
    )
    for n in names:
        print(f"    - {n}")

    db_disabled = _disable_in_db(names)
    print(f"  DB enabled=false: {db_disabled or '(none)'}")

    yaml_write_ok = True
    yaml_write_error = None
    flipped: list[str] = []
    try:
        flipped = _disable_in_yaml(yaml_path, names)
    except OSError as e:
        yaml_write_ok = False
        yaml_write_error = f"{e.__class__.__name__}: {e}"
        logger.warning(
            f"Could not write to {yaml_path} ({yaml_write_error}). "
            f"DB was disabled; MANUAL ACTION REQUIRED for YAML: {names}"
        )
        print("")
        print("  >>> YAML write failed — MANUAL ACTION REQUIRED <<<")
        print(f"  Reason: {yaml_write_error}")
        print("  Edit companies.yaml and set `enabled: false` for:")
        for n in names:
            print(f"      - {n}")
        print(
            "  (containers should run as AIRFLOW_UID so the bind-mounted "
            "companies.yaml is writable; see docker-compose.yaml)"
        )
        detail = "\n".join(
            f"  {r['company_name']}: {r.get('error', '?')}" for r in to_disable
        )
        send_alert(
            f"[DEAD BOARD — MANUAL ACTION REQUIRED] "
            f"{len(names)} board(s) hit HTTP 404/401/403 for "
            f"{CONSECUTIVE_CONFIRMATION_DAYS} consecutive runs. "
            f"DB enabled=false was set, but companies.yaml could not be "
            f"updated ({e.__class__.__name__}). Set enabled: false in YAML "
            f"or the next sync will re-enable them:\n{detail}"
        )
    else:
        flipped_set = set(flipped)
        print("  YAML edit:")
        for n in names:
            marker = "disabled in YAML" if n in flipped_set else "already disabled"
            print(f"    [{marker}] {n}")
        detail = "\n".join(
            f"  {r['company_name']}: {r.get('error', '?')}" for r in to_disable
        )
        send_alert(
            f"[DEAD BOARD DISABLED] "
            f"{len(names)} board(s) auto-disabled in companies.yaml and the "
            f"DB after {CONSECUTIVE_CONFIRMATION_DAYS} consecutive "
            f"HTTP 404/401/403. Leftover jobs will close this run:\n{detail}"
        )

    return {
        "disabled": names,
        "permanent_failures": [
            {"company_name": r["company_name"], "error": r.get("error")}
            for r in permanent
        ],
        "yaml_write_ok": yaml_write_ok,
        "yaml_write_error": yaml_write_error,
        "db_disabled": db_disabled,
    }


if __name__ == "__main__":
    import time

    companies = load_companies.function(COMPANIES_FILE)

    if not companies:
        print("No enabled companies found.")
    else:
        for company in companies:
            print(f"\n[{company['name']}]")
            summary = scrape_all_companies.function(company)
            print(f"  -> {summary}")
            time.sleep(1)

        print("\nDone.")
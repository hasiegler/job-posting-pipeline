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

from bs4 import BeautifulSoup
import requests
import yaml

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
        raise RuntimeError(
            f"{company['name']}: Greenhouse reported meta.total={meta_total} "
            f"but returned {len(raw_jobs)} jobs — refusing partial response."
        )

    print(f"  Found {len(raw_jobs)} jobs (meta.total={meta_total}).")

    jobs = []
    for raw in raw_jobs:
        title = (raw.get("title") or "").strip()
        if not title:
            continue

        location = raw.get("location", {}).get("name", "Unknown")
        departments = [d["name"] for d in raw.get("departments", []) if "name" in d]
        offices = [o["name"] for o in raw.get("offices", []) if "name" in o]

        jobs.append(
            {
                "id": raw.get("id"),
                "title": title,
                "url": raw.get("absolute_url"),
                "location": location,
                "departments": departments,
                "offices": offices,
                "content_html": raw.get("content"),
                "content_text": BeautifulSoup(raw.get("content", ""), "html.parser").get_text(separator="\n", strip=True),
                "language": raw.get("language"),
                "first_published": raw.get("first_published"),
                "updated_at": raw.get("updated_at"),
            }
        )

    return {"company_name": company["name"], "jobs": jobs, "meta_total": meta_total}


def _sentinel_result(company: dict, error_type: str, error: str) -> dict:
    """Return a result shape that downstream tasks treat as a skip.

    `error_type` is either:
      - "permanent"  → `disable_dead_boards` will flip the YAML to enabled=false
      - "transient"  → skip this run only; next run retries

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
    the rest of the pipeline. Permanent failures (HTTP 404 / 401 / 403) are
    flagged so `disable_dead_boards` can flip them off in companies.yaml.
    Transient failures (network, 5xx, partial responses, schema breaks, and
    S3 write errors) are skipped only — the next run will retry.
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
                company, "transient", f"unknown scraper_type '{scraper_type}'"
            )

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
        }
    except requests.HTTPError as e:
        status = e.response.status_code if e.response is not None else None
        if status in (404, 401, 403):
            logger.error(
                f"{name}: permanent failure HTTP {status} — will be auto-disabled "
                f"in companies.yaml by disable_dead_boards."
            )
            return _sentinel_result(company, "permanent", f"HTTP {status}")
        logger.warning(
            f"{name}: transient HTTP {status}, skipping this run — will retry next run."
        )
        return _sentinel_result(company, "transient", f"HTTP {status}")
    except (requests.ConnectionError, requests.Timeout) as e:
        logger.warning(
            f"{name}: transient network failure, skipping this run: "
            f"{e.__class__.__name__}"
        )
        return _sentinel_result(
            company, "transient", f"network: {e.__class__.__name__}"
        )
    except RuntimeError as e:
        # Includes the Greenhouse `meta.total` partial-response guard.
        # The board is fine — the response was truncated — so don't auto-disable.
        logger.warning(f"{name}: runtime failure, skipping this run: {e}")
        return _sentinel_result(company, "transient", str(e))
    except (ValueError, KeyError, json.JSONDecodeError) as e:
        # Schema break — log loudly because this likely affects every company
        # on the same ATS. Do NOT auto-disable; one of these would silently
        # take out the entire fleet.
        logger.error(
            f"{name}: SCHEMA BREAK ({e.__class__.__name__}: {e}) — "
            f"the ATS may have changed its API, investigate!"
        )
        return _sentinel_result(company, "transient", f"schema break: {e}")
    except Exception as e:  # noqa: BLE001 — last-resort catch-all
        logger.error(
            f"{name}: unexpected error ({e.__class__.__name__}: {e}), "
            f"skipping this run."
        )
        return _sentinel_result(
            company, "transient", f"{e.__class__.__name__}: {e}"
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


@task
def disable_dead_boards(
    scrape_results: list[dict],
    yaml_path: str = COMPANIES_FILE,
) -> dict:
    """Report permanently-failed companies and (best-effort) flip them to
    `enabled: false` in companies.yaml.

    Reads the full list of `scrape_all_companies` mapped results and filters
    for sentinels with `error_type == "permanent"` (HTTP 404/401/403).

    Behavior:
      - If the YAML is writable, edit it in place. The next DAG run's
        `sync_companies` step propagates `enabled = false` to Supabase.
      - If the YAML is NOT writable (e.g. mounted read-only in Docker, or
        the host file is root-owned), log a clear "MANUAL ACTION REQUIRED"
        message listing the companies that need to be disabled. The task
        still succeeds — we never want this task to fail the pipeline.

    Transient failures (network, 5xx, partial responses, schema breaks) are
    intentionally NOT acted on here: they'll retry on the next DAG run.
    """
    permanent = [
        r for r in (scrape_results or [])
        if isinstance(r, dict)
        and r.get("skipped")
        and r.get("error_type") == "permanent"
    ]

    if not permanent:
        print("  No permanent scrape failures — nothing to disable.")
        return {"disabled": [], "permanent_failures": [], "yaml_write_ok": True}

    names = [r["company_name"] for r in permanent]

    print(f"  {len(permanent)} permanent failure(s) detected:")
    for r in permanent:
        print(f"    - {r['company_name']}: {r.get('error', '(no detail)')}")

    try:
        flipped = _disable_in_yaml(yaml_path, names)
    except OSError as e:
        # Most common cause: companies.yaml is read-only inside the Airflow
        # container (root-owned host file or read-only bind mount). Don't
        # fail the task — surface a clear actionable message instead.
        logger.warning(
            f"Could not write to {yaml_path} ({e.__class__.__name__}: {e}). "
            f"MANUAL ACTION REQUIRED: edit companies.yaml and set "
            f"`enabled: false` for: {names}"
        )
        print("")
        print("  >>> YAML write failed — MANUAL ACTION REQUIRED <<<")
        print(f"  Reason: {e.__class__.__name__}: {e}")
        print("  Edit companies.yaml and set `enabled: false` for:")
        for n in names:
            print(f"      - {n}")
        print(
            "  (To enable auto-disable in the future, make companies.yaml "
            "writable by the Airflow worker, e.g. `chmod 666 companies.yaml`)"
        )
        return {
            "disabled": [],
            "permanent_failures": [
                {"company_name": r["company_name"], "error": r.get("error")}
                for r in permanent
            ],
            "yaml_write_ok": False,
            "yaml_write_error": f"{e.__class__.__name__}: {e}",
        }

    flipped_set = set(flipped)
    print("  YAML edit successful:")
    for r in permanent:
        cname = r["company_name"]
        marker = "disabled in YAML" if cname in flipped_set else "already disabled"
        print(f"    [{marker}] {cname}")

    return {
        "disabled": flipped,
        "permanent_failures": [
            {"company_name": r["company_name"], "error": r.get("error")}
            for r in permanent
        ],
        "yaml_write_ok": True,
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
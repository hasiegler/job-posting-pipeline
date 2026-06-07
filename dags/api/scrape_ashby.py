"""
Scrape Ashby job boards via the public posting API.

Endpoint pattern:
    https://api.ashbyhq.com/posting-api/job-board/{board_token}?includeCompensation=true

The Ashby API differs from Greenhouse in three important ways that this module
absorbs so the downstream pipeline can stay scraper-agnostic:

* Locations are split into a primary `location` plus an array of
  `secondaryLocations`; we concatenate them with "; " for a single column.
* Remote-policy comes from the `workplaceType` field (Remote/Hybrid/OnSite).
  We map OnSite -> "On-Site" so it matches the canonical string Greenhouse
  uses (referenced by `company_stats.remote_policy = 'On-Site'`).
* Salary is provided directly on the posting under `compensation` and is
  treated as the source of truth for Ashby jobs.  Description-text salary
  extraction is intentionally skipped downstream.
"""

import logging
from typing import Optional
from urllib.parse import urlparse

import requests

from alerting import send_alert
from datawarehouse.data_utils import run_with_db

logger = logging.getLogger(__name__)

ASHBY_API_BASE = "https://api.ashbyhq.com/posting-api/job-board"

# Greenhouse uses "On-Site" (hyphenated); Ashby returns "OnSite" (one word).
# Keep the existing canonical strings so analytics SQL and any downstream
# consumers stay unchanged.
_WORKPLACE_TYPE_MAP = {
    "Remote": "Remote",
    "Hybrid": "Hybrid",
    "OnSite": "On-Site",
}


def _get_baseline_scraped_jobs(company_name: str) -> Optional[int]:
    """Return ``scraped_jobs`` from this company's most recent
    ``company_run_metrics`` row with ``scraped_jobs > 0``, or ``None`` if no
    such row exists.

    The ``scraped_jobs > 0`` filter restricts the baseline to runs that
    reflect a real successful scrape.  It excludes:

    * Skipped runs.  ``finalize_run_metrics`` in ``dwh.py`` does not write a
      row directly for skipped scrapes (the staging-summary loop filters out
      ``None`` entries), but a row CAN still get written for a skipped
      company via the jobs-summary loop (carry-over unprocessed staging from
      a prior failed run) or the extraction loop (un-extracted active jobs
      from earlier).  In both carry-over paths ``scraped_jobs`` is hard-set
      to ``0`` because only the loop that actually populates ``scraped_jobs``
      is the staging-summary loop, which the skipped scrape never reaches.
    * Carry-over rows where the company appeared only via staging or
      extraction work, which by the same mechanism have ``scraped_jobs = 0``.

    Known limitation: a company with a legitimate zero-listing successful
    scrape (the board is live but currently has no postings) is also
    excluded by this filter.  That's an acceptable tradeoff because a
    zero-baseline company can't be meaningfully regression-checked anyway —
    rule (a) needs ``baseline > 0`` and rule (b) needs ``baseline >= 10``,
    so a 0-baseline never trips either rule even when included.
    """
    def _q(conn, cur):
        cur.execute(
            """
            SELECT scraped_jobs
            FROM company_run_metrics
            WHERE company_name = %s AND scraped_jobs > 0
            ORDER BY run_started_at DESC
            LIMIT 1
            """,
            (company_name,),
        )
        row = cur.fetchone()
        return row["scraped_jobs"] if row else None

    return run_with_db(_q)


def extract_board_token(url: str) -> str:
    """Extract the board token (last path segment) from an Ashby board URL.

    e.g. https://jobs.ashbyhq.com/Ramp -> 'Ramp'
    """
    path = urlparse(url).path.rstrip("/")
    return path.split("/")[-1]


def _combine_locations(job: dict) -> Optional[str]:
    """Concatenate primary `location` and `secondaryLocations[].location` with '; '."""
    primary = job.get("location")
    secondary = [
        loc.get("location")
        for loc in (job.get("secondaryLocations") or [])
        if loc.get("location")
    ]
    parts = [p for p in [primary, *secondary] if p]
    return "; ".join(parts) if parts else None


def _extract_salary(compensation: Optional[dict]) -> dict:
    """Pick a single Salary component out of `compensation.summaryComponents`.

    Walks the flat top-level `summaryComponents` list and returns the first
    entry where `compensationType == "Salary"` and both min and max are
    populated. Equity / EquityCashValue / EquityPercentage components are
    ignored. Interval is mapped: "1 YEAR" -> "yearly", "1 HOUR" -> "hourly";
    anything else leaves salary_period None.
    """
    empty = {
        "salary_min": None,
        "salary_max": None,
        "salary_currency": None,
        "salary_period": None,
    }
    if not compensation:
        return empty

    for comp in compensation.get("summaryComponents") or []:
        if comp.get("compensationType") != "Salary":
            continue
        if comp.get("minValue") is None or comp.get("maxValue") is None:
            continue

        interval = (comp.get("interval") or "").upper()
        if interval == "1 YEAR":
            period: Optional[str] = "yearly"
        elif interval == "1 HOUR":
            period = "hourly"
        else:
            period = None

        return {
            "salary_min": comp.get("minValue"),
            "salary_max": comp.get("maxValue"),
            "salary_currency": comp.get("currencyCode"),
            "salary_period": period,
        }

    return empty


def scrape_ashby_jobs(company: dict) -> dict:
    """Scrape all listed jobs from a single Ashby public job board.

    Returns the same envelope shape as the Greenhouse scraper so the
    `scrape_all_companies` S3 writer, `insert_staging_jobs`, and
    `normalize_ashby` downstream can treat both ATSes identically.
    """
    board_token = extract_board_token(company["url"])
    api_url = f"{ASHBY_API_BASE}/{board_token}"

    print(f"  Fetching Ashby job list from {api_url} ...")
    resp = requests.get(api_url, params={"includeCompensation": "true"}, timeout=60)
    resp.raise_for_status()

    body = resp.json()
    # Defensive — the Ashby public endpoint should already only return listed jobs.
    raw_jobs = [j for j in body.get("jobs", []) if j.get("isListed", True)]

    print(f"  Found {len(raw_jobs)} listed jobs.")

    # Store raw API objects exactly as received.  Only filter out unlisted jobs
    # (isListed guard above) and jobs with no title (unpublished drafts).  All
    # other fields (jobUrl, location, secondaryLocations, department,
    # descriptionHtml, descriptionPlain, workplaceType, compensation, etc.) are
    # preserved verbatim so normalize_ashby can work from the pristine source.
    jobs = [raw for raw in raw_jobs if (raw.get("title") or "").strip()]

    # Ashby's posting-api response has no meta.total equivalent, so we use a
    # regression heuristic instead: compare today's count to this company's
    # most recent successful scrape and trip on a hard zero-floor or a >50%
    # drop.  Trip routes through the same RuntimeError → "transient" sentinel
    # path the Greenhouse meta.total guard uses, so the board is skipped this
    # run only — no S3 write, no staging insert, no auto-disable.
    today_count = len(jobs)
    baseline = _get_baseline_scraped_jobs(company["name"])

    if today_count == 0 and baseline is not None and baseline > 0:
        send_alert(
            f"[Ashby] {company['name']}: zero-floor trip — fetched 0 jobs "
            f"vs baseline={baseline}. Skipping this run."
        )
        raise RuntimeError(
            f"{company['name']}: Ashby completeness guard zero-floor "
            f"(today_count=0, baseline={baseline})."
        )
    if (
        baseline is not None
        and baseline >= 10
        and today_count < baseline * 0.5
    ):
        send_alert(
            f"[Ashby] {company['name']}: percentage-drop trip — fetched "
            f"{today_count} jobs vs baseline={baseline} (<50%). "
            f"Skipping this run."
        )
        raise RuntimeError(
            f"{company['name']}: Ashby completeness guard percentage-drop "
            f"(today_count={today_count}, baseline={baseline})."
        )

    return {"company_name": company["name"], "jobs": jobs, "meta_total": None}

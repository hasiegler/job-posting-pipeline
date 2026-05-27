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

    jobs = []
    for raw in raw_jobs:
        title = (raw.get("title") or "").strip()
        if not title:
            continue

        department = raw.get("department")
        departments = [department] if department else []

        remote_policy = _WORKPLACE_TYPE_MAP.get(raw.get("workplaceType"))
        salary = _extract_salary(raw.get("compensation"))

        jobs.append(
            {
                "id": raw.get("id"),
                "title": title,
                "url": raw.get("jobUrl"),
                "location": _combine_locations(raw),
                "departments": departments,
                "offices": [],
                "content_html": raw.get("descriptionHtml"),
                "content_text": raw.get("descriptionPlain"),
                "language": None,
                "first_published": raw.get("publishedAt"),
                "updated_at": None,
                "remote_policy": remote_policy,
                **salary,
            }
        )

    # Ashby's posting-api response has no meta.total equivalent, so there's no
    # partial-response guardrail to apply here.
    return {"company_name": company["name"], "jobs": jobs, "meta_total": None}

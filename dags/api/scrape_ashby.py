"""
Scrape Ashby job boards via the public posting API.

Endpoint pattern:
    https://api.ashbyhq.com/posting-api/job-board/{board_token}?includeCompensation=true

This module only fetches and guards: it stores raw Ashby posting objects
verbatim so the pristine payload lands in S3.  The Ashby-vs-Greenhouse shape
differences (concatenating `location` with `secondaryLocations`, mapping
`workplaceType` OnSite -> "On-Site", and reading salary off `compensation`)
are reconciled downstream by `normalize_ashby` in
`datawarehouse/data_modification.py`, which is what lets the rest of the
pipeline stay scraper-agnostic.
"""

from typing import Optional
from urllib.parse import urlparse

import requests

from alerting import send_alert
from api.board_guards import (
    CONSECUTIVE_CONFIRMATION_DAYS,
    MIN_BASELINE_FOR_GUARD,
    AshbyGuardTrip,
    should_trip_zero_floor,
)
from datawarehouse.data_utils import run_with_db

ASHBY_API_BASE = "https://api.ashbyhq.com/posting-api/job-board"


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
    both trip rules require ``baseline >= MIN_BASELINE_FOR_GUARD``, so a small
    or zero baseline never trips either rule even when included.
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


def _get_consecutive_zero_scrapes(company_name: str) -> int:
    """Return ``companies.consecutive_zero_scrapes``, or 0 if unknown.

    ``sync_companies`` adds the column at the start of every DAG run, so it
    is present by the time a scrape task runs.  Missing-column / missing-row
    failures fall back to 0, which preserves the old skip-on-zero behavior.
    """
    def _q(conn, cur):
        cur.execute(
            """
            SELECT consecutive_zero_scrapes
            FROM companies
            WHERE company_name = %s
            """,
            (company_name,),
        )
        row = cur.fetchone()
        return int(row["consecutive_zero_scrapes"]) if row else 0

    try:
        return run_with_db(_q)
    except Exception:  # noqa: BLE001 — fail closed (keep skipping zeros)
        return 0


def extract_board_token(url: str) -> str:
    """Extract the board token (last path segment) from an Ashby board URL.

    e.g. https://jobs.ashbyhq.com/Ramp -> 'Ramp'
    """
    path = urlparse(url).path.rstrip("/")
    return path.split("/")[-1]


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
    # drop.  Both rules are gated behind MIN_BASELINE_FOR_GUARD so small,
    # noisy boards are never skipped for a legitimate 0.
    #
    # Zero-floor trips skip the board (no S3 write, no close) until the same
    # HTTP 200 + 0 jobs condition has held for CONSECUTIVE_CONFIRMATION_DAYS.
    # After that the empty payload is accepted as real so leftover jobs close,
    # but the board stays enabled.  Baseline queries filter scraped_jobs > 0,
    # so accepted zeros never become the new baseline.
    today_count = len(jobs)
    baseline = _get_baseline_scraped_jobs(company["name"])
    consecutive_zeros = (
        _get_consecutive_zero_scrapes(company["name"]) if today_count == 0 else 0
    )

    if should_trip_zero_floor(today_count, baseline, consecutive_zeros):
        send_alert(
            f"[Ashby] {company['name']}: zero-floor trip — fetched 0 jobs "
            f"vs baseline={baseline} "
            f"(day {consecutive_zeros + 1}/{CONSECUTIVE_CONFIRMATION_DAYS}). "
            f"Skipping this run."
        )
        raise AshbyGuardTrip(
            f"{company['name']}: Ashby completeness guard zero-floor "
            f"(today_count=0, baseline={baseline}).",
            skip_reason="zero_floor",
        )
    if (
        today_count == 0
        and baseline is not None
        and baseline >= MIN_BASELINE_FOR_GUARD
        and consecutive_zeros + 1 == CONSECUTIVE_CONFIRMATION_DAYS
    ):
        send_alert(
            f"[Ashby] {company['name']}: {CONSECUTIVE_CONFIRMATION_DAYS} "
            f"consecutive empty scrapes vs baseline={baseline} — treating "
            f"as a real empty board. Jobs will close; scraping continues."
        )
    if (
        baseline is not None
        and baseline >= MIN_BASELINE_FOR_GUARD
        and today_count < baseline * 0.5
        and today_count != 0
    ):
        send_alert(
            f"[Ashby] {company['name']}: percentage-drop trip — fetched "
            f"{today_count} jobs vs baseline={baseline} (<50%). "
            f"Skipping this run."
        )
        raise AshbyGuardTrip(
            f"{company['name']}: Ashby completeness guard percentage-drop "
            f"(today_count={today_count}, baseline={baseline}).",
            skip_reason="percentage_drop",
        )

    return {"company_name": company["name"], "jobs": jobs, "meta_total": None}

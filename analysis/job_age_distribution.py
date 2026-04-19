"""
job_age_distribution.py
-----------------------

What we're looking for
~~~~~~~~~~~~~~~~~~~~~~
For currently active jobs, how *old* are they? "Old" here means days since
the job was first published on the ATS — i.e. `jobs.first_published_at`,
the date a candidate would actually see on the listing.

Why NOT job_history.recorded_at
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
`MIN(job_history.recorded_at)` is bounded by when the pipeline first ran,
so it caps every job's age at the pipeline's lifetime (currently a few
weeks). That's "when we first saw it," not "how old it really is." For
ghost-job and stale-listing storytelling we need the true age, which only
the ATS-reported `first_published_at` provides.

Output
~~~~~~
- Overall: median, p75, p90, max age in days, plus age-bucket histogram.
- Per-company: same percentiles + min/max + active-job count, sorted by
  median age descending. Companies with <5 active jobs are excluded.

Empty-data handling: jobs with `first_published_at IS NULL` are excluded
from the percentile math and counted separately in the .md so the headline
numbers can't be skewed by missing dates.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from db import (
    fmt_int,
    print_table,
    readonly_cursor,
    write_csv,
    write_md,
)

QUERY_NAME = "job_age_distribution"
TITLE = "Job age distribution (active jobs, days since ATS first_published_at)"


def _overall(cur) -> dict:
    cur.execute(
        """
        SELECT
            COUNT(*)                                                                AS active_jobs,
            ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY (CURRENT_DATE - first_published_at::DATE)))::INT AS median_days,
            ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY (CURRENT_DATE - first_published_at::DATE)))::INT AS p75_days,
            ROUND(PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY (CURRENT_DATE - first_published_at::DATE)))::INT AS p90_days,
            MAX(CURRENT_DATE - first_published_at::DATE)                            AS max_days
        FROM jobs
        WHERE is_active = TRUE
          AND first_published_at IS NOT NULL
        """
    )
    return cur.fetchone() or {}


def _missing_first_published(cur) -> int:
    cur.execute(
        """
        SELECT COUNT(*) AS missing
        FROM jobs
        WHERE is_active = TRUE
          AND first_published_at IS NULL
        """
    )
    row = cur.fetchone() or {"missing": 0}
    return row["missing"] or 0


def _buckets(cur) -> list[dict]:
    """Histogram of active-job age in canonical buckets."""
    cur.execute(
        """
        WITH ages AS (
            SELECT (CURRENT_DATE - first_published_at::DATE) AS age_days
            FROM jobs
            WHERE is_active = TRUE
              AND first_published_at IS NOT NULL
        )
        SELECT bucket, COUNT(*) AS jobs
        FROM (
            SELECT CASE
                WHEN age_days <=   7 THEN '0-7d'
                WHEN age_days <=  30 THEN '8-30d'
                WHEN age_days <=  60 THEN '31-60d'
                WHEN age_days <=  90 THEN '61-90d'
                WHEN age_days <= 180 THEN '91-180d'
                ELSE '180d+'
            END AS bucket
            FROM ages
        ) b
        GROUP BY bucket
        ORDER BY CASE bucket
            WHEN '0-7d'    THEN 1
            WHEN '8-30d'   THEN 2
            WHEN '31-60d'  THEN 3
            WHEN '61-90d'  THEN 4
            WHEN '91-180d' THEN 5
            ELSE 6
        END
        """
    )
    return cur.fetchall()


def _oldest_active_jobs(cur, limit: int = 10) -> list[dict]:
    """Top-N oldest still-active postings — used as the 'is the 2,601-day
    outlier real or a bug?' spot-check. Surfaces company + title + post
    date + URL so the headline anecdote can be verified by clicking."""
    cur.execute(
        """
        SELECT
            c.company_name,
            j.title,
            j.first_published_at::DATE                             AS first_published_at,
            (CURRENT_DATE - j.first_published_at::DATE)::INT       AS age_days,
            j.location,
            j.source_url
        FROM jobs j
        JOIN companies c ON c.company_id = j.company_id
        WHERE j.is_active = TRUE
          AND j.first_published_at IS NOT NULL
        ORDER BY j.first_published_at ASC
        LIMIT %s
        """,
        (limit,),
    )
    return cur.fetchall()


def _per_company(cur) -> list[dict]:
    cur.execute(
        """
        SELECT
            c.company_name,
            COUNT(*)                                                                AS active_jobs,
            ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY (CURRENT_DATE - j.first_published_at::DATE)))::INT AS median_days,
            ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY (CURRENT_DATE - j.first_published_at::DATE)))::INT AS p75_days,
            ROUND(PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY (CURRENT_DATE - j.first_published_at::DATE)))::INT AS p90_days,
            MIN(CURRENT_DATE - j.first_published_at::DATE)                          AS min_days,
            MAX(CURRENT_DATE - j.first_published_at::DATE)                          AS max_days
        FROM jobs j
        JOIN companies c ON c.company_id = j.company_id
        WHERE j.is_active = TRUE
          AND j.first_published_at IS NOT NULL
        GROUP BY c.company_name
        HAVING COUNT(*) >= 5
        ORDER BY median_days DESC NULLS LAST, active_jobs DESC
        """
    )
    return cur.fetchall()


def main() -> int:
    bullets: list[str] = []

    with readonly_cursor() as cur:
        overall = _overall(cur)
        missing = _missing_first_published(cur)
        buckets = _buckets(cur)
        per_co = _per_company(cur)
        oldest = _oldest_active_jobs(cur, limit=10)

        if not overall.get("active_jobs"):
            print(f"[{QUERY_NAME}] No active jobs with first_published_at.")
            write_csv(QUERY_NAME, [], fieldnames=[
                "company_name", "active_jobs", "median_days",
                "p75_days", "p90_days", "min_days", "max_days",
            ])
            write_md(QUERY_NAME, TITLE, [
                "No active jobs have a `first_published_at` value yet — nothing to measure."
            ])
            return 0

        print(f"== Overall job age (active jobs, ATS first_published_at) ==")
        print(
            f"  active jobs: {fmt_int(overall['active_jobs'])}"
            f"   |  median: {overall['median_days']}d"
            f"   |  p75: {overall['p75_days']}d"
            f"   |  p90: {overall['p90_days']}d"
            f"   |  max: {overall['max_days']}d"
        )
        if missing:
            print(f"  ({fmt_int(missing)} active jobs have no first_published_at and are excluded.)")
        print()

        print("Age distribution buckets:")
        print_table(buckets, ["bucket", "jobs"], limit=10)
        print()

        print("Top 10 companies by median job age (≥5 active jobs):")
        print_table(per_co, ["company_name", "active_jobs", "median_days", "p75_days", "p90_days", "max_days"])
        print()

        print("Top 10 OLDEST individual active postings (spot-check):")
        print_table(oldest, ["company_name", "title", "first_published_at", "age_days"])

        bullets.append(
            f"Across {fmt_int(overall['active_jobs'])} active jobs (anchored to ATS "
            f"`first_published_at`), the median age is {overall['median_days']} days, "
            f"p75 is {overall['p75_days']} days, p90 is {overall['p90_days']} days, "
            f"and the oldest open posting is {overall['max_days']} days old."
        )
        if buckets:
            bucket_str = ", ".join(f"{b['bucket']}: {fmt_int(b['jobs'])}" for b in buckets)
            bullets.append(f"Active-job age histogram — {bucket_str}.")
        if overall.get("p90_days") and overall["p90_days"] >= 60:
            bullets.append(
                f"10% of all active postings are older than {overall['p90_days']} days — "
                "a meaningful long-tail of stale listings worth quoting."
            )
        if per_co:
            top = per_co[:3]
            names = ", ".join(f"{r['company_name']} (median {r['median_days']}d)" for r in top)
            bullets.append(f"Companies with the oldest median listings: {names}.")
            youngest = sorted(per_co, key=lambda r: (r["median_days"] or 0))[:3]
            names = ", ".join(f"{r['company_name']} (median {r['median_days']}d)" for r in youngest)
            bullets.append(f"Companies with the freshest median listings: {names}.")
        if missing:
            bullets.append(
                f"{fmt_int(missing)} active jobs have no ATS `first_published_at` and "
                "were excluded from the percentile math."
            )

    tables = [
        {
            "caption": "Active-job age histogram",
            "headers": ["Bucket", "Active jobs"],
            "rows": [[b["bucket"], fmt_int(b["jobs"])] for b in buckets],
        },
        {
            "caption": "Top 10 oldest individual active postings (spot-check the 2,601-day outlier)",
            "headers": ["Company", "Title", "Posted (ATS)", "Age (days)", "Location", "URL"],
            "rows": [
                [
                    r["company_name"],
                    r["title"],
                    r["first_published_at"],
                    fmt_int(r["age_days"]),
                    r.get("location") or "",
                    r.get("source_url") or "",
                ]
                for r in oldest
            ],
        },
        {
            "caption": "Top 15 companies by median active-job age (≥5 active jobs)",
            "headers": ["Company", "Active jobs", "Median (d)", "p75 (d)", "p90 (d)", "Max (d)"],
            "rows": [
                [
                    r["company_name"],
                    fmt_int(r["active_jobs"]),
                    r["median_days"],
                    r["p75_days"],
                    r["p90_days"],
                    r["max_days"],
                ]
                for r in per_co[:15]
            ],
        },
    ]

    extra = [
        "> **Spot-check note:** the oldest-postings table above is meant to be "
        "verified by clicking through the URLs. If a 5+ year old role resolves "
        "to a real, live ATS page, it is the marketing anecdote. If it 404s or "
        "is obviously a template/perpetual req, treat it as a data-quality "
        "footnote rather than a quote."
    ]

    write_csv(
        QUERY_NAME,
        per_co,
        fieldnames=[
            "company_name", "active_jobs", "median_days",
            "p75_days", "p90_days", "min_days", "max_days",
        ],
    )
    write_md(QUERY_NAME, TITLE, bullets, tables=tables, extra=extra)
    return 0


if __name__ == "__main__":
    sys.exit(main())

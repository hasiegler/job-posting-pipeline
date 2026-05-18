"""
freshest_vs_stalest_companies.py
--------------------------------

What we're looking for
~~~~~~~~~~~~~~~~~~~~~~
Median age of currently-active jobs per company, sorted from stalest to
freshest. This is the "single number" answer to the question "which
employers genuinely refresh their board, and which ones leave dead jobs
sitting open?".

We require ≥20 active jobs per company to filter out small, noisy boards
(per the marketing brief). Age is computed strictly from
`jobs.first_published_at` — the ATS-reported post date that candidates
actually see. We deliberately do NOT fall back to `job_history` minimums:
those are bounded by the pipeline's lifetime and would understate the
true age of long-lived listings, breaking the entire freshness story.
Jobs with `first_published_at IS NULL` are excluded and counted.
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

QUERY_NAME = "freshest_vs_stalest_companies"
TITLE = "Freshest vs stalest companies (median age of active jobs)"
MIN_ACTIVE = 20


def _query(cur) -> list[dict]:
    cur.execute(
        """
        SELECT
            c.company_name,
            COUNT(*)                                                                       AS active_jobs,
            ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY (CURRENT_DATE - j.first_published_at::DATE)))::INT AS median_days,
            ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY (CURRENT_DATE - j.first_published_at::DATE)))::INT AS p75_days,
            MIN(CURRENT_DATE - j.first_published_at::DATE)                                 AS min_days,
            MAX(CURRENT_DATE - j.first_published_at::DATE)                                 AS max_days,
            (SELECT COUNT(*) FROM jobs j2
              WHERE j2.company_id = c.company_id
                AND j2.is_active = TRUE
                AND j2.first_published_at IS NULL)                                         AS unknown_age
        FROM jobs j
        JOIN companies c ON c.company_id = j.company_id
        WHERE j.is_active = TRUE
          AND j.first_published_at IS NOT NULL
        GROUP BY c.company_id, c.company_name
        HAVING COUNT(*) >= %s
        ORDER BY median_days DESC NULLS LAST
        """,
        (MIN_ACTIVE,),
    )
    return cur.fetchall()


def main() -> int:
    bullets: list[str] = []

    with readonly_cursor() as cur:
        rows = _query(cur)

        if not rows:
            print(f"[{QUERY_NAME}] No companies with ≥{MIN_ACTIVE} active jobs.")
            write_csv(QUERY_NAME, [], fieldnames=[
                "company_name", "active_jobs", "median_days",
                "p75_days", "min_days", "max_days", "unknown_age",
            ])
            write_md(QUERY_NAME, TITLE, [
                f"No companies meet the ≥{MIN_ACTIVE} active-jobs threshold yet.",
            ])
            return 0

        print(f"== {len(rows)} companies with ≥{MIN_ACTIVE} active jobs ==\n")

        print("Stalest 10 (highest median job age):")
        print_table(rows, ["company_name", "active_jobs", "median_days", "p75_days", "max_days"])
        print()

        freshest = sorted(rows, key=lambda r: (r["median_days"] or 0))
        print("Freshest 10 (lowest median job age):")
        print_table(freshest, ["company_name", "active_jobs", "median_days", "p75_days", "max_days"])
        print()

        stalest_top = rows[:3]
        freshest_top = freshest[:3]

        bullets.append(
            f"Of {fmt_int(len(rows))} companies with ≥{MIN_ACTIVE} active jobs, the "
            f"median listing age ranges from {freshest[0]['median_days']}d "
            f"({freshest[0]['company_name']}) to {rows[0]['median_days']}d "
            f"({rows[0]['company_name']})."
        )
        if stalest_top:
            names = ", ".join(f"{r['company_name']} (median {r['median_days']}d)" for r in stalest_top)
            bullets.append(f"Stalest boards: {names}.")
        if freshest_top:
            names = ", ".join(f"{r['company_name']} (median {r['median_days']}d)" for r in freshest_top)
            bullets.append(f"Freshest boards: {names}.")

        very_stale = [r for r in rows if (r["median_days"] or 0) >= 60]
        if very_stale:
            bullets.append(
                f"{fmt_int(len(very_stale))} companies have a median active-job age "
                "of 60+ days — half their listings are at least two months old."
            )

        unknowns = sum((r.get("unknown_age") or 0) for r in rows)
        if unknowns:
            bullets.append(
                f"{fmt_int(unknowns)} active jobs at these companies have no ATS "
                "`first_published_at` value and were excluded from the percentile math."
            )

    stalest_15 = rows[:15]
    freshest_10 = freshest[:10]

    tables = [
        {
            "caption": f"Stalest 15 boards (≥{MIN_ACTIVE} active jobs, sorted by median age)",
            "headers": ["Company", "Active jobs", "Median (d)", "p75 (d)", "Max (d)"],
            "rows": [
                [
                    r["company_name"],
                    fmt_int(r["active_jobs"]),
                    r["median_days"],
                    r["p75_days"],
                    r["max_days"],
                ]
                for r in stalest_15
            ],
        },
        {
            "caption": f"Freshest 10 boards (≥{MIN_ACTIVE} active jobs)",
            "headers": ["Company", "Active jobs", "Median (d)", "p75 (d)", "Max (d)"],
            "rows": [
                [
                    r["company_name"],
                    fmt_int(r["active_jobs"]),
                    r["median_days"],
                    r["p75_days"],
                    r["max_days"],
                ]
                for r in freshest_10
            ],
        },
    ]

    write_csv(
        QUERY_NAME,
        rows,
        fieldnames=[
            "company_name", "active_jobs", "median_days",
            "p75_days", "min_days", "max_days", "unknown_age",
        ],
    )
    write_md(QUERY_NAME, TITLE, bullets, tables=tables)
    return 0


if __name__ == "__main__":
    sys.exit(main())

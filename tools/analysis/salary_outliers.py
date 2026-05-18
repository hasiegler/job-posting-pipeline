"""
salary_outliers.py
------------------

What we're looking for
~~~~~~~~~~~~~~~~~~~~~~
Companies whose median salary is meaningfully above or below the
cross-company median. We use the latest `company_stats` snapshot
(snapshot_date = MAX) so this matches what the product surfaces. The
pipeline already restricts these aggregates to USD / yearly listings.

Output
~~~~~~
- Cross-company median of median_salary_min and median_salary_max.
- Outliers in both directions: ≥ +25% above the cross-company median, or
  ≤ –25% below it. Plus the top/bottom 10 by absolute median_salary_max.

Empty-data handling: companies with too few salary-bearing jobs to compute
medians have NULLs in `company_stats` — they're excluded from outlier math
but reported as a count.
"""

from __future__ import annotations

import statistics
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from db import (
    fmt_int,
    fmt_money,
    print_table,
    readonly_cursor,
    write_csv,
    write_md,
)

QUERY_NAME = "salary_outliers"
TITLE = "Company salary outliers (vs cross-company median)"


def _query(cur) -> list[dict]:
    cur.execute(
        """
        WITH latest AS (
            SELECT MAX(snapshot_date) AS d FROM company_stats
        )
        SELECT
            c.company_name,
            cs.snapshot_date,
            cs.active_jobs,
            cs.median_salary_min,
            cs.median_salary_max,
            cs.avg_salary_min,
            cs.avg_salary_max
        FROM company_stats cs
        JOIN companies c ON c.company_id = cs.company_id
        WHERE cs.snapshot_date = (SELECT d FROM latest)
        ORDER BY cs.median_salary_max DESC NULLS LAST
        """
    )
    return cur.fetchall()


def _safe_median(values: list[float]) -> float | None:
    values = [float(v) for v in values if v is not None]
    if not values:
        return None
    return statistics.median(values)


def main() -> int:
    bullets: list[str] = []

    with readonly_cursor() as cur:
        rows = _query(cur)

        if not rows:
            print(f"[{QUERY_NAME}] company_stats is empty.")
            write_csv(QUERY_NAME, [], fieldnames=[
                "company_name", "snapshot_date", "active_jobs",
                "median_salary_min", "median_salary_max", "avg_salary_min", "avg_salary_max",
                "max_vs_global_pct",
            ])
            write_md(QUERY_NAME, TITLE, [
                "company_stats has no rows yet — pipeline analytics step hasn't populated this table."
            ])
            return 0

        with_max = [r for r in rows if r["median_salary_max"] is not None]
        without_max = len(rows) - len(with_max)

        if not with_max:
            print(f"[{QUERY_NAME}] No companies have a median_salary_max yet.")
            write_csv(QUERY_NAME, rows, fieldnames=[
                "company_name", "snapshot_date", "active_jobs",
                "median_salary_min", "median_salary_max", "avg_salary_min", "avg_salary_max",
            ])
            write_md(QUERY_NAME, TITLE, [
                "No companies in the latest snapshot have enough USD/yearly salary data "
                "to compute a median yet — outlier comparison skipped.",
            ])
            return 0

        global_med_max = _safe_median([r["median_salary_max"] for r in with_max])
        global_med_min = _safe_median([r["median_salary_min"] for r in with_max if r["median_salary_min"] is not None])

        for r in with_max:
            r["max_vs_global_pct"] = round(
                100.0 * (float(r["median_salary_max"]) - global_med_max) / global_med_max, 1
            )

        snapshot_date = rows[0]["snapshot_date"]
        print(f"== Snapshot date: {snapshot_date} ==")
        print(
            f"  Companies with median: {fmt_int(len(with_max))} of {fmt_int(len(rows))}  "
            f"({fmt_int(without_max)} have no salary medians yet)"
        )
        print(
            f"  Cross-company median of median_salary_min: {fmt_money(global_med_min)}"
        )
        print(
            f"  Cross-company median of median_salary_max: {fmt_money(global_med_max)}"
        )
        print()

        top_paying = sorted(with_max, key=lambda r: r["median_salary_max"], reverse=True)[:10]
        low_paying = sorted(with_max, key=lambda r: r["median_salary_max"])[:10]
        high_outliers = [r for r in with_max if (r["max_vs_global_pct"] or 0) >= 25]
        low_outliers = [r for r in with_max if (r["max_vs_global_pct"] or 0) <= -25]

        print("Top 10 by median_salary_max:")
        print_table(top_paying, ["company_name", "active_jobs", "median_salary_min", "median_salary_max", "max_vs_global_pct"])
        print()
        print("Bottom 10 by median_salary_max:")
        print_table(low_paying, ["company_name", "active_jobs", "median_salary_min", "median_salary_max", "max_vs_global_pct"])
        print()
        print(f"High outliers (≥ +25% vs cross-company median): {len(high_outliers)}")
        print(f"Low  outliers (≤ –25% vs cross-company median): {len(low_outliers)}")

        bullets.append(
            f"Cross-company median of median_salary_max is {fmt_money(global_med_max)} "
            f"(snapshot {snapshot_date}, {fmt_int(len(with_max))} companies)."
        )
        if top_paying:
            names = ", ".join(
                f"{r['company_name']} ({fmt_money(r['median_salary_max'])})"
                for r in top_paying[:3]
            )
            bullets.append(f"Highest-paying medians: {names}.")
        if low_paying:
            names = ", ".join(
                f"{r['company_name']} ({fmt_money(r['median_salary_max'])})"
                for r in low_paying[:3]
            )
            bullets.append(f"Lowest-paying medians: {names}.")
        if high_outliers:
            bullets.append(
                f"{fmt_int(len(high_outliers))} companies pay 25%+ above the "
                "cross-company median at the top of band."
            )
        if low_outliers:
            bullets.append(
                f"{fmt_int(len(low_outliers))} companies pay 25%+ below the "
                "cross-company median at the top of band."
            )
        if without_max:
            bullets.append(
                f"{fmt_int(without_max)} tracked companies have no USD/yearly salary "
                "data in the current snapshot and were excluded."
            )

    tables = [
        {
            "caption": "Top 10 by median_salary_max",
            "headers": ["Company", "Active jobs", "Median min", "Median max", "Δ vs global %"],
            "rows": [
                [
                    r["company_name"],
                    fmt_int(r["active_jobs"]),
                    fmt_money(r["median_salary_min"]),
                    fmt_money(r["median_salary_max"]),
                    f"{r['max_vs_global_pct']:+.1f}%",
                ]
                for r in top_paying
            ],
        },
        {
            "caption": "Bottom 10 by median_salary_max",
            "headers": ["Company", "Active jobs", "Median min", "Median max", "Δ vs global %"],
            "rows": [
                [
                    r["company_name"],
                    fmt_int(r["active_jobs"]),
                    fmt_money(r["median_salary_min"]),
                    fmt_money(r["median_salary_max"]),
                    f"{r['max_vs_global_pct']:+.1f}%",
                ]
                for r in low_paying
            ],
        },
    ]

    write_csv(
        QUERY_NAME,
        with_max,
        fieldnames=[
            "company_name", "snapshot_date", "active_jobs",
            "median_salary_min", "median_salary_max",
            "avg_salary_min", "avg_salary_max", "max_vs_global_pct",
        ],
    )
    write_md(QUERY_NAME, TITLE, bullets, tables=tables)
    return 0


if __name__ == "__main__":
    sys.exit(main())

"""
remote_hybrid_onsite_split.py
-----------------------------

What we're looking for
~~~~~~~~~~~~~~~~~~~~~~
The remote / hybrid / on-site mix per company in the latest snapshot, plus
the overall distribution. Useful for stories like "These N% of tech companies
are 'remote-first' on paper but only X% of their open jobs are remote."

Outliers are flagged at both ends:
  - Heavy remote (≥ 50% of classified jobs are Remote).
  - Heavy on-site / no-remote (0 Remote AND ≥ 20 active jobs).

Source: latest `company_stats` snapshot. Note that some active jobs have
no remote_policy extracted at all — they're included in `active_jobs`
but not in remote/hybrid/onsite counts; we surface that gap explicitly.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from db import (
    fmt_int,
    fmt_pct,
    print_table,
    readonly_cursor,
    write_csv,
    write_md,
)

QUERY_NAME = "remote_hybrid_onsite_split"
TITLE = "Remote / hybrid / on-site split by company"


def _query(cur) -> list[dict]:
    cur.execute(
        """
        WITH latest AS (SELECT MAX(snapshot_date) AS d FROM company_stats)
        SELECT
            c.company_name,
            cs.snapshot_date,
            cs.active_jobs,
            cs.remote_count,
            cs.hybrid_count,
            cs.onsite_count,
            (cs.remote_count + cs.hybrid_count + cs.onsite_count)               AS classified,
            cs.active_jobs - (cs.remote_count + cs.hybrid_count + cs.onsite_count) AS unclassified,
            ROUND(
                100.0 * cs.remote_count
                / NULLIF(cs.remote_count + cs.hybrid_count + cs.onsite_count, 0),
                1
            )                                                                    AS remote_pct,
            ROUND(
                100.0 * cs.hybrid_count
                / NULLIF(cs.remote_count + cs.hybrid_count + cs.onsite_count, 0),
                1
            )                                                                    AS hybrid_pct,
            ROUND(
                100.0 * cs.onsite_count
                / NULLIF(cs.remote_count + cs.hybrid_count + cs.onsite_count, 0),
                1
            )                                                                    AS onsite_pct
        FROM company_stats cs
        JOIN companies c ON c.company_id = cs.company_id
        WHERE cs.snapshot_date = (SELECT d FROM latest)
        ORDER BY cs.active_jobs DESC
        """
    )
    return cur.fetchall()


def main() -> int:
    bullets: list[str] = []

    with readonly_cursor() as cur:
        rows = _query(cur)

        if not rows:
            print(f"[{QUERY_NAME}] company_stats has no rows.")
            write_csv(QUERY_NAME, [], fieldnames=[
                "company_name", "snapshot_date", "active_jobs",
                "remote_count", "hybrid_count", "onsite_count",
                "classified", "unclassified",
                "remote_pct", "hybrid_pct", "onsite_pct",
            ])
            write_md(QUERY_NAME, TITLE, [
                "company_stats is empty — pipeline analytics step hasn't populated it.",
            ])
            return 0

        snapshot_date = rows[0]["snapshot_date"]
        total_active = sum(r["active_jobs"] or 0 for r in rows)
        total_remote = sum(r["remote_count"] or 0 for r in rows)
        total_hybrid = sum(r["hybrid_count"] or 0 for r in rows)
        total_onsite = sum(r["onsite_count"] or 0 for r in rows)
        total_classified = total_remote + total_hybrid + total_onsite
        total_unclassified = total_active - total_classified

        def pct(part: int) -> float | None:
            return (100.0 * part / total_classified) if total_classified else None

        print(f"== Snapshot date: {snapshot_date} ==")
        print(f"  total active: {fmt_int(total_active)}  |  classified: {fmt_int(total_classified)}  |  unclassified: {fmt_int(total_unclassified)}")
        print(
            f"  Remote: {fmt_int(total_remote)} ({fmt_pct(pct(total_remote))})  "
            f"|  Hybrid: {fmt_int(total_hybrid)} ({fmt_pct(pct(total_hybrid))})  "
            f"|  On-site: {fmt_int(total_onsite)} ({fmt_pct(pct(total_onsite))})"
        )
        print()

        sized = [r for r in rows if (r["active_jobs"] or 0) >= 20 and (r["classified"] or 0) > 0]
        most_remote = sorted(sized, key=lambda r: (r["remote_pct"] or 0), reverse=True)[:10]
        no_remote = [r for r in sized if (r["remote_count"] or 0) == 0]
        heavy_onsite = sorted(sized, key=lambda r: (r["onsite_pct"] or 0), reverse=True)[:10]

        print("Most remote-heavy (≥20 active jobs):")
        print_table(most_remote, ["company_name", "active_jobs", "remote_pct", "hybrid_pct", "onsite_pct"])
        print()
        print("Most on-site-heavy (≥20 active jobs):")
        print_table(heavy_onsite, ["company_name", "active_jobs", "remote_pct", "hybrid_pct", "onsite_pct"])
        print()
        print(f"Companies with ≥20 active jobs and ZERO remote roles: {len(no_remote)}")
        if no_remote:
            print_table(no_remote, ["company_name", "active_jobs", "hybrid_pct", "onsite_pct"], limit=10)

        bullets.append(
            f"Across {fmt_int(len(rows))} companies in the {snapshot_date} snapshot: "
            f"Remote {fmt_pct(pct(total_remote))}, Hybrid {fmt_pct(pct(total_hybrid))}, "
            f"On-site {fmt_pct(pct(total_onsite))} of classified active jobs "
            f"({fmt_int(total_classified)} classified, {fmt_int(total_unclassified)} unclassified)."
        )
        if most_remote:
            names = ", ".join(
                f"{r['company_name']} ({fmt_pct(r['remote_pct'])})"
                for r in most_remote[:3]
            )
            bullets.append(f"Most remote-heavy boards (≥20 active jobs): {names}.")
        if no_remote:
            sample = ", ".join(
                f"{r['company_name']} ({fmt_int(r['active_jobs'])} open)"
                for r in no_remote[:5]
            )
            bullets.append(
                f"{fmt_int(len(no_remote))} companies with ≥20 active jobs offer ZERO "
                f"remote roles — examples: {sample}."
            )
        if total_unclassified > 0:
            unclass_pct = 100.0 * total_unclassified / total_active if total_active else 0
            bullets.append(
                f"{fmt_pct(unclass_pct)} of active jobs ({fmt_int(total_unclassified)}) "
                "have no remote_policy extracted — extraction coverage gap to flag."
            )

    tables = [
        {
            "caption": "Most remote-heavy boards (≥20 active jobs)",
            "headers": ["Company", "Active jobs", "Remote %", "Hybrid %", "On-site %"],
            "rows": [
                [
                    r["company_name"],
                    fmt_int(r["active_jobs"]),
                    fmt_pct(r["remote_pct"]),
                    fmt_pct(r["hybrid_pct"]),
                    fmt_pct(r["onsite_pct"]),
                ]
                for r in most_remote
            ],
        },
        {
            "caption": "Most on-site-heavy boards (≥20 active jobs)",
            "headers": ["Company", "Active jobs", "Remote %", "Hybrid %", "On-site %"],
            "rows": [
                [
                    r["company_name"],
                    fmt_int(r["active_jobs"]),
                    fmt_pct(r["remote_pct"]),
                    fmt_pct(r["hybrid_pct"]),
                    fmt_pct(r["onsite_pct"]),
                ]
                for r in heavy_onsite
            ],
        },
        {
            "caption": "Companies with ≥20 active jobs and ZERO remote roles",
            "headers": ["Company", "Active jobs", "Hybrid %", "On-site %"],
            "rows": [
                [
                    r["company_name"],
                    fmt_int(r["active_jobs"]),
                    fmt_pct(r["hybrid_pct"]),
                    fmt_pct(r["onsite_pct"]),
                ]
                for r in no_remote
            ],
        },
    ]

    write_csv(
        QUERY_NAME,
        rows,
        fieldnames=[
            "company_name", "snapshot_date", "active_jobs",
            "remote_count", "hybrid_count", "onsite_count",
            "classified", "unclassified",
            "remote_pct", "hybrid_pct", "onsite_pct",
        ],
    )
    write_md(QUERY_NAME, TITLE, bullets, tables=tables)
    return 0


if __name__ == "__main__":
    sys.exit(main())

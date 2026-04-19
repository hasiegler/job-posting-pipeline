"""
ghost_jobs_by_inactivity.py
---------------------------

What we're looking for
~~~~~~~~~~~~~~~~~~~~~~
"Ghost jobs" — postings that are still listed as open (is_active = TRUE on
the company's ATS) but show no signs of actually being maintained: no edits,
no description changes, no reactivation, no insert/update event of any kind
in the last N days.

Why job_history is the right source
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
The pipeline writes a row to `job_history` *only when something about the
job changes* (insert / update / reactivated / closed / seed). It does NOT
write a row on every scrape. So `MAX(recorded_at)` per job is the true
"last touched" timestamp from a candidate's point of view. `jobs.last_seen`
is updated every run and is therefore useless for this question.

Output
~~~~~~
Two windows: 60 days and 90 days of inactivity.
- Overall counts and % of active jobs that qualify as ghosts.
- Per-company breakdown sorted by ghost-rate, with raw counts.

If `job_history` is younger than the window, that window is reported as
N/A in the markdown summary (instead of misleading "100% ghost jobs").
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from db import (
    fmt_int,
    fmt_pct,
    pipeline_history_age_days,
    print_table,
    readonly_cursor,
    write_csv,
    write_md,
)

QUERY_NAME = "ghost_jobs_by_inactivity"
TITLE = "Ghost jobs by inactivity (60d / 90d)"
WINDOWS = (60, 90)


def _per_company(cur, days: int) -> list[dict]:
    cur.execute(
        """
        WITH last_touch AS (
            SELECT j.job_id, j.company_id, MAX(h.recorded_at) AS last_event
            FROM jobs j
            LEFT JOIN job_history h ON h.job_id = j.job_id
            WHERE j.is_active = TRUE
            GROUP BY j.job_id, j.company_id
        )
        SELECT
            c.company_name,
            COUNT(*)                                                          AS active_jobs,
            COUNT(*) FILTER (
                WHERE last_event IS NULL
                   OR last_event < NOW() - (%s || ' days')::INTERVAL
            )                                                                 AS ghost_jobs,
            ROUND(
                100.0 * COUNT(*) FILTER (
                    WHERE last_event IS NULL
                       OR last_event < NOW() - (%s || ' days')::INTERVAL
                ) / NULLIF(COUNT(*), 0),
                1
            )                                                                 AS ghost_pct
        FROM last_touch lt
        JOIN companies c ON c.company_id = lt.company_id
        GROUP BY c.company_name
        HAVING COUNT(*) >= 5
        ORDER BY ghost_pct DESC NULLS LAST, active_jobs DESC
        """,
        (days, days),
    )
    return cur.fetchall()


def _overall(cur, days: int) -> dict:
    cur.execute(
        """
        WITH last_touch AS (
            SELECT j.job_id, MAX(h.recorded_at) AS last_event
            FROM jobs j
            LEFT JOIN job_history h ON h.job_id = j.job_id
            WHERE j.is_active = TRUE
            GROUP BY j.job_id
        )
        SELECT
            COUNT(*)                                                          AS active_jobs,
            COUNT(*) FILTER (
                WHERE last_event IS NULL
                   OR last_event < NOW() - (%s || ' days')::INTERVAL
            )                                                                 AS ghost_jobs
        FROM last_touch
        """,
        (days,),
    )
    row = cur.fetchone() or {"active_jobs": 0, "ghost_jobs": 0}
    active = row["active_jobs"] or 0
    ghosts = row["ghost_jobs"] or 0
    pct = (100.0 * ghosts / active) if active else None
    return {"window_days": days, "active_jobs": active, "ghost_jobs": ghosts, "ghost_pct": pct}


def main() -> int:
    csv_rows: list[dict] = []
    bullets: list[str] = []

    with readonly_cursor() as cur:
        history_age = pipeline_history_age_days(cur)
        if history_age is None:
            print(f"[{QUERY_NAME}] job_history is empty — nothing to compute.")
            write_csv(QUERY_NAME, [], fieldnames=["window_days", "company_name", "active_jobs", "ghost_jobs", "ghost_pct"])
            write_md(QUERY_NAME, TITLE, ["job_history is empty — no inactivity signal yet."])
            return 0

        print(f"[{QUERY_NAME}] job_history covers ~{history_age} days of events.\n")

        for days in WINDOWS:
            overall = _overall(cur, days)
            if history_age < days:
                msg = (
                    f"{days}d window: SKIPPED — job_history only has {history_age} days "
                    f"of events, so 'no change in {days} days' would be true for almost "
                    "every job and would be misleading."
                )
                print(f"  {msg}\n")
                bullets.append(msg)
                continue

            bullets.append(
                f"{days}d window: {fmt_int(overall['ghost_jobs'])} of "
                f"{fmt_int(overall['active_jobs'])} active jobs "
                f"({fmt_pct(overall['ghost_pct'])}) had no edits, reactivations, or "
                "any history events — strong ghost-job signal."
            )

            per_co = _per_company(cur, days)
            print(f"== {days}-day inactivity ==")
            print(
                f"  Overall: {fmt_int(overall['ghost_jobs'])} / "
                f"{fmt_int(overall['active_jobs'])} active jobs "
                f"({fmt_pct(overall['ghost_pct'])})"
            )
            print(f"  Top 10 companies by ghost-rate (min 5 active jobs):")
            print_table(per_co, ["company_name", "active_jobs", "ghost_jobs", "ghost_pct"])
            print()

            for r in per_co:
                csv_rows.append(
                    {
                        "window_days": days,
                        "company_name": r["company_name"],
                        "active_jobs": r["active_jobs"],
                        "ghost_jobs": r["ghost_jobs"],
                        "ghost_pct": r["ghost_pct"],
                    }
                )

            top = [r for r in per_co if r["active_jobs"] >= 20][:3]
            if top:
                names = ", ".join(
                    f"{r['company_name']} ({fmt_pct(r['ghost_pct'])})" for r in top
                )
                bullets.append(
                    f"Worst offenders at {days}d (≥20 active jobs): {names}."
                )

    write_csv(
        QUERY_NAME,
        csv_rows,
        fieldnames=["window_days", "company_name", "active_jobs", "ghost_jobs", "ghost_pct"],
    )
    write_md(QUERY_NAME, TITLE, bullets)
    return 0


if __name__ == "__main__":
    sys.exit(main())

"""
close_reactivation_patterns.py
------------------------------

What we're looking for
~~~~~~~~~~~~~~~~~~~~~~
Listing churn behavior from `job_history`:

  - How often jobs get *closed* (change_type = 'closed').
  - How often jobs get *reactivated* (change_type = 'reactivated') — i.e.
    the same source_job_id was closed and then reappeared on the board.
  - Companies that churn listings the most (close + reactivate counts per
    100 jobs ever seen).

Reactivations are the marketing-relevant signal: if a job closed Monday
and reappeared Friday, that's a strong indicator the role isn't really
filling — exactly the "ghost job" angle.

Caveat
~~~~~~
Reactivation can only be observed once `job_history` is at least old enough
to have seen the close → re-open cycle. If the table is younger than ~14
days, the reactivation count is probably under-reported and we say so.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from db import (
    fmt_int,
    pipeline_history_age_days,
    print_table,
    readonly_cursor,
    write_csv,
    write_md,
)

QUERY_NAME = "close_reactivation_patterns"
TITLE = "Close / reactivation patterns (listing churn)"


def _overall(cur) -> dict:
    cur.execute(
        """
        SELECT
            COUNT(*) FILTER (WHERE change_type = 'inserted')    AS inserts,
            COUNT(*) FILTER (WHERE change_type = 'updated')     AS updates,
            COUNT(*) FILTER (WHERE change_type = 'closed')      AS closes,
            COUNT(*) FILTER (WHERE change_type = 'reactivated') AS reactivations,
            COUNT(*) FILTER (WHERE change_type = 'seed')        AS seeds,
            COUNT(*)                                            AS total_events,
            COUNT(DISTINCT job_id)                              AS jobs_touched
        FROM job_history
        """
    )
    return cur.fetchone() or {}


def _churners(cur) -> list[dict]:
    cur.execute(
        """
        WITH ev AS (
            SELECT
                h.company_id,
                COUNT(*) FILTER (WHERE h.change_type = 'closed')      AS closes,
                COUNT(*) FILTER (WHERE h.change_type = 'reactivated') AS reactivations,
                COUNT(DISTINCT h.job_id)                              AS jobs_seen
            FROM job_history h
            GROUP BY h.company_id
        )
        SELECT
            c.company_name,
            ev.jobs_seen,
            ev.closes,
            ev.reactivations,
            ROUND(100.0 * ev.closes        / NULLIF(ev.jobs_seen, 0), 1) AS closes_per_100_jobs,
            ROUND(100.0 * ev.reactivations / NULLIF(ev.jobs_seen, 0), 1) AS reactivations_per_100_jobs
        FROM ev
        JOIN companies c ON c.company_id = ev.company_id
        WHERE ev.jobs_seen >= 10
        ORDER BY reactivations DESC, closes DESC
        """
    )
    return cur.fetchall()


def _repeat_offenders(cur) -> list[dict]:
    """Specific jobs that have been reactivated 2+ times — the
    'definitely a ghost' candidates."""
    cur.execute(
        """
        SELECT
            c.company_name,
            j.title,
            j.source_url,
            COUNT(*) FILTER (WHERE h.change_type = 'closed')      AS times_closed,
            COUNT(*) FILTER (WHERE h.change_type = 'reactivated') AS times_reactivated,
            MIN(h.recorded_at) AS first_event,
            MAX(h.recorded_at) AS last_event
        FROM job_history h
        JOIN jobs j      ON j.job_id     = h.job_id
        JOIN companies c ON c.company_id = h.company_id
        GROUP BY c.company_name, j.title, j.source_url
        HAVING COUNT(*) FILTER (WHERE h.change_type = 'reactivated') >= 1
        ORDER BY times_reactivated DESC, times_closed DESC
        LIMIT 25
        """
    )
    return cur.fetchall()


def main() -> int:
    bullets: list[str] = []

    with readonly_cursor() as cur:
        history_age = pipeline_history_age_days(cur)
        if history_age is None:
            print(f"[{QUERY_NAME}] job_history is empty.")
            write_csv(QUERY_NAME, [], fieldnames=[
                "company_name", "jobs_seen", "closes", "reactivations",
                "closes_per_100_jobs", "reactivations_per_100_jobs",
            ])
            write_md(QUERY_NAME, TITLE, [
                "job_history is empty — no churn signal yet.",
            ])
            return 0

        overall = _overall(cur)
        churners = _churners(cur)
        repeats = _repeat_offenders(cur)

        print(f"[{QUERY_NAME}] job_history covers ~{history_age} days of events.\n")
        print("== Overall event mix ==")
        print(
            f"  inserts: {fmt_int(overall.get('inserts'))}  |  "
            f"updates: {fmt_int(overall.get('updates'))}  |  "
            f"closes: {fmt_int(overall.get('closes'))}  |  "
            f"reactivations: {fmt_int(overall.get('reactivations'))}  |  "
            f"seeds: {fmt_int(overall.get('seeds'))}"
        )
        print(
            f"  total events: {fmt_int(overall.get('total_events'))}  |  "
            f"unique jobs touched: {fmt_int(overall.get('jobs_touched'))}"
        )
        print()

        print("Top 10 churners (≥10 jobs ever seen, ranked by reactivations):")
        print_table(
            churners,
            ["company_name", "jobs_seen", "closes", "reactivations",
             "closes_per_100_jobs", "reactivations_per_100_jobs"],
        )
        print()

        print(f"Jobs with ≥1 reactivation event ({len(repeats)} total, top 10):")
        print_table(repeats, ["company_name", "title", "times_closed", "times_reactivated"])

        bullets.append(
            f"Across {fmt_int(history_age)} days of job_history we've recorded "
            f"{fmt_int(overall.get('closes'))} closes and "
            f"{fmt_int(overall.get('reactivations'))} reactivations across "
            f"{fmt_int(overall.get('jobs_touched'))} unique jobs."
        )
        if history_age < 14:
            bullets.append(
                f"Reactivation count is likely under-reported — job_history is only "
                f"{history_age} days old, so we haven't observed many full close→reopen cycles yet."
            )
        if churners:
            top = churners[:3]
            names = ", ".join(
                f"{r['company_name']} ({fmt_int(r['reactivations'])} reactivations / "
                f"{fmt_int(r['closes'])} closes)"
                for r in top
            )
            bullets.append(f"Highest reactivation counts: {names}.")
        if repeats:
            multi = [r for r in repeats if (r["times_reactivated"] or 0) >= 2]
            if multi:
                bullets.append(
                    f"{fmt_int(len(multi))} specific jobs have been reactivated 2+ times — "
                    "near-certain ghost-job examples worth quoting."
                )
            example = repeats[0]
            bullets.append(
                f"Example reactivated role: \"{example['title']}\" at "
                f"{example['company_name']} — closed {example['times_closed']}× and "
                f"reactivated {example['times_reactivated']}× since first seen."
            )

    tables = [
        {
            "caption": "Top 10 churners — most reactivations (≥10 jobs ever seen)",
            "headers": ["Company", "Jobs seen", "Closes", "Reactivations", "Closes/100", "React/100"],
            "rows": [
                [
                    r["company_name"],
                    fmt_int(r["jobs_seen"]),
                    fmt_int(r["closes"]),
                    fmt_int(r["reactivations"]),
                    r["closes_per_100_jobs"],
                    r["reactivations_per_100_jobs"],
                ]
                for r in churners[:10]
            ],
        },
        {
            "caption": "Top 10 individual jobs by reactivation count (ghost-job candidates)",
            "headers": ["Company", "Title", "Times closed", "Times reactivated"],
            "rows": [
                [
                    r["company_name"],
                    r["title"],
                    fmt_int(r["times_closed"]),
                    fmt_int(r["times_reactivated"]),
                ]
                for r in repeats[:10]
            ],
        },
    ]

    write_csv(
        QUERY_NAME,
        churners,
        fieldnames=[
            "company_name", "jobs_seen", "closes", "reactivations",
            "closes_per_100_jobs", "reactivations_per_100_jobs",
        ],
    )
    write_md(QUERY_NAME, TITLE, bullets, tables=tables)
    return 0


if __name__ == "__main__":
    sys.exit(main())

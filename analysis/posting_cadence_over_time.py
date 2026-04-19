"""
posting_cadence_over_time.py
----------------------------

What we're looking for
~~~~~~~~~~~~~~~~~~~~~~
Per-company weekly posting rate over the last 12 weeks. The eventual story
is "Anduril posted 150 jobs/week in April, up from 100/week in March." That
narrative needs longitudinal data we don't have yet, but the queries that
generate it should exist *now* so that:

  1. Each daily run captures a fresh snapshot of the cadence.
  2. The shape of the report doesn't change once the data is mature — only
     the y-axis fills in.
  3. Anomalies that *can* be detected today (a single-week posting spike
     that's >25% above a company's recent baseline) are surfaced, even
     when the baseline is only 4 weeks long.

Source
~~~~~~
- `jobs.first_published_at` is the ATS post date. Each posting contributes
  to exactly one ISO week bucket (`date_trunc('week', first_published_at)`).
- We use ISO weeks (Monday-start) to be unambiguous across timezones.
- Active count at end of week is taken from the `company_stats` snapshot
  closest to that week's Sunday, when available. Older weeks predate the
  pipeline and will be NULL.

Caveats
~~~~~~~
1. **Pipeline-age cliff.** Weeks that ended before the first scrape are
   under-counted (jobs posted in those weeks but already closed before
   scraping started don't exist in our DB). The summary banner exposes
   the cliff date explicitly.
2. **The 4-week trailing average is mechanical, not statistical.** A
   company that joined the tracked list 2 weeks ago will have a 2-week
   average; we mark those as "insufficient history" rather than pretending
   the comparison is valid.
3. **Recent-week vs trailing average.** We flag deviations >25% in either
   direction. With few data points this fires a lot — that's by design;
   the script's job today is to make movement visible, not to draw
   confident conclusions.
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

QUERY_NAME = "posting_cadence_over_time"
TITLE = "Posting cadence over time (weekly, last 12 weeks)"

WEEKS = 12
TRAILING_BASELINE_WEEKS = 4
ANOMALY_PCT = 25.0
SNAPSHOT_TOLERANCE_DAYS = 3
MIN_ACTIVE_FOR_SUMMARY = 5


def _query_weekly_posts(cur) -> list[dict]:
    """Posts-per-week for each company across the last `WEEKS` ISO weeks.

    The CROSS JOIN with `weeks` ensures every (company, week) pair exists
    even when posted_count is 0 — we want zeros visible, not gaps.
    """
    cur.execute(
        f"""
        WITH weeks AS (
            SELECT generate_series(
                date_trunc('week', NOW())::DATE - ((%s - 1) * 7),
                date_trunc('week', NOW())::DATE,
                INTERVAL '7 days'
            )::DATE AS week_start
        ),
        active_companies AS (
            SELECT c.company_id, c.company_name,
                   COUNT(*) FILTER (WHERE j.is_active) AS active_jobs_now
            FROM companies c
            JOIN jobs j ON j.company_id = c.company_id
            GROUP BY c.company_id, c.company_name
            HAVING COUNT(*) FILTER (WHERE j.is_active) > 0
        ),
        posts AS (
            SELECT
                j.company_id,
                date_trunc('week', j.first_published_at)::DATE AS week_start,
                COUNT(*) AS posted_count
            FROM jobs j
            WHERE j.first_published_at IS NOT NULL
              AND j.first_published_at >= date_trunc('week', NOW()) - ((%s - 1) * INTERVAL '7 days')
            GROUP BY j.company_id, week_start
        )
        SELECT
            ac.company_id,
            ac.company_name,
            ac.active_jobs_now,
            w.week_start,
            COALESCE(p.posted_count, 0) AS posted_count
        FROM active_companies ac
        CROSS JOIN weeks w
        LEFT JOIN posts p
          ON p.company_id = ac.company_id
         AND p.week_start = w.week_start
        ORDER BY ac.company_name, w.week_start
        """,
        (WEEKS, WEEKS),
    )
    return cur.fetchall()


def _query_active_snapshots(cur) -> dict[tuple[int, str], int]:
    """For each (company_id, week_start) we covered, find the closest
    `company_stats` snapshot to the week's *end* (Sunday) within tolerance.

    Keyed by (company_id, week_start ISO string) so it merges cleanly into
    the per-week rows.
    """
    cur.execute(
        f"""
        WITH weeks AS (
            SELECT generate_series(
                date_trunc('week', NOW())::DATE - ((%s - 1) * 7),
                date_trunc('week', NOW())::DATE,
                INTERVAL '7 days'
            )::DATE AS week_start
        ),
        targets AS (
            SELECT week_start, (week_start + 6) AS week_end FROM weeks
        ),
        snapshots AS (
            SELECT DISTINCT ON (cs.company_id, t.week_start)
                cs.company_id,
                t.week_start,
                cs.snapshot_date,
                cs.active_jobs
            FROM targets t
            JOIN company_stats cs
              ON cs.snapshot_date BETWEEN
                    t.week_end - {SNAPSHOT_TOLERANCE_DAYS}
                AND t.week_end + {SNAPSHOT_TOLERANCE_DAYS}
            ORDER BY cs.company_id, t.week_start,
                     ABS(cs.snapshot_date - t.week_end) ASC,
                     cs.snapshot_date DESC
        )
        SELECT company_id, week_start, active_jobs
        FROM snapshots
        """,
        (WEEKS,),
    )
    out: dict[tuple[int, str], int] = {}
    for row in cur.fetchall():
        out[(row["company_id"], row["week_start"].isoformat())] = row["active_jobs"]
    return out


def _summarize_company(weeks_for_company: list[dict]) -> dict:
    """Collapse a per-week run into a single summary row.

    `% change` compares the most recent week's posted_count to the average
    of the previous `TRAILING_BASELINE_WEEKS`. We require at least one
    non-zero baseline week so dividing by zero is avoided cleanly.
    """
    weeks_for_company = sorted(weeks_for_company, key=lambda r: r["week_start"])
    company_name = weeks_for_company[0]["company_name"]
    active_now = weeks_for_company[0]["active_jobs_now"]

    nonzero = [r for r in weeks_for_company if r["posted_count"] > 0]
    weeks_of_data = len(nonzero)

    avg_weekly = (
        sum(r["posted_count"] for r in weeks_for_company) / len(weeks_for_company)
        if weeks_for_company else 0.0
    )

    most_recent = weeks_for_company[-1]
    prior = weeks_for_company[-(TRAILING_BASELINE_WEEKS + 1):-1]

    if len(prior) < TRAILING_BASELINE_WEEKS:
        baseline = None
        pct_change = None
        pace_flag = "insufficient history"
    else:
        baseline = sum(r["posted_count"] for r in prior) / len(prior)
        if baseline == 0 and most_recent["posted_count"] == 0:
            pct_change = 0.0
            pace_flag = "flat (zero baseline)"
        elif baseline == 0:
            pct_change = None
            pace_flag = "spike from zero baseline"
        else:
            pct_change = 100.0 * (most_recent["posted_count"] - baseline) / baseline
            if pct_change >= ANOMALY_PCT:
                pace_flag = "ACCELERATING"
            elif pct_change <= -ANOMALY_PCT:
                pace_flag = "DECELERATING"
            else:
                pace_flag = "steady"

    return {
        "company_name": company_name,
        "active_jobs_now": active_now,
        "weeks_with_data": weeks_of_data,
        "avg_weekly_posts": round(avg_weekly, 2),
        "most_recent_week_start": most_recent["week_start"].isoformat(),
        "most_recent_posts": most_recent["posted_count"],
        "trailing_baseline_weeks": len(prior),
        "trailing_baseline_avg": round(baseline, 2) if baseline is not None else None,
        "pct_change_vs_baseline": round(pct_change, 1) if pct_change is not None else None,
        "pace_flag": pace_flag,
    }


def main() -> int:
    bullets: list[str] = []

    with readonly_cursor() as cur:
        history_age = pipeline_history_age_days(cur)
        per_week_rows = _query_weekly_posts(cur)
        active_snapshots = _query_active_snapshots(cur)

        if not per_week_rows:
            print(f"[{QUERY_NAME}] No active companies / no posting data.")
            write_csv(QUERY_NAME, [], fieldnames=[
                "company_name", "week_start", "posted_count",
                "active_jobs_at_week_end", "active_jobs_now",
            ])
            write_md(QUERY_NAME, TITLE, ["No active companies returned."])
            return 0

        for r in per_week_rows:
            r["active_jobs_at_week_end"] = active_snapshots.get(
                (r["company_id"], r["week_start"].isoformat())
            )

        by_company: dict[str, list[dict]] = {}
        for r in per_week_rows:
            by_company.setdefault(r["company_name"], []).append(r)

        summary_rows: list[dict] = []
        for _, runs in by_company.items():
            if any(r["active_jobs_now"] >= MIN_ACTIVE_FOR_SUMMARY for r in runs):
                summary_rows.append(_summarize_company(runs))
        summary_rows.sort(key=lambda r: r["avg_weekly_posts"], reverse=True)

        accel = [
            r for r in summary_rows
            if r["pace_flag"] == "ACCELERATING"
            and r["pct_change_vs_baseline"] is not None
        ]
        decel = [
            r for r in summary_rows
            if r["pace_flag"] == "DECELERATING"
            and r["pct_change_vs_baseline"] is not None
        ]
        accel.sort(key=lambda r: r["pct_change_vs_baseline"], reverse=True)
        decel.sort(key=lambda r: r["pct_change_vs_baseline"])

        usable_weeks = max(1, min(WEEKS, (history_age // 7) if history_age else 0))

        print(f"== Overall ==")
        print(
            f"  {len(by_company)} active companies, "
            f"{len(summary_rows)} eligible for summary "
            f"(≥{MIN_ACTIVE_FOR_SUMMARY} active jobs)."
        )
        print(
            f"  Pipeline history age: {history_age} days "
            f"(~{usable_weeks} fully-covered weeks of {WEEKS}). "
            f"Earlier weeks under-report posts that closed before scraping."
        )
        print(
            f"  Pace flags: {len(accel)} accelerating, {len(decel)} decelerating, "
            f"{sum(1 for r in summary_rows if r['pace_flag'] == 'steady')} steady, "
            f"{sum(1 for r in summary_rows if r['pace_flag'] == 'insufficient history')} "
            f"insufficient history."
        )
        print()

        print(f"Top 15 by avg weekly posts:")
        print_table(
            summary_rows[:15],
            ["company_name", "avg_weekly_posts", "most_recent_posts",
             "trailing_baseline_avg", "pct_change_vs_baseline", "pace_flag"],
        )
        print()

        if accel:
            print("Accelerating (most recent week vs 4-week trailing avg):")
            print_table(accel[:10], [
                "company_name", "most_recent_posts", "trailing_baseline_avg",
                "pct_change_vs_baseline", "active_jobs_now",
            ])
            print()
        if decel:
            print("Decelerating (most recent week vs 4-week trailing avg):")
            print_table(decel[:10], [
                "company_name", "most_recent_posts", "trailing_baseline_avg",
                "pct_change_vs_baseline", "active_jobs_now",
            ])
            print()

        bullets.append(
            f"Tracking weekly posting cadence across **{len(by_company)} companies** "
            f"over the last {WEEKS} weeks. Pipeline currently has "
            f"**{history_age} days of history** "
            f"(~{usable_weeks} fully-covered weeks); earlier weeks are floor "
            f"estimates only."
        )
        if summary_rows:
            top = summary_rows[0]
            bullets.append(
                f"Highest avg weekly posting rate: **{top['company_name']}** at "
                f"{top['avg_weekly_posts']:.1f} posts/week (most recent week: "
                f"{fmt_int(top['most_recent_posts'])})."
            )
        if accel:
            sample = ", ".join(
                f"{r['company_name']} ({fmt_pct(r['pct_change_vs_baseline'])})"
                for r in accel[:5]
            )
            bullets.append(
                f"**Accelerating** ({len(accel)} companies — recent week ≥ "
                f"+{int(ANOMALY_PCT)}% vs 4-week avg): {sample}."
            )
        if decel:
            sample = ", ".join(
                f"{r['company_name']} ({fmt_pct(r['pct_change_vs_baseline'])})"
                for r in decel[:5]
            )
            bullets.append(
                f"**Decelerating** ({len(decel)} companies — recent week ≤ "
                f"-{int(ANOMALY_PCT)}% vs 4-week avg): {sample}."
            )
        bullets.append(
            "Pace flags are intentionally noisy with this little history; treat "
            "them as 'worth a second look' rather than a conclusion."
        )

        per_week_csv = [
            {
                "company_name": r["company_name"],
                "week_start": r["week_start"].isoformat(),
                "posted_count": r["posted_count"],
                "active_jobs_at_week_end": r["active_jobs_at_week_end"],
                "active_jobs_now": r["active_jobs_now"],
            }
            for r in per_week_rows
        ]

    summary_table = {
        "caption": (
            f"Top 15 companies by avg weekly posts (last {WEEKS} weeks)"
        ),
        "headers": [
            "Company", "Active jobs (now)", "Weeks with posts",
            "Avg posts/week", "Most recent week", "Most recent posts",
            "4wk baseline avg", "% change vs baseline", "Pace flag",
        ],
        "rows": [
            [
                r["company_name"],
                fmt_int(r["active_jobs_now"]),
                fmt_int(r["weeks_with_data"]),
                f"{r['avg_weekly_posts']:.1f}",
                r["most_recent_week_start"],
                fmt_int(r["most_recent_posts"]),
                f"{r['trailing_baseline_avg']:.1f}" if r["trailing_baseline_avg"] is not None else "n/a",
                fmt_pct(r["pct_change_vs_baseline"]),
                r["pace_flag"],
            ]
            for r in summary_rows[:15]
        ],
    }

    accel_table = {
        "caption": f"Accelerating — most recent week ≥ +{int(ANOMALY_PCT)}% vs 4-week trailing avg",
        "headers": [
            "Company", "Most recent posts", "4wk baseline avg",
            "% change vs baseline", "Active jobs (now)",
        ],
        "rows": [
            [
                r["company_name"],
                fmt_int(r["most_recent_posts"]),
                f"{r['trailing_baseline_avg']:.1f}" if r["trailing_baseline_avg"] is not None else "n/a",
                fmt_pct(r["pct_change_vs_baseline"]),
                fmt_int(r["active_jobs_now"]),
            ]
            for r in accel[:15]
        ],
    }
    decel_table = {
        "caption": f"Decelerating — most recent week ≤ -{int(ANOMALY_PCT)}% vs 4-week trailing avg",
        "headers": [
            "Company", "Most recent posts", "4wk baseline avg",
            "% change vs baseline", "Active jobs (now)",
        ],
        "rows": [
            [
                r["company_name"],
                fmt_int(r["most_recent_posts"]),
                f"{r['trailing_baseline_avg']:.1f}" if r["trailing_baseline_avg"] is not None else "n/a",
                fmt_pct(r["pct_change_vs_baseline"]),
                fmt_int(r["active_jobs_now"]),
            ]
            for r in decel[:15]
        ],
    }

    banner_block = (
        "> **Heads up — this script is intentionally early.** With "
        f"{history_age} days of `job_history`, only the most recent "
        f"~{usable_weeks} of {WEEKS} weeks are fully observed. Older "
        "weeks are floor estimates because jobs posted then but closed "
        "before the pipeline started scraping aren't in the database. "
        "Don't quote these numbers publicly until the pipeline crosses "
        "60+ days of history; do let them accumulate so trajectory is "
        "available the moment it's worth talking about."
    )
    how_to_read = "\n".join([
        "### How to read",
        "",
        (
            "- **Avg posts/week** — mean posted_count across all 12 buckets, "
            "including the under-counted early ones. Treat as a lower bound."
        ),
        (
            "- **Most recent week** — the current ISO week (Mon-start). It is "
            "*in progress* on the day this report runs, so the count is "
            "guaranteed to grow until Sunday."
        ),
        (
            f"- **4wk baseline avg** — mean posted_count of the "
            f"{TRAILING_BASELINE_WEEKS} weeks immediately before the most "
            "recent week. Companies with fewer than "
            f"{TRAILING_BASELINE_WEEKS} prior weeks of data are marked "
            "'insufficient history'."
        ),
        (
            f"- **Pace flag** — ACCELERATING if recent ≥ +{int(ANOMALY_PCT)}% "
            f"vs baseline, DECELERATING if recent ≤ -{int(ANOMALY_PCT)}%, "
            "else 'steady'. Bias is toward over-flagging while the dataset "
            "is young — correct posture is 'worth a closer look', not 'story'."
        ),
    ])
    detail_block = "\n".join([
        "### Per-company weekly detail",
        "",
        (
            "Full per-(company, week) rows live in "
            "`posting_cadence_over_time.csv`. Columns: company_name, "
            "week_start (Mon), posted_count, active_jobs_at_week_end "
            "(closest snapshot to that Sunday, blank if none within ±3d), "
            "active_jobs_now."
        ),
    ])
    extras: list[str] = [banner_block, how_to_read, detail_block]

    write_csv(
        QUERY_NAME,
        per_week_csv,
        fieldnames=[
            "company_name", "week_start", "posted_count",
            "active_jobs_at_week_end", "active_jobs_now",
        ],
    )
    write_md(
        QUERY_NAME,
        TITLE,
        bullets,
        tables=[summary_table, accel_table, decel_table],
        extra=extras,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())

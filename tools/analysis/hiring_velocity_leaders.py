"""
hiring_velocity_leaders.py
--------------------------

What we're looking for
~~~~~~~~~~~~~~~~~~~~~~
Which companies are actually adding headcount fast right now, vs which are
sitting on a stale board? Two ways to slice "fast":
  - Raw count: jobs whose first_published_at is within the last 30 days.
  - Rate: that count as a % of currently active jobs (controls for size —
    a tiny company posting 5 in a month is moving faster than a megacorp
    posting 30).

We also surface *net* hiring movement (posts minus closes) and active-count
drift so we can talk about trajectory once the pipeline has enough history
to make those numbers stable.

Source
~~~~~~
- `jobs.first_published_at` is the ATS-reported post date (true post date,
  not when we first scraped). That's the signal we headline.
- `job_history` events with `change_type = 'closed'` are the source of
  truth for closure counts — the production `closed_7d`/`closed_30d`
  columns on `company_stats` are derived from `jobs.date_closed`, which
  only records the most recent close. Counting `job_history` 'closed'
  events handles reactivation cycles correctly.
- `company_stats.active_jobs` is a daily snapshot of the active count per
  company; we use the closest snapshot at-or-before (today - N days) as the
  baseline for active-count drift.

Caveats — read these before quoting numbers
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
1. Posted N-day counts under-report when the pipeline is younger than N
   days: jobs posted in the window that already closed *before* scraping
   started don't exist in our DB.
2. Closed N-day counts under-report when the pipeline is younger than N
   days: closes that happened before scraping started were never observed
   as 'closed' events.
3. Active-count drift requires a `company_stats` snapshot near (today - N
   days). If we don't have one within +/- 3 days of the target, the drift
   is left blank rather than guessed.

Sanity check: posted_window - closed_window ≈ active_now - active_then.
If they disagree by >10 jobs *and* >10%, something is being missed (most
likely closures that happened off-pipeline). We surface the worst offenders
as a footnote — not an error.
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

QUERY_NAME = "hiring_velocity_leaders"
TITLE = "Hiring velocity leaders (last 30 days)"

# How far either side of the target date we'll accept a snapshot for the
# "active jobs N days ago" baseline. company_stats is meant to be daily,
# but pipeline blips happen — 3 days of slack keeps the metric usable
# without silently quoting week-old data as "today - 7d".
SNAPSHOT_TOLERANCE_DAYS = 3

# Sanity-check threshold: posted - closed should equal active_now - active_then.
# We only flag when the gap is both large absolute (>10 jobs) AND large
# relative (>10% of net). This avoids surfacing rounding noise on tiny
# numbers.
SANITY_ABS_GAP = 10
SANITY_REL_GAP = 0.10


def _query(cur) -> list[dict]:
    """One row per company with everything needed for the velocity tables.

    All counts are derived from the same active-jobs universe so the
    sanity check (posted - closed vs Δactive) is internally consistent.
    """
    cur.execute(
        f"""
        WITH per_company AS (
            SELECT
                c.company_id,
                c.company_name,
                COUNT(*) FILTER (WHERE j.is_active)                                            AS active_jobs,
                COUNT(*) FILTER (WHERE j.first_published_at >= NOW() - INTERVAL '30 days')    AS posted_30d,
                COUNT(*) FILTER (WHERE j.first_published_at >= NOW() - INTERVAL '7 days')     AS posted_7d
            FROM companies c
            JOIN jobs j ON j.company_id = c.company_id
            GROUP BY c.company_id, c.company_name
        ),
        closes AS (
            SELECT
                company_id,
                COUNT(*) FILTER (WHERE recorded_at >= NOW() - INTERVAL '7 days')   AS closed_7d,
                COUNT(*) FILTER (WHERE recorded_at >= NOW() - INTERVAL '30 days')  AS closed_30d
            FROM job_history
            WHERE change_type = 'closed'
            GROUP BY company_id
        ),
        snap_7d AS (
            SELECT DISTINCT ON (company_id)
                company_id, snapshot_date, active_jobs AS active_jobs_7d_ago
            FROM company_stats
            WHERE snapshot_date BETWEEN
                  (CURRENT_DATE - INTERVAL '7 days')::DATE - {SNAPSHOT_TOLERANCE_DAYS}
              AND (CURRENT_DATE - INTERVAL '7 days')::DATE + {SNAPSHOT_TOLERANCE_DAYS}
            ORDER BY company_id,
                     ABS(snapshot_date - (CURRENT_DATE - INTERVAL '7 days')::DATE) ASC,
                     snapshot_date DESC
        ),
        snap_30d AS (
            SELECT DISTINCT ON (company_id)
                company_id, snapshot_date, active_jobs AS active_jobs_30d_ago
            FROM company_stats
            WHERE snapshot_date BETWEEN
                  (CURRENT_DATE - INTERVAL '30 days')::DATE - {SNAPSHOT_TOLERANCE_DAYS}
              AND (CURRENT_DATE - INTERVAL '30 days')::DATE + {SNAPSHOT_TOLERANCE_DAYS}
            ORDER BY company_id,
                     ABS(snapshot_date - (CURRENT_DATE - INTERVAL '30 days')::DATE) ASC,
                     snapshot_date DESC
        )
        SELECT
            p.company_name,
            p.active_jobs,
            p.posted_7d,
            p.posted_30d,
            COALESCE(cl.closed_7d, 0)   AS closed_7d,
            COALESCE(cl.closed_30d, 0)  AS closed_30d,
            (p.posted_7d  - COALESCE(cl.closed_7d, 0))   AS net_7d,
            (p.posted_30d - COALESCE(cl.closed_30d, 0))  AS net_30d,
            s7.active_jobs_7d_ago,
            s30.active_jobs_30d_ago,
            CASE WHEN s7.active_jobs_7d_ago  IS NULL THEN NULL
                 ELSE p.active_jobs - s7.active_jobs_7d_ago  END AS active_change_7d,
            CASE WHEN s30.active_jobs_30d_ago IS NULL THEN NULL
                 ELSE p.active_jobs - s30.active_jobs_30d_ago END AS active_change_30d,
            ROUND(
                100.0 * p.posted_30d / NULLIF(p.active_jobs, 0),
                1
            ) AS posted_30d_pct_of_active
        FROM per_company p
        LEFT JOIN closes   cl  ON cl.company_id  = p.company_id
        LEFT JOIN snap_7d  s7  ON s7.company_id  = p.company_id
        LEFT JOIN snap_30d s30 ON s30.company_id = p.company_id
        WHERE p.active_jobs > 0
        ORDER BY p.posted_30d DESC, p.active_jobs DESC
        """
    )
    return cur.fetchall()


def _sanity_mismatches(rows: list[dict], window: str) -> list[dict]:
    """Companies where (posted - closed) and Δactive disagree materially.

    `window` is "7d" or "30d". A mismatch usually means we're missing close
    events (job vanished from the source between scrapes without firing a
    'closed' transition) or we're double-counting posts.
    """
    out: list[dict] = []
    posted_key = f"posted_{window}"
    closed_key = f"closed_{window}"
    net_key = f"net_{window}"
    drift_key = f"active_change_{window}"

    for r in rows:
        drift = r.get(drift_key)
        if drift is None:
            continue
        net = r.get(net_key) or 0
        gap = drift - net
        if abs(gap) < SANITY_ABS_GAP:
            continue
        denom = max(abs(net), abs(drift), 1)
        if abs(gap) / denom < SANITY_REL_GAP:
            continue
        out.append({
            "company_name": r["company_name"],
            "active_jobs": r["active_jobs"],
            "posted": r[posted_key],
            "closed": r[closed_key],
            "net": net,
            "active_drift": drift,
            "gap": gap,
        })
    out.sort(key=lambda r: abs(r["gap"]), reverse=True)
    return out


def _net_change_section(
    rows: list[dict],
    window: str,
    history_age: int | None,
) -> tuple[str, list[dict]]:
    """Build the (caption, table) pair for one net-change leaderboard.

    Returns (heading_block, [gainers_table, shedders_table]) where
    heading_block is a markdown blob (caveat banner) and the two tables
    are db.write_md table dicts.
    """
    posted_key = f"posted_{window}"
    closed_key = f"closed_{window}"
    net_key = f"net_{window}"
    drift_key = f"active_change_{window}"

    eligible = [r for r in rows if r["active_jobs"] >= 5]
    by_net_desc = sorted(eligible, key=lambda r: (r[net_key] or 0), reverse=True)
    by_net_asc = sorted(eligible, key=lambda r: (r[net_key] or 0))

    def _row(r: dict) -> list:
        return [
            r["company_name"],
            fmt_int(r[posted_key]),
            fmt_int(r[closed_key]),
            fmt_int(r[net_key]),
            fmt_int(r["active_jobs"]),
            fmt_int(r[drift_key]) if r[drift_key] is not None else "n/a",
        ]

    headers = [
        f"Company",
        f"Posted {window}",
        f"Closed {window}",
        f"Net {window}",
        "Active jobs (now)",
        f"Δ active jobs ({window})",
    ]

    window_days = int(window.rstrip("d"))
    if history_age is None:
        banner = (
            f"_Pipeline history age unknown — closure counts for the last "
            f"{window_days} days may be incomplete._"
        )
    elif history_age < window_days:
        banner = (
            f"_Heads up: pipeline has only **{history_age} days** of `job_history` "
            f"as of this snapshot. Closes for the full {window_days}-day window are "
            f"under-reported — closures that happened before scraping started were "
            f"never observed. Treat **net {window}** as a **floor estimate** and "
            f"**Δ active jobs ({window})** as the more honest read on real "
            f"trajectory until the pipeline crosses {window_days} days of history._"
        )
    else:
        banner = (
            f"_Pipeline has {history_age} days of `job_history` — both posts and "
            f"closes are fully observed inside the {window_days}-day window._"
        )

    gainers_tbl = {
        "caption": f"Net change leaders — top 10 gainers (last {window})",
        "headers": headers,
        "rows": [_row(r) for r in by_net_desc[:10]],
    }
    shedders_tbl = {
        "caption": f"Net change leaders — top 10 shedders (last {window})",
        "headers": headers,
        "rows": [_row(r) for r in by_net_asc[:10]],
    }
    return banner, [gainers_tbl, shedders_tbl]


def main() -> int:
    bullets: list[str] = []

    with readonly_cursor() as cur:
        history_age = pipeline_history_age_days(cur)
        rows = _query(cur)

        if not rows:
            print(f"[{QUERY_NAME}] No company rows returned.")
            write_csv(QUERY_NAME, [], fieldnames=[
                "company_name", "active_jobs",
                "posted_7d", "posted_30d",
                "closed_7d", "closed_30d",
                "net_7d", "net_30d",
                "active_jobs_7d_ago", "active_jobs_30d_ago",
                "active_change_7d", "active_change_30d",
                "posted_30d_pct_of_active",
            ])
            write_md(QUERY_NAME, TITLE, ["No companies with active jobs."])
            return 0

        total_active = sum(r["active_jobs"] for r in rows)
        total_30d = sum(r["posted_30d"] for r in rows)
        total_7d = sum(r["posted_7d"] for r in rows)
        total_closed_30d = sum(r["closed_30d"] for r in rows)
        total_closed_7d = sum(r["closed_7d"] for r in rows)
        overall_pct = (100.0 * total_30d / total_active) if total_active else None

        print(f"== Overall ==")
        print(
            f"  {fmt_int(len(rows))} companies, {fmt_int(total_active)} active jobs, "
            f"{fmt_int(total_30d)} posted in last 30d ({fmt_pct(overall_pct)}), "
            f"{fmt_int(total_7d)} in last 7d."
        )
        print(
            f"  Closes (job_history): {fmt_int(total_closed_7d)} in 7d, "
            f"{fmt_int(total_closed_30d)} in 30d. "
            f"Net 7d = {fmt_int(total_7d - total_closed_7d)}, "
            f"net 30d = {fmt_int(total_30d - total_closed_30d)}."
        )
        print()

        print("Top 10 by raw count (posted_30d):")
        by_raw = sorted(rows, key=lambda r: (r["posted_30d"] or 0), reverse=True)
        print_table(by_raw, ["company_name", "posted_30d", "active_jobs", "posted_30d_pct_of_active"])
        print()

        print("Top 10 by rate (posted_30d as % of active, min 20 active):")
        sized = [r for r in rows if (r["active_jobs"] or 0) >= 20]
        by_rate = sorted(sized, key=lambda r: (r["posted_30d_pct_of_active"] or 0), reverse=True)
        print_table(by_rate, ["company_name", "posted_30d_pct_of_active", "posted_30d", "active_jobs"])
        print()

        print("Flatlines: ≥20 active jobs but 0 posted in last 30d:")
        flat = [r for r in sized if (r["posted_30d"] or 0) == 0]
        print_table(flat, ["company_name", "active_jobs", "posted_7d", "posted_30d"])
        print()

        print("Net change leaders — 7d (top 10 gainers):")
        eligible = [r for r in rows if r["active_jobs"] >= 5]
        by_net7_desc = sorted(eligible, key=lambda r: (r["net_7d"] or 0), reverse=True)
        by_net7_asc = sorted(eligible, key=lambda r: (r["net_7d"] or 0))
        print_table(
            by_net7_desc[:10],
            ["company_name", "posted_7d", "closed_7d", "net_7d", "active_jobs", "active_change_7d"],
        )
        print("Net change leaders — 7d (top 10 shedders):")
        print_table(
            by_net7_asc[:10],
            ["company_name", "posted_7d", "closed_7d", "net_7d", "active_jobs", "active_change_7d"],
        )
        print()

        print("Net change leaders — 30d (top 10 gainers):")
        by_net30_desc = sorted(eligible, key=lambda r: (r["net_30d"] or 0), reverse=True)
        by_net30_asc = sorted(eligible, key=lambda r: (r["net_30d"] or 0))
        print_table(
            by_net30_desc[:10],
            ["company_name", "posted_30d", "closed_30d", "net_30d", "active_jobs", "active_change_30d"],
        )
        print("Net change leaders — 30d (top 10 shedders):")
        print_table(
            by_net30_asc[:10],
            ["company_name", "posted_30d", "closed_30d", "net_30d", "active_jobs", "active_change_30d"],
        )
        print()

        bullets.append(
            f"Across {fmt_int(len(rows))} tracked companies, "
            f"{fmt_int(total_30d)} jobs were posted in the last 30 days — "
            f"{fmt_pct(overall_pct)} of all currently active listings."
        )
        bullets.append(
            f"Aggregate close events from `job_history`: "
            f"{fmt_int(total_closed_7d)} in last 7d, "
            f"{fmt_int(total_closed_30d)} in last 30d "
            f"→ aggregate net 7d = **{fmt_int(total_7d - total_closed_7d)}**, "
            f"aggregate net 30d = **{fmt_int(total_30d - total_closed_30d)}**."
        )
        if by_raw[:3]:
            top_raw = ", ".join(
                f"{r['company_name']} ({fmt_int(r['posted_30d'])})"
                for r in by_raw[:3]
            )
            bullets.append(f"Highest raw 30d posting volume: {top_raw}.")
        if by_rate[:3]:
            top_rate = ", ".join(
                f"{r['company_name']} ({fmt_pct(r['posted_30d_pct_of_active'])} of active)"
                for r in by_rate[:3]
            )
            bullets.append(f"Highest 30d posting rate (≥20 active jobs): {top_rate}.")
        if flat:
            sample = ", ".join(
                f"{r['company_name']} ({fmt_int(r['active_jobs'])} open)"
                for r in flat[:5]
            )
            bullets.append(
                f"Flatlines — companies with ≥20 active jobs but zero posted in the "
                f"last 30 days ({fmt_int(len(flat))} total): {sample}."
            )

        if history_age is not None:
            bullets.append(
                f"Pipeline `job_history` age at this snapshot: **{history_age} days**. "
                f"Net change is meaningful for windows ≤ this; longer windows are "
                f"floor estimates because off-pipeline closes are invisible."
            )
        if history_age is not None and history_age < 30:
            bullets.append(
                f"Caveat: pipeline has only {history_age} days of `job_history`, so "
                "any job posted in the last 30 days that closed before scraping "
                "started is missing from these counts — true 30-day volume is "
                "likely higher and **closed_30d** is under-reported."
            )

    banner_7d, tables_7d = _net_change_section(rows, "7d", history_age)
    banner_30d, tables_30d = _net_change_section(rows, "30d", history_age)

    sanity_7d = _sanity_mismatches(rows, "7d")
    sanity_30d = _sanity_mismatches(rows, "30d")

    tables = [
        {
            "caption": "Top 10 by raw 30-day posting volume",
            "headers": ["Company", "Posted 30d", "Posted 7d", "Active jobs", "30d % of active"],
            "rows": [
                [
                    r["company_name"],
                    fmt_int(r["posted_30d"]),
                    fmt_int(r["posted_7d"]),
                    fmt_int(r["active_jobs"]),
                    fmt_pct(r["posted_30d_pct_of_active"]),
                ]
                for r in by_raw[:10]
            ],
        },
        {
            "caption": "Top 10 by 30-day posting rate (≥20 active jobs)",
            "headers": ["Company", "30d % of active", "Posted 30d", "Active jobs"],
            "rows": [
                [
                    r["company_name"],
                    fmt_pct(r["posted_30d_pct_of_active"]),
                    fmt_int(r["posted_30d"]),
                    fmt_int(r["active_jobs"]),
                ]
                for r in by_rate[:10]
            ],
        },
        {
            "caption": "Flatlines — ≥20 active jobs and ZERO posted in last 30 days",
            "headers": ["Company", "Active jobs", "Posted 7d"],
            "rows": [
                [r["company_name"], fmt_int(r["active_jobs"]), fmt_int(r["posted_7d"])]
                for r in flat
            ],
        },
        tables_7d[0],
        tables_7d[1],
        tables_30d[0],
        tables_30d[1],
    ]

    about_block = "\n".join([
        "### About the net change tables",
        "",
        "- **Posted Nd** — count of jobs whose `first_published_at` falls inside the window. ATS-reported, not pipeline-reported.",
        "- **Closed Nd** — count of `job_history` events with `change_type = 'closed'` inside the window. This is the per-event closure count, so a job that was closed → reactivated → closed again inside the window contributes 2.",
        "- **Net Nd** = Posted Nd − Closed Nd. Should approximately equal Δ active jobs over the same window.",
        f"- **Δ active jobs (Nd)** = `active_jobs` now − `active_jobs` from the closest `company_stats` snapshot within ±{SNAPSHOT_TOLERANCE_DAYS} days of (today − N days). Blank if no snapshot exists in that window.",
        "- Eligibility for the gainers/shedders tables: ≥5 active jobs today, to avoid surfacing single-job swings.",
        "",
        f"#### Window 7d",
        "",
        banner_7d,
        "",
        f"#### Window 30d",
        "",
        banner_30d,
    ])
    extras: list[str] = [about_block]

    if sanity_7d or sanity_30d:
        sanity_lines: list[str] = [
            "### Sanity check — net change vs Δ active jobs",
            "",
            (
                f"_Per-company gap between (posted − closed) and Δ active jobs. "
                f"Surfaced when the gap is both >{SANITY_ABS_GAP} jobs AND "
                f">{int(SANITY_REL_GAP * 100)}% of net. A persistent gap usually "
                f"means closures are being missed (most likely: a job vanished "
                f"between scrapes without firing a 'closed' transition). Treat "
                f"as a diagnostic, not an error._"
            ),
        ]
        for window, mismatches in (("7d", sanity_7d), ("30d", sanity_30d)):
            if not mismatches:
                continue
            sanity_lines.append("")
            sanity_lines.append(f"**{window} window — {len(mismatches)} mismatches**")
            sanity_lines.append("")
            sanity_lines.append(
                "| Company | Posted | Closed | Net | Δ active | Gap (Δactive − net) |"
            )
            sanity_lines.append("| --- | --- | --- | --- | --- | --- |")
            for m in mismatches[:15]:
                sanity_lines.append(
                    f"| {m['company_name']} | {fmt_int(m['posted'])} | "
                    f"{fmt_int(m['closed'])} | {fmt_int(m['net'])} | "
                    f"{fmt_int(m['active_drift'])} | {fmt_int(m['gap'])} |"
                )
        extras.append("\n".join(sanity_lines))

    write_csv(
        QUERY_NAME,
        rows,
        fieldnames=[
            "company_name", "active_jobs",
            "posted_7d", "posted_30d",
            "closed_7d", "closed_30d",
            "net_7d", "net_30d",
            "active_jobs_7d_ago", "active_jobs_30d_ago",
            "active_change_7d", "active_change_30d",
            "posted_30d_pct_of_active",
        ],
    )
    write_md(QUERY_NAME, TITLE, bullets, tables=tables, extra=extras)
    return 0


if __name__ == "__main__":
    sys.exit(main())

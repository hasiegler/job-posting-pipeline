"""
skills_demand_signals.py
------------------------

What we're looking for
~~~~~~~~~~~~~~~~~~~~~~
Three angles on tech-stack demand:

  1. Top skills overall, by mention frequency across currently-active jobs.
  2. Skills that grew or shrunk in the last 30 days, comparing skill share
     in jobs first published in the last 30 days vs jobs first published
     31–120 days ago. Uses `jobs.skills` joined with `jobs.first_published_at`
     (the ATS post date) — this is "what employers are NEWLY asking for".
  3. Skills that are heavily over-represented at one or two specific
     companies (concentration index): potential angle of "Company X is
     basically the only one hiring for Y right now".

Notes
~~~~~
- Active-only for #1 and #3.
- For #2, the comparison is only valid when the pipeline has been
  collecting `job_history` for at least the length of the prior window
  (120 days). Otherwise the prior window suffers severe survivor bias:
  it can ONLY contain jobs that were posted 31–120 days ago AND were
  still open when the pipeline started scraping. Long-lived/evergreen
  skills (Python, R, etc.) become massively over-represented in that
  biased prior window, so they spuriously appear to be "declining" in
  the recent window. We hard-skip the trend section until the gate
  passes and explain why in the markdown.
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

QUERY_NAME = "skills_demand_signals"
TITLE = "Skills demand signals (top, trending, concentrated)"

TREND_PRIOR_WINDOW_DAYS = 120  # must be fully observed to avoid survivor bias

# ---------------------------------------------------------------------------
# Boilerplate-skill suppression
# ---------------------------------------------------------------------------
# Mirror of the rule applied in
# `dags/datawarehouse/data_modification.refresh_company_analytics`:
# a (skill, company) pair is treated as "About Us" boilerplate — and excluded
# from every aggregate below — when the skill is mentioned in MORE than
# BOILERPLATE_SKILL_MIN_MENTIONS jobs at that company AND in at least
# BOILERPLATE_SKILL_MIN_PERCENTAGE percent of that company's active postings.
#
# The numbers MUST stay in sync with the constants in data_modification.py
# so this script reports what the frontend will actually see in
# `company_skills`. If you change one, change the other.
# ---------------------------------------------------------------------------
BOILERPLATE_SKILL_MIN_MENTIONS = 20
BOILERPLATE_SKILL_MIN_PERCENTAGE = 90.0

# Inline CTE block. Embed at the top of any WITH clause that needs to join
# against `boilerplate_pairs (company_id, skill)`. f-string-interpolated at
# import time so the thresholds above are baked into the SQL.
_BOILERPLATE_CTE_SQL = f"""
    _bp_active AS (
        SELECT j.company_id, j.job_id, j.skills
        FROM jobs j
        WHERE j.is_active = TRUE
          AND j.skills IS NOT NULL
    ),
    _bp_company_totals AS (
        SELECT company_id, COUNT(*) AS n FROM _bp_active GROUP BY company_id
    ),
    _bp_skill_company AS (
        SELECT a.company_id, s.skill, COUNT(*) AS c
        FROM _bp_active a, LATERAL unnest(a.skills) AS s(skill)
        GROUP BY a.company_id, s.skill
    ),
    boilerplate_pairs AS (
        SELECT scc.company_id, scc.skill
        FROM _bp_skill_company scc
        JOIN _bp_company_totals ct ON ct.company_id = scc.company_id
        WHERE scc.c > {BOILERPLATE_SKILL_MIN_MENTIONS}
          AND 100.0 * scc.c / NULLIF(ct.n, 0) >= {BOILERPLATE_SKILL_MIN_PERCENTAGE}
    )
"""


def boilerplate_filter_description() -> str:
    """One-sentence description of the active boilerplate rule for callouts."""
    return (
        f"Skill mentions in any (skill, company) pair where the skill appears "
        f"in more than {BOILERPLATE_SKILL_MIN_MENTIONS} of that company's "
        f"active jobs AND in ≥{BOILERPLATE_SKILL_MIN_PERCENTAGE:g}% of them are "
        "treated as 'About Us' boilerplate and excluded from every aggregate "
        "below (matches the production `company_skills` snapshot rule)."
    )


def _top_skills(cur, limit: int = 25) -> list[dict]:
    cur.execute(
        f"""
        WITH {_BOILERPLATE_CTE_SQL},
        active_jobs AS (
            SELECT job_id, company_id, skills
            FROM jobs
            WHERE is_active = TRUE
              AND skills IS NOT NULL
        ),
        total AS (SELECT COUNT(*) AS n FROM active_jobs)
        SELECT
            s.skill                                           AS skill_name,
            COUNT(*)                                          AS mention_count,
            ROUND(100.0 * COUNT(*) / NULLIF((SELECT n FROM total), 0), 1) AS pct_of_active
        FROM active_jobs a, LATERAL unnest(a.skills) AS s(skill)
        WHERE NOT EXISTS (
            SELECT 1 FROM boilerplate_pairs b
            WHERE b.company_id = a.company_id AND b.skill = s.skill
        )
        GROUP BY s.skill
        ORDER BY mention_count DESC
        LIMIT %s
        """,
        (limit,),
    )
    return cur.fetchall()


def _trending(cur) -> list[dict]:
    cur.execute(
        f"""
        WITH {_BOILERPLATE_CTE_SQL},
        recent AS (
            SELECT job_id, company_id, skills
            FROM jobs
            WHERE skills IS NOT NULL
              AND first_published_at >= NOW() - INTERVAL '30 days'
        ),
        prior AS (
            SELECT job_id, company_id, skills
            FROM jobs
            WHERE skills IS NOT NULL
              AND first_published_at >= NOW() - INTERVAL '120 days'
              AND first_published_at <  NOW() - INTERVAL '30 days'
        ),
        recent_count AS (SELECT COUNT(*) AS n FROM recent),
        prior_count  AS (SELECT COUNT(*) AS n FROM prior),
        recent_skills AS (
            SELECT s.skill, COUNT(*) AS c
            FROM recent r, LATERAL unnest(r.skills) AS s(skill)
            WHERE NOT EXISTS (
                SELECT 1 FROM boilerplate_pairs b
                WHERE b.company_id = r.company_id AND b.skill = s.skill
            )
            GROUP BY s.skill
        ),
        prior_skills AS (
            SELECT s.skill, COUNT(*) AS c
            FROM prior p, LATERAL unnest(p.skills) AS s(skill)
            WHERE NOT EXISTS (
                SELECT 1 FROM boilerplate_pairs b
                WHERE b.company_id = p.company_id AND b.skill = s.skill
            )
            GROUP BY s.skill
        )
        SELECT
            COALESCE(r.skill, p.skill)                                AS skill_name,
            COALESCE(r.c, 0)                                          AS recent_mentions,
            COALESCE(p.c, 0)                                          AS prior_mentions,
            ROUND(100.0 * COALESCE(r.c, 0) / NULLIF((SELECT n FROM recent_count), 0), 2) AS recent_share_pct,
            ROUND(100.0 * COALESCE(p.c, 0) / NULLIF((SELECT n FROM prior_count),  0), 2) AS prior_share_pct,
            ROUND(
                100.0 * COALESCE(r.c, 0) / NULLIF((SELECT n FROM recent_count), 0)
              - 100.0 * COALESCE(p.c, 0) / NULLIF((SELECT n FROM prior_count),  0),
                2
            )                                                         AS share_delta_pct_points
        FROM recent_skills r
        FULL OUTER JOIN prior_skills p ON p.skill = r.skill
        WHERE COALESCE(r.c, 0) + COALESCE(p.c, 0) >= 5
        ORDER BY share_delta_pct_points DESC NULLS LAST
        """
    )
    return cur.fetchall()


def _window_counts(cur) -> tuple[int, int]:
    cur.execute(
        """
        SELECT
            COUNT(*) FILTER (WHERE first_published_at >= NOW() - INTERVAL '30 days') AS recent_jobs,
            COUNT(*) FILTER (WHERE first_published_at >= NOW() - INTERVAL '120 days'
                              AND first_published_at <  NOW() - INTERVAL '30  days') AS prior_jobs
        FROM jobs
        WHERE skills IS NOT NULL
        """
    )
    row = cur.fetchone() or {"recent_jobs": 0, "prior_jobs": 0}
    return (row["recent_jobs"] or 0), (row["prior_jobs"] or 0)


def _self_vendor_audit(cur) -> list[dict]:
    """Cross-check whether a skill name doubles as a company name on our
    board (e.g. 'Cloudflare', 'Datadog', 'GitLab', 'Salesforce', 'New Relic',
    'Stripe', 'Snowflake', 'Databricks', 'MongoDB', 'Elastic', etc.). When
    it does, the skill regex will fire on the company's own job descriptions
    ('Join Cloudflare to build...') and inflate that skill's apparent
    concentration at that company.

    For every skill whose name appears as a substring in some company's
    name (case-insensitive, punctuation/space-tolerant), report:
      - mentions of that skill at the matching company,
      - total mentions of that skill across the board,
      - the matching company's share of those mentions.

    Anything ≥ 50% is suspect; ≥ 80% is almost certainly a self-vendor
    extraction artifact and should be excluded from the marketing post.

    NOTE: this query intentionally does NOT apply the boilerplate-pair
    filter used by the other queries in this script.  This is a diagnostic
    that exists to surface extraction bias; if we filtered out the very rows
    we're trying to flag, the audit would always look clean even when it
    isn't.  After the per-company self-vendor exclusion in
    `dags/extraction/skills.py` is deployed and re-extraction has run,
    these rows are expected to drop to ~0% on their own.
    """
    cur.execute(
        """
        WITH active_skills AS (
            SELECT j.company_id, s.skill
            FROM jobs j, LATERAL unnest(j.skills) AS s(skill)
            WHERE j.is_active = TRUE AND j.skills IS NOT NULL
        ),
        per_company AS (
            SELECT skill, company_id, COUNT(*) AS c
            FROM active_skills
            GROUP BY skill, company_id
        ),
        per_skill AS (
            SELECT skill, SUM(c) AS total_mentions
            FROM per_company
            GROUP BY skill
        )
        SELECT
            pc.skill                                                                         AS skill_name,
            c.company_name                                                                   AS matching_company,
            pc.c                                                                             AS mentions_at_match,
            ps.total_mentions,
            ROUND(100.0 * pc.c / NULLIF(ps.total_mentions, 0), 1)                            AS match_share_pct
        FROM per_company pc
        JOIN per_skill ps ON ps.skill = pc.skill
        JOIN companies c ON c.company_id = pc.company_id
        WHERE
            -- skill name appears as a substring of the company name (case-insensitive),
            -- after normalizing both sides by stripping non-alphanumerics.
            regexp_replace(LOWER(c.company_name), '[^a-z0-9]+', '', 'g')
            LIKE '%' || regexp_replace(LOWER(pc.skill), '[^a-z0-9]+', '', 'g') || '%'
            AND length(regexp_replace(LOWER(pc.skill), '[^a-z0-9]+', '', 'g')) >= 4
        ORDER BY match_share_pct DESC, ps.total_mentions DESC
        """
    )
    return cur.fetchall()


def _concentration(cur) -> list[dict]:
    """Skills that look concentrated at 1–2 companies (≥50% of mentions
    come from a single company AND total mentions ≥ 8).

    Boilerplate (skill, company) pairs are excluded from the input — without
    that filter, this leaderboard would be dominated by 100%-concentration
    rows that are really intro-paragraph noise (MLflow @ Databricks, AWS @
    MongoDB, Elasticsearch @ Elastic, etc.).  See `_BOILERPLATE_CTE_SQL`.
    """
    cur.execute(
        f"""
        WITH {_BOILERPLATE_CTE_SQL},
        active_skills AS (
            SELECT j.company_id, s.skill
            FROM jobs j, LATERAL unnest(j.skills) AS s(skill)
            WHERE j.is_active = TRUE AND j.skills IS NOT NULL
              AND NOT EXISTS (
                  SELECT 1 FROM boilerplate_pairs b
                  WHERE b.company_id = j.company_id AND b.skill = s.skill
              )
        ),
        per_company AS (
            SELECT skill, company_id, COUNT(*) AS c
            FROM active_skills
            GROUP BY skill, company_id
        ),
        per_skill AS (
            SELECT skill, SUM(c) AS total_mentions
            FROM per_company
            GROUP BY skill
        ),
        ranked AS (
            SELECT
                pc.skill,
                pc.company_id,
                pc.c,
                ps.total_mentions,
                ROW_NUMBER() OVER (PARTITION BY pc.skill ORDER BY pc.c DESC) AS rk
            FROM per_company pc
            JOIN per_skill ps ON ps.skill = pc.skill
            WHERE ps.total_mentions >= 8
        )
        SELECT
            r.skill                                                       AS skill_name,
            c.company_name                                                AS top_company,
            r.c                                                           AS top_company_mentions,
            r.total_mentions,
            ROUND(100.0 * r.c / NULLIF(r.total_mentions, 0), 1)           AS top_company_share_pct
        FROM ranked r
        JOIN companies c ON c.company_id = r.company_id
        WHERE r.rk = 1
          AND 100.0 * r.c / NULLIF(r.total_mentions, 0) >= 50
        ORDER BY top_company_share_pct DESC, total_mentions DESC
        LIMIT 25
        """
    )
    return cur.fetchall()


def main() -> int:
    bullets: list[str] = [
        f"Boilerplate filter ACTIVE: any (skill, company) pair with "
        f">{BOILERPLATE_SKILL_MIN_MENTIONS} mentions AND "
        f"≥{BOILERPLATE_SKILL_MIN_PERCENTAGE:g}% of that company's active jobs is "
        "treated as 'About Us' boilerplate and excluded from the top, trending, "
        "and concentration tables below. Mirrors the production "
        "`company_skills` snapshot rule. The self-vendor audit is intentionally "
        "left raw so it can still surface extraction bias.",
    ]
    csv_rows: list[dict] = []

    with readonly_cursor() as cur:
        top = _top_skills(cur)
        history_age = pipeline_history_age_days(cur)
        recent_n, prior_n = _window_counts(cur)

        trend_gate_ok = (
            history_age is not None
            and history_age >= TREND_PRIOR_WINDOW_DAYS
            and recent_n > 0
            and prior_n > 0
        )
        trending = _trending(cur) if trend_gate_ok else []
        concentrated = _concentration(cur)
        self_vendor = _self_vendor_audit(cur)

        # 1. Top skills
        print("== Top skills across active jobs ==")
        if top:
            print_table(top, ["skill_name", "mention_count", "pct_of_active"], limit=15)
            bullets.append(
                f"Most-asked skills across active postings: "
                + ", ".join(f"{r['skill_name']} ({fmt_pct(r['pct_of_active'])})" for r in top[:5])
                + "."
            )
        else:
            print("  (no extracted skills yet)")
            bullets.append("No skill mentions extracted yet — skip this angle.")
        print()

        # 2. Trending
        print("== Trending skills (last 30d vs prior 31–120d, by share-point delta) ==")
        if not trend_gate_ok:
            if history_age is None:
                reason = (
                    "job_history is empty, so the prior 31–120d window cannot be "
                    "validated as observed."
                )
            elif history_age < TREND_PRIOR_WINDOW_DAYS:
                reason = (
                    f"pipeline has only {history_age} days of job_history, but the "
                    f"prior comparison window needs {TREND_PRIOR_WINDOW_DAYS} days of "
                    "real-time observation. Until then, the prior window suffers "
                    "survivor bias (only jobs that were still open when scraping "
                    "started are present), so evergreen skills like Python look "
                    "spuriously 'declining'."
                )
            else:
                reason = (
                    f"recent jobs={fmt_int(recent_n)}, prior jobs={fmt_int(prior_n)} "
                    "— need both windows populated."
                )
            print(f"  SKIPPED — {reason}")
            bullets.append(f"30-day skill trend: SKIPPED — {reason}")
        else:
            risers = [r for r in trending if (r["share_delta_pct_points"] or 0) > 0][:10]
            fallers = sorted(trending, key=lambda r: (r["share_delta_pct_points"] or 0))[:10]
            print("  Top risers:")
            print_table(
                risers,
                ["skill_name", "recent_share_pct", "prior_share_pct", "share_delta_pct_points", "recent_mentions"],
            )
            print("  Top fallers:")
            print_table(
                fallers,
                ["skill_name", "recent_share_pct", "prior_share_pct", "share_delta_pct_points", "recent_mentions"],
            )
            if risers:
                bullets.append(
                    "Skills gaining share in last-30-day postings: "
                    + ", ".join(
                        f"{r['skill_name']} (+{r['share_delta_pct_points']}pp)"
                        for r in risers[:5]
                    )
                    + "."
                )
            if fallers:
                bullets.append(
                    "Skills losing share: "
                    + ", ".join(
                        f"{r['skill_name']} ({r['share_delta_pct_points']}pp)"
                        for r in fallers[:5]
                    )
                    + "."
                )
        print()

        # 3. Concentration
        print("== Skills concentrated at one company (≥50% of mentions from top employer) ==")
        if not concentrated:
            print("  (none above threshold)")
        else:
            print_table(
                concentrated,
                ["skill_name", "top_company", "top_company_mentions", "total_mentions", "top_company_share_pct"],
                limit=15,
            )
            example = concentrated[0]
            bullets.append(
                f"Concentration angle: {example['skill_name']} appears in "
                f"{fmt_int(example['top_company_mentions'])} jobs at "
                f"{example['top_company']}, which is "
                f"{fmt_pct(example['top_company_share_pct'])} of the "
                f"{fmt_int(example['total_mentions'])} jobs across all tracked "
                f"companies that mention it. "
                f"{fmt_int(len(concentrated))} skills meet this concentration threshold."
            )
        print()

        # 4. Self-vendor extraction audit
        print("== Self-vendor skill audit (skill name == matching company name) ==")
        if not self_vendor:
            print("  (no skill names overlap with company names — clean)")
        else:
            print_table(
                self_vendor,
                ["skill_name", "matching_company", "mentions_at_match", "total_mentions", "match_share_pct"],
                limit=20,
            )
            suspect = [r for r in self_vendor if (r["match_share_pct"] or 0) >= 50]
            severe = [r for r in self_vendor if (r["match_share_pct"] or 0) >= 80]
            if suspect:
                names = ", ".join(
                    f"{r['skill_name']}@{r['matching_company']} ({fmt_pct(r['match_share_pct'])})"
                    for r in suspect[:5]
                )
                bullets.append(
                    f"Self-vendor skill bias detected — these skill names also match a "
                    f"tracked company, so the regex likely fires on the company's own "
                    f"job descriptions: {names}. {fmt_int(len(suspect))} pairs above 50%, "
                    f"{fmt_int(len(severe))} above 80% (treat 80%+ as extraction artifacts, "
                    "exclude from any post)."
                )

        # CSV combines all three sections via a "section" column.
        for r in top:
            csv_rows.append({"section": "top", **r})
        for r in trending:
            csv_rows.append({"section": "trending", **r})
        for r in concentrated:
            csv_rows.append({"section": "concentration", **r})

    for r in self_vendor:
        csv_rows.append({"section": "self_vendor", **r})

    fieldnames = [
        "section", "skill_name",
        "mention_count", "pct_of_active",
        "recent_mentions", "prior_mentions",
        "recent_share_pct", "prior_share_pct", "share_delta_pct_points",
        "top_company", "top_company_mentions", "total_mentions", "top_company_share_pct",
        "matching_company", "mentions_at_match", "match_share_pct",
    ]

    tables = [
        {
            "caption": "Top 15 skills across active jobs",
            "headers": [
                "Skill",
                "Active jobs mentioning skill",
                "% of all active jobs",
            ],
            "rows": [
                [r["skill_name"], fmt_int(r["mention_count"]), fmt_pct(r["pct_of_active"])]
                for r in top[:15]
            ],
        },
        {
            "caption": "Top 15 single-company-concentrated skills (≥50% from one employer)",
            "headers": [
                "Skill",
                "Top employer",
                "Jobs mentioning skill at top employer",
                "Jobs mentioning skill across ALL tracked companies",
                "% of mentions from top employer",
            ],
            "rows": [
                [
                    r["skill_name"],
                    r["top_company"],
                    fmt_int(r["top_company_mentions"]),
                    fmt_int(r["total_mentions"]),
                    fmt_pct(r["top_company_share_pct"]),
                ]
                for r in concentrated[:15]
            ],
        },
        {
            "caption": "Self-vendor audit — skill names that overlap a tracked company name",
            "headers": [
                "Skill",
                "Matching company",
                "Jobs mentioning skill at that company",
                "Jobs mentioning skill across ALL tracked companies",
                "% of mentions from matching company",
            ],
            "rows": [
                [
                    r["skill_name"],
                    r["matching_company"],
                    fmt_int(r["mentions_at_match"]),
                    fmt_int(r["total_mentions"]),
                    fmt_pct(r["match_share_pct"]),
                ]
                for r in self_vendor[:20]
            ],
        },
    ]

    extra = [
        f"> **Boilerplate filter applied to the top, trending, and concentration "
        f"tables.** "
        f"{boilerplate_filter_description()} The self-vendor audit table below "
        "is intentionally left raw (filtering would hide the bias the audit "
        "exists to flag).",
        "> **How to read the concentration table:** the row "
        "`MLflow | databricks_greenhouse | 851 | 889 | 95.7%` would mean: across "
        "ALL 69 tracked companies, 889 currently-active jobs mention MLflow; "
        "851 of those 889 jobs (95.7%) are at Databricks. It does NOT mean "
        "851 of 889 jobs at Databricks. The denominator is 'jobs anywhere in "
        "the dataset that mention this skill', not 'jobs at the top employer'. "
        "(Note: with the boilerplate filter active, MLflow @ Databricks itself "
        "no longer appears here — those mentions are now classified as 'About "
        "Us' boilerplate and excluded.)",
        "> **How to read the self-vendor table:** if a skill is ≥80% concentrated at "
        "the company whose name it shares, the extractor is almost certainly matching "
        "the company's own description text (e.g. 'Join Cloudflare to build…'). "
        "Skills in that bucket should be excluded from any marketing claim about "
        "demand. 50–80% pairs need a manual sanity check before quoting.",
    ]

    write_csv(QUERY_NAME, csv_rows, fieldnames=fieldnames)
    write_md(QUERY_NAME, TITLE, bullets, tables=tables, extra=extra)
    return 0


if __name__ == "__main__":
    sys.exit(main())

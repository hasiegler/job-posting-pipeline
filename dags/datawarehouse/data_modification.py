"""
Helper functions for inserting/modifying data in Supabase tables.
"""

import json
import logging
from datetime import datetime

from psycopg2.extras import execute_values, Json

from extraction import EXTRACTION_VERSION
from extraction.salary import extract_salary
from extraction.remote_policy import extract_remote_policy
from extraction.skills import (
    build_company_skill_exclusions,
    build_skill_matchers,
    extract_skills,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Boilerplate skill suppression for the company_skills snapshot.
#
# A skill mentioned in (almost) every active posting at a single company is
# almost certainly sitting in the "About <Company>" boilerplate that gets
# pasted into every JD, not a real per-role requirement.  Examples we observed
# at the 2026-04-18 snapshot:
#   - MLflow / Apache / Spark @ Databricks (851/851 = 100%)
#   - AWS / Azure @ MongoDB (413/418 = 98.80%)
#   - Elasticsearch @ Elastic (204/204 = 100%)
#
# These rows are dropped from `company_skills` only — `jobs.skills` is kept as
# the raw source of truth in case we want to revisit the rule.  Adjust the two
# constants below to widen/tighten the cut.
# ---------------------------------------------------------------------------
BOILERPLATE_SKILL_MIN_MENTIONS = 20        # ignore tiny-N companies
BOILERPLATE_SKILL_MIN_PERCENTAGE = 90.0    # mentioned in ≥X% of active jobs


def _empty_company_metrics() -> dict:
    return {
        "inserted": 0,
        "updated": 0,
        "unchanged": 0,
        "closed": 0,
        "extraction_attempted": 0,
        "salary_found": 0,
        "remote_policy_found": 0,
        "skills_found": 0,
    }


def ensure_monitoring_tables(conn, cur) -> None:
    """Create telemetry tables used for run monitoring if missing."""
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS pipeline_runs (
            dag_id               TEXT NOT NULL,
            run_id               TEXT NOT NULL,
            run_started_at       TIMESTAMPTZ NOT NULL,
            run_finished_at      TIMESTAMPTZ NOT NULL,
            status               TEXT NOT NULL,
            total_companies      INTEGER NOT NULL DEFAULT 0,
            total_scraped        INTEGER NOT NULL DEFAULT 0,
            total_staged         INTEGER NOT NULL DEFAULT 0,
            total_new            INTEGER NOT NULL DEFAULT 0,
            total_updated        INTEGER NOT NULL DEFAULT 0,
            total_unchanged      INTEGER NOT NULL DEFAULT 0,
            total_closed         INTEGER NOT NULL DEFAULT 0,
            total_extracted      INTEGER NOT NULL DEFAULT 0,
            salary_found         INTEGER NOT NULL DEFAULT 0,
            remote_policy_found  INTEGER NOT NULL DEFAULT 0,
            skills_found         INTEGER NOT NULL DEFAULT 0,
            created_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (dag_id, run_id)
        );
        """
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_pipeline_runs_started
        ON pipeline_runs (run_started_at DESC);
        """
    )
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS company_run_metrics (
            dag_id               TEXT NOT NULL,
            run_id               TEXT NOT NULL,
            run_started_at       TIMESTAMPTZ NOT NULL,
            company_id           INTEGER NOT NULL REFERENCES companies(company_id),
            company_name         TEXT NOT NULL,
            scraped_jobs         INTEGER NOT NULL DEFAULT 0,
            staged_jobs          INTEGER NOT NULL DEFAULT 0,
            new_jobs             INTEGER NOT NULL DEFAULT 0,
            updated_jobs         INTEGER NOT NULL DEFAULT 0,
            unchanged_jobs       INTEGER NOT NULL DEFAULT 0,
            closed_jobs          INTEGER NOT NULL DEFAULT 0,
            extraction_attempted INTEGER NOT NULL DEFAULT 0,
            salary_found         INTEGER NOT NULL DEFAULT 0,
            remote_policy_found  INTEGER NOT NULL DEFAULT 0,
            skills_found         INTEGER NOT NULL DEFAULT 0,
            created_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (dag_id, run_id, company_id)
        );
        """
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_company_run_metrics_company_started
        ON company_run_metrics (company_id, run_started_at DESC);
        """
    )
    conn.commit()


def ensure_job_history_table(conn, cur) -> None:
    """Create job_history table and indexes if missing."""
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS job_history (
            history_id          BIGSERIAL PRIMARY KEY,
            job_id              INTEGER NOT NULL REFERENCES jobs(job_id),
            company_id          INTEGER NOT NULL REFERENCES companies(company_id),
            source_job_id       TEXT NOT NULL,
            change_type         TEXT NOT NULL,
            changed_fields      TEXT[] NOT NULL DEFAULT '{}'::TEXT[],
            title               TEXT,
            source_url          TEXT,
            location            TEXT,
            departments         TEXT[],
            offices             TEXT[],
            language            TEXT,
            description_text    TEXT,
            description_html    TEXT,
            skills              TEXT[],
            salary_min          NUMERIC,
            salary_max          NUMERIC,
            salary_currency     TEXT,
            salary_period       TEXT,
            remote_policy       TEXT,
            experience_level    TEXT,
            education_required  TEXT,
            benefits            TEXT[],
            first_published_at  TIMESTAMPTZ,
            is_active           BOOLEAN NOT NULL,
            recorded_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_job_history_job_id
        ON job_history (job_id, recorded_at DESC);
        """
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_job_history_company
        ON job_history (company_id, recorded_at DESC);
        """
    )
    cur.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_job_history_change_type
        ON job_history (change_type, recorded_at DESC);
        """
    )
    conn.commit()


def snapshot_changed_jobs(conn, cur, changed_jobs: list[dict]) -> dict:
    """Write a history row for each changed job, capturing its current state."""
    ensure_job_history_table(conn, cur)

    if not changed_jobs:
        return {"snapshots_written": 0}

    job_ids = [row["job_id"] for row in changed_jobs]
    change_map = {
        row["job_id"]: {
            "change_type": row["change_type"],
            "changed_fields": row.get("changed_fields", []),
        }
        for row in changed_jobs
    }

    cur.execute(
        """
        SELECT job_id, company_id, source_job_id, title, source_url, location,
               departments, offices, language, description_text, description_html,
               skills, salary_min, salary_max, salary_currency, salary_period,
               remote_policy, experience_level, education_required, benefits,
               first_published_at, is_active
        FROM jobs
        WHERE job_id = ANY(%s)
        """,
        (job_ids,),
    )
    rows = cur.fetchall()

    execute_values(cur, """
        INSERT INTO job_history (
            job_id, company_id, source_job_id, change_type, changed_fields,
            title, source_url, location, departments, offices, language,
            description_text, description_html, skills,
            salary_min, salary_max, salary_currency, salary_period,
            remote_policy, experience_level, education_required, benefits,
            first_published_at, is_active
        ) VALUES %s
    """, [
        (
            row["job_id"], row["company_id"], row["source_job_id"],
            change_map[row["job_id"]]["change_type"],
            change_map[row["job_id"]]["changed_fields"],
            row["title"], row["source_url"], row["location"],
            row["departments"], row["offices"], row["language"],
            row["description_text"], row["description_html"],
            row["skills"], row["salary_min"], row["salary_max"],
            row["salary_currency"], row["salary_period"],
            row["remote_policy"], row["experience_level"],
            row["education_required"], row["benefits"],
            row["first_published_at"], row["is_active"],
        )
        for row in rows
    ])

    conn.commit()
    return {"snapshots_written": len(rows)}


def upsert_run_monitoring(conn, cur, run_metrics: dict, company_metrics: list[dict]) -> None:
    """Upsert one run summary row and all company rows for that run."""
    ensure_monitoring_tables(conn, cur)

    cur.execute(
        """
        INSERT INTO pipeline_runs (
            dag_id, run_id, run_started_at, run_finished_at, status,
            total_companies, total_scraped, total_staged, total_new, total_updated,
            total_unchanged, total_closed, total_extracted, salary_found,
            remote_policy_found, skills_found, updated_at
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW()
        )
        ON CONFLICT (dag_id, run_id) DO UPDATE SET
            run_started_at = EXCLUDED.run_started_at,
            run_finished_at = EXCLUDED.run_finished_at,
            status = EXCLUDED.status,
            total_companies = EXCLUDED.total_companies,
            total_scraped = EXCLUDED.total_scraped,
            total_staged = EXCLUDED.total_staged,
            total_new = EXCLUDED.total_new,
            total_updated = EXCLUDED.total_updated,
            total_unchanged = EXCLUDED.total_unchanged,
            total_closed = EXCLUDED.total_closed,
            total_extracted = EXCLUDED.total_extracted,
            salary_found = EXCLUDED.salary_found,
            remote_policy_found = EXCLUDED.remote_policy_found,
            skills_found = EXCLUDED.skills_found,
            updated_at = NOW();
        """,
        (
            run_metrics["dag_id"],
            run_metrics["run_id"],
            run_metrics["run_started_at"],
            run_metrics["run_finished_at"],
            run_metrics["status"],
            run_metrics["total_companies"],
            run_metrics["total_scraped"],
            run_metrics["total_staged"],
            run_metrics["total_new"],
            run_metrics["total_updated"],
            run_metrics["total_unchanged"],
            run_metrics["total_closed"],
            run_metrics["total_extracted"],
            run_metrics["salary_found"],
            run_metrics["remote_policy_found"],
            run_metrics["skills_found"],
        ),
    )

    if company_metrics:
        execute_values(
            cur,
            """
            INSERT INTO company_run_metrics (
                dag_id, run_id, run_started_at, company_id, company_name,
                scraped_jobs, staged_jobs, new_jobs, updated_jobs, unchanged_jobs,
                closed_jobs, extraction_attempted, salary_found, remote_policy_found,
                skills_found, updated_at
            ) VALUES %s
            ON CONFLICT (dag_id, run_id, company_id) DO UPDATE SET
                run_started_at = EXCLUDED.run_started_at,
                company_name = EXCLUDED.company_name,
                scraped_jobs = EXCLUDED.scraped_jobs,
                staged_jobs = EXCLUDED.staged_jobs,
                new_jobs = EXCLUDED.new_jobs,
                updated_jobs = EXCLUDED.updated_jobs,
                unchanged_jobs = EXCLUDED.unchanged_jobs,
                closed_jobs = EXCLUDED.closed_jobs,
                extraction_attempted = EXCLUDED.extraction_attempted,
                salary_found = EXCLUDED.salary_found,
                remote_policy_found = EXCLUDED.remote_policy_found,
                skills_found = EXCLUDED.skills_found,
                updated_at = NOW()
            """,
            [
                (
                    row["dag_id"], row["run_id"], row["run_started_at"],
                    row["company_id"], row["company_name"],
                    row["scraped_jobs"], row["staged_jobs"],
                    row["new_jobs"], row["updated_jobs"], row["unchanged_jobs"],
                    row["closed_jobs"], row["extraction_attempted"],
                    row["salary_found"], row["remote_policy_found"],
                    row["skills_found"],
                )
                for row in company_metrics
            ],
            template="(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())",
        )

    conn.commit()


def refresh_company_analytics(conn, cur, statement_timeout_s: int = 300) -> dict:
    """Snapshot company-level analytics from the current state of the jobs table.

    closed_* and net_change_* are only populated when the pipeline has at least
    one recorded run that predates the lookback window (7 or 30 days).  Before
    that history exists, those counts would be artificially low (we wouldn't
    know what closed because we had no prior baseline), so they are left NULL.
    """
    cur.execute("SHOW statement_timeout")
    original_timeout = cur.fetchone()["statement_timeout"]
    cur.execute("SET statement_timeout = %s", (f"{statement_timeout_s}s",))

    today = datetime.now().date()

    try:
        cur.execute(
            """
            WITH first_run AS (
                SELECT company_id, MIN(run_started_at)::DATE AS first_run_date
                FROM company_run_metrics
                GROUP BY company_id
            )
            INSERT INTO company_stats (
                company_id, snapshot_date, active_jobs,
                posted_7d, posted_30d, closed_7d, closed_30d,
                net_change_7d, net_change_30d,
                remote_count, hybrid_count, onsite_count,
                avg_salary_min, avg_salary_max,
                median_salary_min, median_salary_max
            )
            SELECT
                j.company_id,
                %s,
                COUNT(*) FILTER (WHERE j.is_active),
                COUNT(*) FILTER (WHERE j.first_published_at >= %s - INTERVAL '7 days'),
                COUNT(*) FILTER (WHERE j.first_published_at >= %s - INTERVAL '30 days'),
                CASE WHEN fr.first_run_date <= %s - INTERVAL '7 days'
                     THEN COUNT(*) FILTER (WHERE j.date_closed >= %s - INTERVAL '7 days')
                END,
                CASE WHEN fr.first_run_date <= %s - INTERVAL '30 days'
                     THEN COUNT(*) FILTER (WHERE j.date_closed >= %s - INTERVAL '30 days')
                END,
                CASE WHEN fr.first_run_date <= %s - INTERVAL '7 days'
                     THEN COUNT(*) FILTER (WHERE j.first_published_at >= %s - INTERVAL '7 days')
                        - COUNT(*) FILTER (WHERE j.date_closed >= %s - INTERVAL '7 days')
                END,
                CASE WHEN fr.first_run_date <= %s - INTERVAL '30 days'
                     THEN COUNT(*) FILTER (WHERE j.first_published_at >= %s - INTERVAL '30 days')
                        - COUNT(*) FILTER (WHERE j.date_closed >= %s - INTERVAL '30 days')
                END,
                COUNT(*) FILTER (WHERE j.is_active AND j.remote_policy = 'Remote'),
                COUNT(*) FILTER (WHERE j.is_active AND j.remote_policy = 'Hybrid'),
                COUNT(*) FILTER (WHERE j.is_active AND j.remote_policy = 'On-Site'),
                AVG(j.salary_min)  FILTER (WHERE j.is_active AND j.salary_min IS NOT NULL AND j.salary_currency = 'USD' AND j.salary_period = 'yearly'),
                AVG(j.salary_max)  FILTER (WHERE j.is_active AND j.salary_max IS NOT NULL AND j.salary_currency = 'USD' AND j.salary_period = 'yearly'),
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY j.salary_min)
                    FILTER (WHERE j.is_active AND j.salary_min IS NOT NULL AND j.salary_currency = 'USD' AND j.salary_period = 'yearly'),
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY j.salary_max)
                    FILTER (WHERE j.is_active AND j.salary_max IS NOT NULL AND j.salary_currency = 'USD' AND j.salary_period = 'yearly')
            FROM jobs j
            LEFT JOIN first_run fr ON fr.company_id = j.company_id
            GROUP BY j.company_id, fr.first_run_date
            ON CONFLICT (company_id, snapshot_date) DO UPDATE SET
                active_jobs       = EXCLUDED.active_jobs,
                posted_7d         = EXCLUDED.posted_7d,
                posted_30d        = EXCLUDED.posted_30d,
                closed_7d         = EXCLUDED.closed_7d,
                closed_30d        = EXCLUDED.closed_30d,
                net_change_7d     = EXCLUDED.net_change_7d,
                net_change_30d    = EXCLUDED.net_change_30d,
                remote_count      = EXCLUDED.remote_count,
                hybrid_count      = EXCLUDED.hybrid_count,
                onsite_count      = EXCLUDED.onsite_count,
                avg_salary_min    = EXCLUDED.avg_salary_min,
                avg_salary_max    = EXCLUDED.avg_salary_max,
                median_salary_min = EXCLUDED.median_salary_min,
                median_salary_max = EXCLUDED.median_salary_max;
            """,
            (today,) + (today,) * 12,
        )
        stats_rows = cur.rowcount

        cur.execute(
            """
            DELETE FROM company_skills
            WHERE snapshot_date = %s;
            """,
            (today,),
        )
        cur.execute(
            """
            INSERT INTO company_skills (
                company_id, snapshot_date, skill_name, mention_count, percentage,
                avg_salary_min, avg_salary_max,
                median_salary_min, median_salary_max
            )
            SELECT
                j.company_id,
                %s,
                s.skill,
                COUNT(*),
                ROUND(100.0 * COUNT(*) / ac.total, 2),
                AVG(j.salary_min)  FILTER (WHERE j.salary_min IS NOT NULL AND j.salary_currency = 'USD' AND j.salary_period = 'yearly'),
                AVG(j.salary_max)  FILTER (WHERE j.salary_max IS NOT NULL AND j.salary_currency = 'USD' AND j.salary_period = 'yearly'),
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY j.salary_min)
                    FILTER (WHERE j.salary_min IS NOT NULL AND j.salary_currency = 'USD' AND j.salary_period = 'yearly'),
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY j.salary_max)
                    FILTER (WHERE j.salary_max IS NOT NULL AND j.salary_currency = 'USD' AND j.salary_period = 'yearly')
            FROM jobs j,
                 LATERAL (SELECT DISTINCT unnest(j.skills)) AS s(skill),
                 (SELECT company_id, COUNT(*) AS total
                  FROM jobs WHERE is_active GROUP BY company_id) ac
            WHERE j.is_active
              AND j.company_id = ac.company_id
            GROUP BY j.company_id, s.skill, ac.total
            -- Drop boilerplate-shaped rows: a skill that fires on (almost)
            -- every active posting at a company is almost always sitting in
            -- the "About <Company>" intro, not a real per-role requirement.
            -- Raw `jobs.skills` is left untouched.
            HAVING NOT (
                COUNT(*) > %s
                AND 100.0 * COUNT(*) / ac.total >= %s
            )
            ON CONFLICT (company_id, snapshot_date, skill_name) DO UPDATE SET
                mention_count     = EXCLUDED.mention_count,
                percentage        = EXCLUDED.percentage,
                avg_salary_min    = EXCLUDED.avg_salary_min,
                avg_salary_max    = EXCLUDED.avg_salary_max,
                median_salary_min = EXCLUDED.median_salary_min,
                median_salary_max = EXCLUDED.median_salary_max;
            """,
            (today, BOILERPLATE_SKILL_MIN_MENTIONS, BOILERPLATE_SKILL_MIN_PERCENTAGE),
        )
        skills_rows = cur.rowcount

        cur.execute(
            """
            DELETE FROM company_departments
            WHERE snapshot_date = %s;
            """,
            (today,),
        )
        cur.execute(
            """
            INSERT INTO company_departments (
                company_id, snapshot_date, department_name,
                active_job_count, percentage,
                avg_salary_min, avg_salary_max,
                median_salary_min, median_salary_max
            )
            SELECT
                j.company_id,
                %s,
                d.dept,
                COUNT(*),
                ROUND(100.0 * COUNT(*) / ac.total, 2),
                AVG(j.salary_min)  FILTER (WHERE j.salary_min IS NOT NULL AND j.salary_currency = 'USD' AND j.salary_period = 'yearly'),
                AVG(j.salary_max)  FILTER (WHERE j.salary_max IS NOT NULL AND j.salary_currency = 'USD' AND j.salary_period = 'yearly'),
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY j.salary_min)
                    FILTER (WHERE j.salary_min IS NOT NULL AND j.salary_currency = 'USD' AND j.salary_period = 'yearly'),
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY j.salary_max)
                    FILTER (WHERE j.salary_max IS NOT NULL AND j.salary_currency = 'USD' AND j.salary_period = 'yearly')
            FROM jobs j,
                 LATERAL unnest(j.departments) AS d(dept),
                 (SELECT company_id, COUNT(*) AS total
                  FROM jobs WHERE is_active GROUP BY company_id) ac
            WHERE j.is_active
              AND j.company_id = ac.company_id
            GROUP BY j.company_id, d.dept, ac.total
            ON CONFLICT (company_id, snapshot_date, department_name) DO UPDATE SET
                active_job_count  = EXCLUDED.active_job_count,
                percentage        = EXCLUDED.percentage,
                avg_salary_min    = EXCLUDED.avg_salary_min,
                avg_salary_max    = EXCLUDED.avg_salary_max,
                median_salary_min = EXCLUDED.median_salary_min,
                median_salary_max = EXCLUDED.median_salary_max;
            """,
            (today,),
        )
        departments_rows = cur.rowcount

        conn.commit()
        summary = {
            "snapshot_date": str(today),
            "stats_rows": stats_rows,
            "skills_rows": skills_rows,
            "departments_rows": departments_rows,
        }
        logger.info(f"Company analytics refreshed: {summary}")
        return summary
    finally:
        cur.execute("SET statement_timeout = %s", (original_timeout,))


def _parse_timestamp(value) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return value
    return datetime.fromisoformat(value)


def normalize_greenhouse(raw_data: dict) -> dict:
    """Normalize a Greenhouse raw_data JSONB object to the jobs table schema."""
    return {
        "source_job_id": str(raw_data.get("id", "")),
        "source_url": raw_data.get("url"),
        "title": raw_data.get("title"),
        "location": raw_data.get("location"),
        "departments": raw_data.get("departments", []),
        "offices": raw_data.get("offices", []),
        "language": raw_data.get("language"),
        "description_text": raw_data.get("content_text"),
        "description_html": raw_data.get("content_html"),
        "first_published_at": _parse_timestamp(raw_data.get("first_published")),
        "skills": None,
        "salary_min": None,
        "salary_max": None,
        "salary_currency": None,
        "salary_period": None,
        "remote_policy": None,
        "experience_level": None,
        "education_required": None,
        "benefits": None,
        "extracted_at": None,
        "extraction_version": None,
    }


NORMALIZERS = {
    "greenhouse": normalize_greenhouse,
}


def process_staging_to_jobs(conn, cur) -> dict:
    """Process all unprocessed staging_jobs rows into the jobs table.

    Uses bulk SQL operations instead of row-by-row queries:
      1. Normalize every staging row in Python.
      2. Load normalized data into a temp table.
      3. Single LEFT JOIN to classify rows as new / changed / unchanged.
      4. Bulk INSERT, UPDATE, and closed-detection via set-based SQL.
      5. One UPDATE to mark all staging rows processed.
    """
    cur.execute("""
        SELECT job_id, company_id, scraper_type, source_job_id, raw_data, scraped_at
        FROM staging_jobs
        WHERE processed = FALSE
        ORDER BY company_id, job_id
    """)
    staging_rows = cur.fetchall()

    if not staging_rows:
        logger.info("No unprocessed staging rows found.")
        return {
            "inserted": 0,
            "updated": 0,
            "unchanged": 0,
            "closed": 0,
            "company_metrics": [],
            "changed_jobs": [],
        }

    # -- Phase 1: normalise in Python, track companies --------------------
    all_staging_ids = [r["job_id"] for r in staging_rows]
    normalized = []
    companies_seen = set()
    company_metrics: dict[int, dict] = {}

    for row in staging_rows:
        company_id = row["company_id"]
        scraper_type = row["scraper_type"]
        raw_data = row["raw_data"]
        if isinstance(raw_data, str):
            raw_data = json.loads(raw_data)

        companies_seen.add((company_id, scraper_type))
        company_metrics.setdefault(company_id, _empty_company_metrics())

        normalizer = NORMALIZERS.get(scraper_type)
        if not normalizer:
            logger.warning(
                f"No normalizer for scraper_type '{scraper_type}', "
                f"skipping staging_id={row['job_id']}"
            )
            continue

        norm = normalizer(raw_data)
        normalized.append({
            "staging_id": row["job_id"],
            "company_id": company_id,
            "scraper_type": scraper_type,
            **norm,
        })

    if not normalized:
        cur.execute(
            "UPDATE staging_jobs SET processed = TRUE, processed_at = NOW() "
            "WHERE job_id = ANY(%s)",
            (all_staging_ids,),
        )
        conn.commit()
        return {
            "inserted": 0, "updated": 0, "unchanged": 0, "closed": 0,
            "company_metrics": [], "changed_jobs": [],
        }

    # Deduplicate: keep latest staging row per (company_id, source_job_id)
    seen_keys: dict[tuple, dict] = {}
    for norm in normalized:
        seen_keys[(norm["company_id"], norm["source_job_id"])] = norm
    deduped = list(seen_keys.values())

    # -- Phase 2: load into temp table ------------------------------------
    cur.execute("""
        CREATE TEMP TABLE _norm (
            staging_id         INTEGER,
            company_id         INTEGER,
            scraper_type       TEXT,
            source_job_id      TEXT,
            source_url         TEXT,
            title              TEXT,
            location           TEXT,
            departments        TEXT[],
            offices            TEXT[],
            language           TEXT,
            description_text   TEXT,
            description_html   TEXT,
            first_published_at TIMESTAMPTZ
        ) ON COMMIT DROP
    """)

    execute_values(cur, """
        INSERT INTO _norm (
            staging_id, company_id, scraper_type, source_job_id, source_url,
            title, location, departments, offices, language,
            description_text, description_html, first_published_at
        ) VALUES %s
    """, [
        (
            r["staging_id"], r["company_id"], r["scraper_type"],
            r["source_job_id"], r["source_url"], r["title"], r["location"],
            r["departments"], r["offices"], r["language"],
            r["description_text"], r["description_html"],
            r["first_published_at"],
        )
        for r in deduped
    ])

    # -- Phase 3: classify with a single JOIN -----------------------------
    cur.execute("""
        SELECT
            n.staging_id, n.company_id, n.scraper_type, n.source_job_id,
            n.source_url,        n.title,        n.location,
            n.departments,       n.offices,      n.language,
            n.description_text,  n.description_html, n.first_published_at,
            j.job_id           AS existing_job_id,
            j.source_url       AS old_source_url,
            j.title            AS old_title,
            j.location         AS old_location,
            j.departments      AS old_departments,
            j.offices          AS old_offices,
            j.language         AS old_language,
            j.description_text AS old_description_text,
            j.description_html AS old_description_html,
            j.first_published_at AS old_first_published_at,
            j.is_active        AS old_is_active
        FROM _norm n
        LEFT JOIN jobs j
            ON  j.company_id    = n.company_id
            AND j.source_job_id = n.source_job_id
    """)
    classified = cur.fetchall()

    compare_fields = [
        "source_url", "title", "location", "departments", "offices",
        "language", "description_text", "description_html",
        "first_published_at",
    ]

    new_staging_ids: list[int] = []
    changed_job_ids: list[int] = []
    unchanged_job_ids: list[int] = []
    changed_jobs: list[dict] = []

    for row in classified:
        cid = row["company_id"]
        if row["existing_job_id"] is None:
            new_staging_ids.append(row["staging_id"])
        else:
            changed_fields = [
                f for f in compare_fields
                if row[f] != row[f"old_{f}"]
            ]
            reactivated = not row["old_is_active"]

            if changed_fields or reactivated:
                changed_job_ids.append(row["existing_job_id"])
                company_metrics[cid]["updated"] += 1
                change_type = "reactivated" if reactivated else "updated"
                changed_jobs.append({
                    "job_id": row["existing_job_id"],
                    "change_type": change_type,
                    "changed_fields": changed_fields,
                })
                if reactivated:
                    logger.info(f"Reactivated job {row['source_job_id']}")
                else:
                    logger.info(
                        f"Updated job {row['source_job_id']}: {changed_fields}"
                    )
            else:
                unchanged_job_ids.append(row["existing_job_id"])
                company_metrics[cid]["unchanged"] += 1

    # -- Phase 4: bulk INSERT new jobs ------------------------------------
    inserted = 0
    if new_staging_ids:
        cur.execute("""
            INSERT INTO jobs (
                company_id, scraper_type, source_job_id, source_url,
                title, location, departments, offices, language,
                description_text, description_html, first_published_at,
                first_seen, last_seen, is_active
            )
            SELECT
                n.company_id, n.scraper_type, n.source_job_id, n.source_url,
                n.title, n.location, n.departments, n.offices, n.language,
                n.description_text, n.description_html, n.first_published_at,
                NOW(), NOW(), TRUE
            FROM _norm n
            WHERE n.staging_id = ANY(%s)
            RETURNING job_id, company_id
        """, (new_staging_ids,))
        new_rows = cur.fetchall()
        inserted = len(new_rows)
        for r in new_rows:
            company_metrics[r["company_id"]]["inserted"] += 1
            changed_jobs.append({
                "job_id": r["job_id"],
                "change_type": "inserted",
                "changed_fields": [],
            })

    # -- Phase 5: bulk UPDATE changed / reactivated jobs ------------------
    if changed_job_ids:
        cur.execute("""
            UPDATE jobs SET
                source_url         = n.source_url,
                title              = n.title,
                location           = n.location,
                departments        = n.departments,
                offices            = n.offices,
                language           = n.language,
                description_text   = n.description_text,
                description_html   = n.description_html,
                first_published_at = n.first_published_at,
                is_active          = TRUE,
                date_closed        = NULL,
                extracted_at       = NULL,
                extraction_version = NULL,
                last_seen          = NOW(),
                updated_at         = NOW()
            FROM _norm n
            WHERE jobs.company_id    = n.company_id
              AND jobs.source_job_id = n.source_job_id
              AND jobs.job_id = ANY(%s)
        """, (changed_job_ids,))

    # -- Phase 6: bulk UPDATE unchanged jobs (touch last_seen) ------------
    if unchanged_job_ids:
        cur.execute(
            "UPDATE jobs SET last_seen = NOW() WHERE job_id = ANY(%s)",
            (unchanged_job_ids,),
        )

    # -- Phase 7: closed jobs — single query across all companies ---------
    closed = 0
    if companies_seen:
        cs_company_ids = [p[0] for p in companies_seen]
        cs_scraper_types = [p[1] for p in companies_seen]
        cur.execute("""
            UPDATE jobs
            SET is_active = FALSE, date_closed = NOW(), updated_at = NOW()
            FROM unnest(%s::INTEGER[], %s::TEXT[]) AS v(vid, vtype)
            WHERE jobs.company_id   = v.vid
              AND jobs.scraper_type = v.vtype
              AND jobs.is_active    = TRUE
              AND jobs.last_seen    < CURRENT_DATE
            RETURNING jobs.job_id, jobs.company_id
        """, (cs_company_ids, cs_scraper_types))
        closed_rows = cur.fetchall()
        closed = len(closed_rows)
        for r in closed_rows:
            company_metrics.setdefault(r["company_id"], _empty_company_metrics())
            company_metrics[r["company_id"]]["closed"] += 1
            changed_jobs.append({
                "job_id": r["job_id"],
                "change_type": "closed",
                "changed_fields": [],
            })

    # -- Phase 8: mark every staging row processed in one statement -------
    cur.execute(
        "UPDATE staging_jobs SET processed = TRUE, processed_at = NOW() "
        "WHERE job_id = ANY(%s)",
        (all_staging_ids,),
    )

    conn.commit()

    summary = {
        "inserted": inserted,
        "updated": len(changed_job_ids),
        "unchanged": len(unchanged_job_ids),
        "closed": closed,
        "company_metrics": [
            {
                "company_id": cid,
                "inserted": m["inserted"],
                "updated": m["updated"],
                "unchanged": m["unchanged"],
                "closed": m["closed"],
            }
            for cid, m in company_metrics.items()
        ],
        "changed_jobs": changed_jobs,
    }
    logger.info(f"Staging processed: {summary}")
    return summary


def extract_fields_from_jobs(conn, cur) -> dict:
    """Run field extraction on all jobs that haven't been extracted yet.

    Extraction itself (salary, remote policy, skills) must run in Python,
    but the resulting updates are applied in a single bulk UPDATE via a
    temp table rather than one UPDATE per row.
    """
    skill_matchers = build_skill_matchers(cur)
    skill_exclusions = build_company_skill_exclusions(cur)

    cur.execute("""
        SELECT job_id, company_id, title, description_text, location
        FROM jobs
        WHERE extracted_at IS NULL
          AND is_active = TRUE
        ORDER BY job_id
    """)
    rows = cur.fetchall()

    if not rows:
        logger.info("No unextracted jobs found.")
        return {
            "processed": 0,
            "salary_found": 0,
            "remote_policy_found": 0,
            "skills_found": 0,
            "company_metrics": [],
        }

    processed = 0
    salary_found = 0
    remote_policy_found = 0
    skills_found = 0
    company_metrics: dict[int, dict] = {}
    extraction_tuples: list[tuple] = []

    for row in rows:
        job_id = row["job_id"]
        company_id = row["company_id"]
        title = row["title"]
        desc = row["description_text"]
        loc = row["location"]

        company_metrics.setdefault(company_id, _empty_company_metrics())
        company_metrics[company_id]["extraction_attempted"] += 1

        salary = extract_salary(desc)
        remote_policy = extract_remote_policy(desc, loc)
        skills = extract_skills(
            " ".join(part for part in [title, desc] if part), skill_matchers
        )
        excluded = skill_exclusions.get(company_id)
        if excluded and skills:
            skills = [s for s in skills if s not in excluded]

        s_min = s_max = s_curr = s_period = None
        if salary:
            s_min = salary.salary_min
            s_max = salary.salary_max
            s_curr = salary.salary_currency
            s_period = salary.salary_period
            salary_found += 1
            company_metrics[company_id]["salary_found"] += 1

        rp = None
        if remote_policy:
            rp = remote_policy
            remote_policy_found += 1
            company_metrics[company_id]["remote_policy_found"] += 1

        if skills:
            skills_found += 1
            company_metrics[company_id]["skills_found"] += 1

        extraction_tuples.append((
            job_id, s_min, s_max, s_curr, s_period,
            rp, skills, EXTRACTION_VERSION,
        ))
        processed += 1

    # -- Bulk UPDATE via temp table ---------------------------------------
    cur.execute("""
        CREATE TEMP TABLE _extraction (
            job_id             INTEGER,
            salary_min         NUMERIC,
            salary_max         NUMERIC,
            salary_currency    TEXT,
            salary_period      TEXT,
            remote_policy      TEXT,
            skills             TEXT[],
            extraction_version TEXT
        ) ON COMMIT DROP
    """)

    execute_values(
        cur,
        "INSERT INTO _extraction VALUES %s",
        extraction_tuples,
    )

    cur.execute("""
        UPDATE jobs SET
            salary_min         = COALESCE(e.salary_min, jobs.salary_min),
            salary_max         = COALESCE(e.salary_max, jobs.salary_max),
            salary_currency    = COALESCE(e.salary_currency, jobs.salary_currency),
            salary_period      = COALESCE(e.salary_period, jobs.salary_period),
            remote_policy      = COALESCE(e.remote_policy, jobs.remote_policy),
            skills             = e.skills,
            extraction_version = e.extraction_version,
            extracted_at       = NOW(),
            updated_at         = NOW()
        FROM _extraction e
        WHERE jobs.job_id = e.job_id
    """)

    conn.commit()

    summary = {
        "processed": processed,
        "salary_found": salary_found,
        "remote_policy_found": remote_policy_found,
        "skills_found": skills_found,
        "company_metrics": [
            {
                "company_id": company_id,
                "extraction_attempted": metrics["extraction_attempted"],
                "salary_found": metrics["salary_found"],
                "remote_policy_found": metrics["remote_policy_found"],
                "skills_found": metrics["skills_found"],
            }
            for company_id, metrics in company_metrics.items()
        ],
    }
    logger.info(f"Field extraction complete: {summary}")
    return summary


def purge_processed_staging(conn, cur) -> int:
    """Delete all processed rows from staging_jobs."""
    cur.execute("DELETE FROM staging_jobs WHERE processed = TRUE")
    deleted = cur.rowcount
    conn.commit()
    logger.info(f"Purged {deleted} processed rows from staging_jobs.")
    return deleted


def insert_staging_jobs(conn, cur, data: dict) -> int:
    """Insert jobs from a scraped JSON payload into the staging_jobs table.

    Deletes any existing unprocessed rows for this company before inserting,
    making the task fully idempotent — a retry always loads fresh S3 data
    without leaving duplicate unprocessed rows behind from a prior attempt.

    Args:
        conn: psycopg2 connection
        cur: psycopg2 cursor
        data: parsed JSON from S3 (keys: company, scraped_at, total_jobs, jobs)

    Returns:
        Number of rows inserted.
    """
    company_name = data["company"]
    scraped_at = data["scraped_at"]
    jobs = data["jobs"]

    cur.execute(
        "SELECT company_id, scraper_type FROM companies WHERE company_name = %s",
        (company_name,)
    )
    company_row = cur.fetchone()

    if not company_row:
        raise ValueError(f"Company '{company_name}' not found in companies table.")

    company_id = company_row["company_id"]
    scraper_type = company_row["scraper_type"]

    cur.execute(
        "DELETE FROM staging_jobs WHERE company_id = %s AND processed = FALSE",
        (company_id,),
    )
    if cur.rowcount:
        logger.info(
            f"Cleared {cur.rowcount} stale unprocessed staging rows for {company_name}"
        )

    if not jobs:
        conn.commit()
        return 0

    execute_values(cur, """
        INSERT INTO staging_jobs
            (company_id, scraper_type, source_job_id, source_url, raw_data, scraped_at)
        VALUES %s
    """, [
        (company_id, scraper_type, str(job["id"]), job.get("url"),
         Json(job), scraped_at)
        for job in jobs
    ])

    inserted = len(jobs)
    conn.commit()
    logger.info(f"Inserted {inserted} jobs into staging_jobs for {company_name}")
    return inserted

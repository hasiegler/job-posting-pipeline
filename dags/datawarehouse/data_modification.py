"""
Helper functions for inserting/modifying data in Supabase tables.
"""

import json
import logging
from datetime import datetime

from bs4 import BeautifulSoup
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
    # Coverage columns added after initial release — safe to run on existing tables.
    for col in (
        "salary_coverage_pct NUMERIC",
        "remote_coverage_pct NUMERIC",
        "skills_coverage_pct NUMERIC",
    ):
        cur.execute(
            f"ALTER TABLE pipeline_runs ADD COLUMN IF NOT EXISTS {col};"
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
    """Write a history row for each changed job, capturing its current state.

    Memory-conscious: the row data (including description_html) is copied
    from `jobs` to `job_history` entirely server-side via INSERT ... SELECT,
    joining against a small temp table of (job_id, change_type,
    changed_fields).  Without this, a big-delta run (e.g. first run, batch
    onboarding, backfill) would SELECT every changed job's full HTML into
    Python and then build a giant INSERT string from it — OOM-killing the
    task on a 2 GB droplet.
    """
    ensure_job_history_table(conn, cur)

    if not changed_jobs:
        return {"snapshots_written": 0}

    # Stream only the small change-classification tuples into a temp table;
    # the actual job row data stays inside Postgres.
    cur.execute("""
        CREATE TEMP TABLE _changes (
            job_id         INTEGER PRIMARY KEY,
            change_type    TEXT,
            changed_fields TEXT[]
        ) ON COMMIT DROP
    """)
    execute_values(cur, """
        INSERT INTO _changes (job_id, change_type, changed_fields) VALUES %s
    """, [
        (row["job_id"], row["change_type"], row.get("changed_fields", []))
        for row in changed_jobs
    ])

    # Copy each changed job's current state into job_history in a single
    # server-side statement.  No row data round-trips through Python.
    cur.execute("""
        INSERT INTO job_history (
            job_id, company_id, source_job_id, change_type, changed_fields,
            title, source_url, location, departments, offices, language,
            description_text, description_html, skills,
            salary_min, salary_max, salary_currency, salary_period,
            remote_policy, experience_level, education_required, benefits,
            first_published_at, is_active
        )
        SELECT
            j.job_id, j.company_id, j.source_job_id,
            c.change_type, c.changed_fields,
            j.title, j.source_url, j.location,
            j.departments, j.offices, j.language,
            j.description_text, j.description_html, j.skills,
            j.salary_min, j.salary_max, j.salary_currency, j.salary_period,
            j.remote_policy, j.experience_level, j.education_required, j.benefits,
            j.first_published_at, j.is_active
        FROM jobs j
        JOIN _changes c ON c.job_id = j.job_id
    """)
    written = cur.rowcount

    conn.commit()
    return {"snapshots_written": written}


def upsert_run_monitoring(conn, cur, run_metrics: dict, company_metrics: list[dict]) -> None:
    """Upsert one run summary row and all company rows for that run."""
    ensure_monitoring_tables(conn, cur)

    cur.execute(
        """
        INSERT INTO pipeline_runs (
            dag_id, run_id, run_started_at, run_finished_at, status,
            total_companies, total_scraped, total_staged, total_new, total_updated,
            total_unchanged, total_closed, total_extracted, salary_found,
            remote_policy_found, skills_found,
            salary_coverage_pct, remote_coverage_pct, skills_coverage_pct,
            updated_at
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, NOW()
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
            salary_coverage_pct = EXCLUDED.salary_coverage_pct,
            remote_coverage_pct = EXCLUDED.remote_coverage_pct,
            skills_coverage_pct = EXCLUDED.skills_coverage_pct,
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
            run_metrics.get("salary_coverage_pct"),
            run_metrics.get("remote_coverage_pct"),
            run_metrics.get("skills_coverage_pct"),
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
        # Remove today's stats rows for companies that have since been disabled.
        # company_stats uses upsert (no preceding DELETE), so without this a
        # disabled company's stale today-row would persist across future runs.
        cur.execute(
            """
            DELETE FROM company_stats
            WHERE snapshot_date = %s
              AND company_id IN (SELECT company_id FROM companies WHERE enabled = false);
            """,
            (today,),
        )

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
            JOIN companies c ON c.company_id = j.company_id AND c.enabled = true
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
              AND j.company_id IN (SELECT company_id FROM companies WHERE enabled = true)
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
              AND j.company_id IN (SELECT company_id FROM companies WHERE enabled = true)
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


# ---------------------------------------------------------------------------
# Greenhouse "Location Type" metadata → canonical remote_policy string.
#
# The Greenhouse API includes a `metadata` array on each job object.  One
# entry typically has `{"name": "Location Type", "value": "..."}`.  The
# values Greenhouse emits are mapped to the same canonical strings that
# extract_remote_policy produces ("Remote", "Hybrid", "On-Site") so
# API-derived and text-derived values are always consistent.
# ---------------------------------------------------------------------------
_GH_LOCATION_TYPE_MAP: dict[str, str] = {
    "remote":     "Remote",
    "hybrid":     "Hybrid",
    "on-site":    "On-Site",
    "on site":    "On-Site",
    "onsite":     "On-Site",
    "in-office":  "On-Site",
    "in office":  "On-Site",
}


def _greenhouse_location_type(metadata) -> str | None:
    """Return a canonical remote_policy from the Greenhouse metadata array, or None."""
    for entry in (metadata or []):
        if not isinstance(entry, dict):
            continue
        if (entry.get("name") or "").strip().lower() == "location type":
            val = (entry.get("value") or "").strip()
            return _GH_LOCATION_TYPE_MAP.get(val.lower())
    return None


def normalize_greenhouse(raw_data: dict) -> dict:
    """Normalize a raw Greenhouse API job object to the jobs table schema.

    raw_data is the verbatim JSON object from body["jobs"] as stored in
    staging_jobs.raw_data.  Field names are the Greenhouse API names
    (absolute_url, content, location object, etc.) — NOT the pre-reshaped
    names the old scraper used (url, content_html/text, flat location string).
    """
    loc_obj = raw_data.get("location") or {}
    location = loc_obj.get("name") if isinstance(loc_obj, dict) else loc_obj

    departments = [d["name"] for d in raw_data.get("departments", []) if "name" in d]
    offices = [o["name"] for o in raw_data.get("offices", []) if "name" in o]

    content_html = raw_data.get("content")
    content_text = BeautifulSoup(
        content_html or "", "html.parser"
    ).get_text(separator="\n", strip=True)

    return {
        "source_job_id": str(raw_data.get("id", "")),
        "source_url": raw_data.get("absolute_url"),
        "title": raw_data.get("title"),
        "location": location,
        "departments": departments,
        "offices": offices,
        "language": raw_data.get("language"),
        "description_text": content_text or None,
        "description_html": content_html,
        "first_published_at": _parse_timestamp(raw_data.get("first_published")),
        "skills": None,
        "salary_min": None,
        "salary_max": None,
        "salary_currency": None,
        "salary_period": None,
        "remote_policy": _greenhouse_location_type(raw_data.get("metadata")),
        "experience_level": None,
        "education_required": None,
        "benefits": None,
        "extracted_at": None,
        "extraction_version": None,
    }


# ---------------------------------------------------------------------------
# Ashby normalizer helpers.
#
# These mirror the logic previously baked into scrape_ashby.py's reshape loop.
# They're duplicated here (not imported) to avoid a cross-package dependency
# between api/ and datawarehouse/.
# ---------------------------------------------------------------------------
_ASHBY_WORKPLACE_TYPE_MAP: dict[str, str] = {
    "Remote": "Remote",
    "Hybrid": "Hybrid",
    "OnSite": "On-Site",   # Ashby returns "OnSite"; canonical is "On-Site"
}


def _ashby_combine_locations(raw_data: dict) -> str | None:
    """Concatenate primary location and secondaryLocations[].location with '; '."""
    primary = raw_data.get("location")
    secondary = [
        loc.get("location")
        for loc in (raw_data.get("secondaryLocations") or [])
        if loc.get("location")
    ]
    parts = [p for p in [primary, *secondary] if p]
    return "; ".join(parts) if parts else None


def _ashby_extract_salary(compensation) -> dict:
    """Extract salary min/max/currency/period from an Ashby compensation object."""
    empty: dict = {
        "salary_min": None,
        "salary_max": None,
        "salary_currency": None,
        "salary_period": None,
    }
    if not compensation:
        return empty
    for comp in (compensation.get("summaryComponents") or []):
        if comp.get("compensationType") != "Salary":
            continue
        if comp.get("minValue") is None or comp.get("maxValue") is None:
            continue
        interval = (comp.get("interval") or "").upper()
        if interval == "1 YEAR":
            period: str | None = "yearly"
        elif interval == "1 HOUR":
            period = "hourly"
        else:
            period = None
        return {
            "salary_min": comp.get("minValue"),
            "salary_max": comp.get("maxValue"),
            "salary_currency": comp.get("currencyCode"),
            "salary_period": period,
        }
    return empty


def normalize_ashby(raw_data: dict) -> dict:
    """Normalize a raw Ashby API job object to the jobs table schema.

    raw_data is the verbatim JSON object from body["jobs"] as stored in
    staging_jobs.raw_data.  Field names are the Ashby API names (jobUrl,
    descriptionHtml/Plain, publishedAt, workplaceType, compensation, etc.) —
    NOT the pre-shaped names the old scraper used.

    Unlike Greenhouse, Ashby provides salary and remote_policy directly on the
    posting via its API, so those fields are populated here.
    `extract_fields_from_jobs` respects them and only falls back to the
    description-text extractors when the API didn't supply a value.
    """
    salary = _ashby_extract_salary(raw_data.get("compensation"))
    dept = raw_data.get("department")

    return {
        "source_job_id": str(raw_data.get("id", "")),
        "source_url": raw_data.get("jobUrl"),
        "title": raw_data.get("title"),
        "location": _ashby_combine_locations(raw_data),
        "departments": [dept] if dept else [],
        "offices": [],
        "language": None,
        "description_text": raw_data.get("descriptionPlain") or None,
        "description_html": raw_data.get("descriptionHtml"),
        "first_published_at": _parse_timestamp(raw_data.get("publishedAt")),
        "salary_min": salary["salary_min"],
        "salary_max": salary["salary_max"],
        "salary_currency": salary["salary_currency"],
        "salary_period": salary["salary_period"],
        "remote_policy": _ASHBY_WORKPLACE_TYPE_MAP.get(raw_data.get("workplaceType")),
        "skills": None,
        "experience_level": None,
        "education_required": None,
        "benefits": None,
        "extracted_at": None,
        "extraction_version": None,
    }


NORMALIZERS = {
    "greenhouse": normalize_greenhouse,
    "ashby": normalize_ashby,
}


def process_staging_to_jobs(conn, cur) -> dict:
    """Process all unprocessed staging_jobs rows into the jobs table.

    Memory-conscious design for a 2 GB droplet: a single `SELECT * FROM
    staging_jobs` pulls every job's raw_data JSONB (kilobytes of HTML each)
    into Python and OOM-kills the worker.  Instead we:

      0. Discover which (company_id, scraper_type) pairs have unprocessed
         staging rows.
      1+2. For each pair, fetch only that company's staging rows, normalise
         them in Python, and stream them into the _norm temp table.  Peak
         Python memory is bounded to one company's payload at a time, not
         the entire fleet.
      3. Classify _norm against jobs with a single JOIN that builds the
         changed-field list server-side via IS DISTINCT FROM — Python never
         sees description_html columns from both sides at once.
      4–8. Bulk INSERT / UPDATE / close-detect, all set-based SQL on the
         server, then mark every staging row processed in one statement.
    """
    # -- Phase 0: discover companies with unprocessed staging rows -------
    cur.execute("""
        SELECT DISTINCT company_id, scraper_type
        FROM staging_jobs
        WHERE processed = FALSE
        ORDER BY company_id
    """)
    company_pairs = [
        (r["company_id"], r["scraper_type"]) for r in cur.fetchall()
    ]

    if not company_pairs:
        logger.info("No unprocessed staging rows found.")
        return {
            "inserted": 0,
            "updated": 0,
            "unchanged": 0,
            "closed": 0,
            "company_metrics": [],
            "changed_jobs": [],
        }

    company_metrics: dict[int, dict] = {
        cid: _empty_company_metrics() for cid, _ in company_pairs
    }
    companies_seen: set[tuple[int, str]] = set(company_pairs)
    all_staging_ids: list[int] = []

    # -- Create the _norm temp table once for the whole transaction -----
    # salary_* and remote_policy travel through _norm so ATSes that expose
    # those fields on their API (currently only Ashby) can populate them at
    # this step instead of relying on the description-text extractors.
    # For Greenhouse the normalizer returns None for all five columns.
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
            first_published_at TIMESTAMPTZ,
            salary_min         NUMERIC,
            salary_max         NUMERIC,
            salary_currency    TEXT,
            salary_period      TEXT,
            remote_policy      TEXT
        ) ON COMMIT DROP
    """)

    # -- Phase 1+2: per-company normalize + stream into _norm ------------
    for company_id, scraper_type in company_pairs:
        cur.execute("""
            SELECT job_id, raw_data
            FROM staging_jobs
            WHERE processed = FALSE
              AND company_id = %s
              AND scraper_type = %s
            ORDER BY job_id
        """, (company_id, scraper_type))
        staging_rows = cur.fetchall()

        all_staging_ids.extend(r["job_id"] for r in staging_rows)

        normalizer = NORMALIZERS.get(scraper_type)
        if not normalizer:
            logger.warning(
                f"No normalizer for scraper_type '{scraper_type}', "
                f"skipping company_id={company_id} ({len(staging_rows)} rows)"
            )
            continue

        # Deduplicate within this company by source_job_id; rows arrive in
        # job_id ASC so last-write-wins matches the prior implementation.
        deduped: dict[str, dict] = {}
        for row in staging_rows:
            raw_data = row["raw_data"]
            if isinstance(raw_data, str):
                raw_data = json.loads(raw_data)
            norm = normalizer(raw_data)
            deduped[norm["source_job_id"]] = {
                "staging_id": row["job_id"],
                "company_id": company_id,
                "scraper_type": scraper_type,
                **norm,
            }
        # Free this company's raw payload before building the INSERT.
        del staging_rows

        if not deduped:
            continue

        execute_values(cur, """
            INSERT INTO _norm (
                staging_id, company_id, scraper_type, source_job_id, source_url,
                title, location, departments, offices, language,
                description_text, description_html, first_published_at,
                salary_min, salary_max, salary_currency, salary_period, remote_policy
            ) VALUES %s
        """, [
            (
                r["staging_id"], r["company_id"], r["scraper_type"],
                r["source_job_id"], r["source_url"], r["title"], r["location"],
                r["departments"], r["offices"], r["language"],
                r["description_text"], r["description_html"],
                r["first_published_at"],
                r["salary_min"], r["salary_max"], r["salary_currency"],
                r["salary_period"], r["remote_policy"],
            )
            for r in deduped.values()
        ])
        del deduped

    # -- Phase 3: classify with server-side change detection -------------
    # NOTE: salary_*/remote_policy are intentionally NOT compared here —
    # those are API-sourced for some ATSes and extractor-sourced for others,
    # so churn on those columns shouldn't classify a job as "changed".
    # IS DISTINCT FROM treats NULL-vs-NULL as equal (unlike `=`).
    # The CASE expressions build a TEXT[] of just the field names that
    # changed; description_text/description_html never travel back to Python.
    cur.execute("""
        SELECT
            n.staging_id,
            n.company_id,
            n.source_job_id,
            j.job_id   AS existing_job_id,
            j.is_active AS old_is_active,
            ARRAY_REMOVE(ARRAY[
                CASE WHEN j.source_url         IS DISTINCT FROM n.source_url         THEN 'source_url' END,
                CASE WHEN j.title              IS DISTINCT FROM n.title              THEN 'title' END,
                CASE WHEN j.location           IS DISTINCT FROM n.location           THEN 'location' END,
                CASE WHEN j.departments        IS DISTINCT FROM n.departments        THEN 'departments' END,
                CASE WHEN j.offices            IS DISTINCT FROM n.offices            THEN 'offices' END,
                CASE WHEN j.language           IS DISTINCT FROM n.language           THEN 'language' END,
                CASE WHEN j.description_text   IS DISTINCT FROM n.description_text   THEN 'description_text' END,
                CASE WHEN j.description_html   IS DISTINCT FROM n.description_html   THEN 'description_html' END,
                CASE WHEN j.first_published_at IS DISTINCT FROM n.first_published_at THEN 'first_published_at' END
            ]::text[], NULL) AS changed_fields
        FROM _norm n
        LEFT JOIN jobs j
            ON  j.company_id    = n.company_id
            AND j.source_job_id = n.source_job_id
    """)
    classified = cur.fetchall()

    new_staging_ids: list[int] = []
    changed_job_ids: list[int] = []
    unchanged_job_ids: list[int] = []
    changed_jobs: list[dict] = []

    for row in classified:
        cid = row["company_id"]
        if row["existing_job_id"] is None:
            new_staging_ids.append(row["staging_id"])
            continue

        changed_fields = list(row["changed_fields"] or [])
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
                salary_min, salary_max, salary_currency, salary_period,
                remote_policy,
                first_seen, last_seen, is_active
            )
            SELECT
                n.company_id, n.scraper_type, n.source_job_id, n.source_url,
                n.title, n.location, n.departments, n.offices, n.language,
                n.description_text, n.description_html, n.first_published_at,
                n.salary_min, n.salary_max, n.salary_currency, n.salary_period,
                n.remote_policy,
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
    # COALESCE on the API-sourced fields keeps Greenhouse jobs' extractor-
    # populated salary/remote_policy intact (Greenhouse's normalizer returns
    # None for all five), while Ashby refreshes them from the API on each run.
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
                salary_min         = COALESCE(n.salary_min, jobs.salary_min),
                salary_max         = COALESCE(n.salary_max, jobs.salary_max),
                salary_currency    = COALESCE(n.salary_currency, jobs.salary_currency),
                salary_period      = COALESCE(n.salary_period, jobs.salary_period),
                remote_policy      = COALESCE(n.remote_policy, jobs.remote_policy),
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

    Memory-conscious: jobs are fetched and extracted one company at a time
    so the description_text column for tens of thousands of unextracted
    rows never lands in Python memory at once.  Extraction itself (salary,
    remote policy, skills) still runs in Python; the per-company batches
    are accumulated into a single _extraction temp table and applied via
    one bulk UPDATE at the end.
    """
    skill_matchers = build_skill_matchers(cur)
    skill_exclusions = build_company_skill_exclusions(cur)

    cur.execute("""
        SELECT DISTINCT company_id
        FROM jobs
        WHERE extracted_at IS NULL
          AND is_active = TRUE
        ORDER BY company_id
    """)
    company_ids = [r["company_id"] for r in cur.fetchall()]

    if not company_ids:
        logger.info("No unextracted jobs found.")
        return {
            "processed": 0,
            "salary_found": 0,
            "remote_policy_found": 0,
            "skills_found": 0,
            "company_metrics": [],
        }

    # -- Create temp table once for the whole transaction ----------------
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

    processed = 0
    salary_found = 0
    remote_policy_found = 0
    skills_found = 0
    company_metrics: dict[int, dict] = {}

    # -- Per-company: fetch, extract, stream into _extraction ------------
    for company_id in company_ids:
        cur.execute("""
            SELECT
                job_id, scraper_type,
                title, description_text, location,
                salary_min, remote_policy
            FROM jobs
            WHERE extracted_at IS NULL
              AND is_active = TRUE
              AND company_id = %s
            ORDER BY job_id
        """, (company_id,))
        rows = cur.fetchall()

        if not rows:
            continue

        company_metrics.setdefault(company_id, _empty_company_metrics())
        excluded = skill_exclusions.get(company_id)
        extraction_tuples: list[tuple] = []

        for row in rows:
            job_id = row["job_id"]
            scraper_type = row["scraper_type"]
            title = row["title"]
            desc = row["description_text"]
            loc = row["location"]

            company_metrics[company_id]["extraction_attempted"] += 1

            # Ashby and Greenhouse both surface remote_policy directly from
            # their APIs at normalization time (Ashby via workplaceType, Greenhouse
            # via the metadata "Location Type" entry).  Only fall back to the
            # description-text extractor when the API didn't supply a value, so
            # API-sourced values are never silently overwritten.
            # Salary: only Ashby exposes it via API; always run the extractor for
            # Greenhouse (and any other ATS) since they never provide salary.
            if scraper_type == "ashby":
                salary = extract_salary(desc) if row["salary_min"] is None else None
            else:
                salary = extract_salary(desc)

            if scraper_type in ("ashby", "greenhouse"):
                remote_policy = (
                    extract_remote_policy(desc, loc)
                    if row["remote_policy"] is None
                    else None
                )
            else:
                remote_policy = extract_remote_policy(desc, loc)
            skills = extract_skills(
                " ".join(part for part in [title, desc] if part), skill_matchers
            )
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

        # Release the per-company description payload before the INSERT.
        del rows

        execute_values(
            cur,
            "INSERT INTO _extraction VALUES %s",
            extraction_tuples,
        )
        del extraction_tuples

    # -- Bulk UPDATE jobs from the accumulated _extraction table ---------
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

    # Both Greenhouse and Ashby jobs are now raw API objects.
    # Greenhouse:  source_url → "absolute_url"
    # Ashby:       source_url → "jobUrl"
    _URL_KEYS = {"greenhouse": "absolute_url", "ashby": "jobUrl"}
    url_key = _URL_KEYS.get(scraper_type, "url")

    execute_values(cur, """
        INSERT INTO staging_jobs
            (company_id, scraper_type, source_job_id, source_url, raw_data, scraped_at)
        VALUES %s
    """, [
        (company_id, scraper_type, str(job["id"]), job.get(url_key),
         Json(job), scraped_at)
        for job in jobs
    ])

    inserted = len(jobs)
    conn.commit()
    logger.info(f"Inserted {inserted} jobs into staging_jobs for {company_name}")
    return inserted

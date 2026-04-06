"""
One-time script to create database tables in Supabase.
Run locally or from inside the Airflow container:
    set -a && source .env && set +a && python dags/datawarehouse/init_db.py
"""

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from datawarehouse.data_utils import get_conn_cursor, close_conn_cursor

def create_companies_table():
    conn, cur = get_conn_cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS companies (
            company_id    SERIAL PRIMARY KEY,
            company_name  TEXT NOT NULL UNIQUE,
            scraper_type  TEXT NOT NULL,
            base_url      TEXT NOT NULL,
            enabled       BOOLEAN NOT NULL DEFAULT TRUE,
            canonical_name TEXT,
            created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
    """)

    conn.commit()
    print("companies table created successfully.")
    close_conn_cursor(conn, cur)


def create_staging_jobs_table():
    conn, cur = get_conn_cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS staging_jobs (
            job_id         SERIAL PRIMARY KEY,
            company_id     INTEGER NOT NULL REFERENCES companies(company_id),
            scraper_type   TEXT NOT NULL,
            source_job_id  TEXT NOT NULL,
            source_url     TEXT,
            raw_data       JSONB NOT NULL,
            scraped_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            processed      BOOLEAN NOT NULL DEFAULT FALSE,
            processed_at   TIMESTAMPTZ
        );
    """)

    conn.commit()
    print("staging_jobs table created successfully.")
    close_conn_cursor(conn, cur)


def create_jobs_table():
    conn, cur = get_conn_cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS jobs (
            job_id              SERIAL PRIMARY KEY,
            company_id          INTEGER NOT NULL REFERENCES companies(company_id),
            scraper_type        TEXT NOT NULL,
            source_job_id       TEXT,
            source_url          TEXT,
            title               TEXT NOT NULL,
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
            extracted_at        TIMESTAMPTZ,
            extraction_version  TEXT,
            first_published_at  TIMESTAMPTZ,
            first_seen          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            last_seen           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            is_active           BOOLEAN NOT NULL DEFAULT TRUE,
            date_closed         TIMESTAMPTZ,
            updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE(company_id, source_job_id)
        );
    """)

    conn.commit()
    print("jobs table created successfully.")
    close_conn_cursor(conn, cur)


def create_skills_table():
    conn, cur = get_conn_cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS skills (
            skill_id      SERIAL PRIMARY KEY,
            skill_name    TEXT NOT NULL,
            category      TEXT NOT NULL,
            aliases       TEXT[] NOT NULL,
            is_active     BOOLEAN NOT NULL DEFAULT TRUE,
            created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE(skill_name, category),
            CHECK (COALESCE(array_length(aliases, 1), 0) >= 2)
        );
    """)

    conn.commit()
    print("skills table created successfully.")
    close_conn_cursor(conn, cur)


def create_company_analytics_tables():
    conn, cur = get_conn_cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS company_stats (
            company_id        INTEGER NOT NULL REFERENCES companies(company_id),
            snapshot_date     DATE    NOT NULL,
            active_jobs       INTEGER NOT NULL DEFAULT 0,
            posted_7d         INTEGER NOT NULL DEFAULT 0,
            posted_30d        INTEGER NOT NULL DEFAULT 0,
            closed_7d         INTEGER,
            closed_30d        INTEGER,
            net_change_7d     INTEGER,
            net_change_30d    INTEGER,
            remote_count      INTEGER NOT NULL DEFAULT 0,
            hybrid_count      INTEGER NOT NULL DEFAULT 0,
            onsite_count      INTEGER NOT NULL DEFAULT 0,
            avg_salary_min    NUMERIC,
            avg_salary_max    NUMERIC,
            median_salary_min NUMERIC,
            median_salary_max NUMERIC,
            PRIMARY KEY (company_id, snapshot_date)
        );
    """)
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_company_stats_date
        ON company_stats (snapshot_date DESC, company_id);
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS company_skills (
            company_id    INTEGER NOT NULL REFERENCES companies(company_id),
            snapshot_date DATE    NOT NULL,
            skill_name    TEXT    NOT NULL,
            mention_count INTEGER NOT NULL DEFAULT 0,
            percentage    NUMERIC NOT NULL DEFAULT 0,
            PRIMARY KEY (company_id, snapshot_date, skill_name)
        );
    """)
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_company_skills_date
        ON company_skills (snapshot_date DESC, company_id);
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS company_departments (
            company_id       INTEGER NOT NULL REFERENCES companies(company_id),
            snapshot_date    DATE    NOT NULL,
            department_name  TEXT    NOT NULL,
            active_job_count INTEGER NOT NULL DEFAULT 0,
            percentage       NUMERIC NOT NULL DEFAULT 0,
            PRIMARY KEY (company_id, snapshot_date, department_name)
        );
    """)
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_company_departments_date
        ON company_departments (snapshot_date DESC, company_id);
    """)

    conn.commit()
    print("company analytics tables created successfully.")
    close_conn_cursor(conn, cur)


def create_monitoring_tables():
    conn, cur = get_conn_cursor()

    cur.execute("""
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
    """)

    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_pipeline_runs_started
        ON pipeline_runs (run_started_at DESC);
    """)

    cur.execute("""
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
    """)

    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_company_run_metrics_company_started
        ON company_run_metrics (company_id, run_started_at DESC);
    """)

    conn.commit()
    print("monitoring tables created successfully.")
    close_conn_cursor(conn, cur)


def create_job_history_table():
    conn, cur = get_conn_cursor()

    cur.execute("""
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
    """)

    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_job_history_job_id
        ON job_history (job_id, recorded_at DESC);
    """)
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_job_history_company
        ON job_history (company_id, recorded_at DESC);
    """)
    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_job_history_change_type
        ON job_history (change_type, recorded_at DESC);
    """)

    conn.commit()
    print("job_history table created successfully.")
    close_conn_cursor(conn, cur)


def seed_job_history():
    """Manual one-time baseline seed for active jobs."""
    conn, cur = get_conn_cursor()
    create_job_history_table()

    cur.execute("""
        INSERT INTO job_history (
            job_id, company_id, source_job_id, change_type, changed_fields,
            title, source_url, location, departments, offices, language,
            description_text, description_html, skills,
            salary_min, salary_max, salary_currency, salary_period,
            remote_policy, experience_level, education_required, benefits,
            first_published_at, is_active, recorded_at
        )
        SELECT
            job_id, company_id, source_job_id, 'seed', '{}'::TEXT[],
            title, source_url, location, departments, offices, language,
            description_text, description_html, skills,
            salary_min, salary_max, salary_currency, salary_period,
            remote_policy, experience_level, education_required, benefits,
            first_published_at, is_active, NOW()
        FROM jobs
        WHERE is_active = TRUE;
    """)

    seeded = cur.rowcount
    conn.commit()
    print(f"Seeded {seeded} job_history rows.")
    close_conn_cursor(conn, cur)


if __name__ == "__main__":
    create_companies_table()
    create_staging_jobs_table()
    create_jobs_table()
    create_skills_table()
    create_job_history_table()
    create_company_analytics_tables()
    create_monitoring_tables()
    print("\nDone.")

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


if __name__ == "__main__":
    create_companies_table()
    create_staging_jobs_table()
    create_jobs_table()
    create_skills_table()
    create_monitoring_tables()
    print("\nDone.")

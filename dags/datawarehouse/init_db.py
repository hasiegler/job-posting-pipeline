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
            UNIQUE(company_id, source_job_id)
        );
    """)

    conn.commit()
    print("jobs table created successfully.")
    close_conn_cursor(conn, cur)


if __name__ == "__main__":
    create_companies_table()
    create_staging_jobs_table()
    create_jobs_table()
    print("\nDone.")

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
            created_at    TIMESTAMP NOT NULL DEFAULT NOW(),
            updated_at    TIMESTAMP NOT NULL DEFAULT NOW()
        );
    """)

    conn.commit()
    print("companies table created successfully.")
    close_conn_cursor(conn, cur)


if __name__ == "__main__":
    create_companies_table()
    print("\nDone.")

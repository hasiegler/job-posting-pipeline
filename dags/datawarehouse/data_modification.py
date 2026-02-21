"""
Helper functions for inserting/modifying data in Supabase tables.
"""

import json
import logging

logger = logging.getLogger(__name__)


def insert_staging_jobs(conn, cur, data: dict) -> int:
    """Insert jobs from a scraped JSON payload into the staging_jobs table.

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

    inserted = 0
    for job in jobs:
        cur.execute("""
            INSERT INTO staging_jobs (company_id, scraper_type, source_job_id, source_url, raw_data, scraped_at)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            company_id,
            scraper_type,
            str(job["id"]),
            job.get("url"),
            json.dumps(job),
            scraped_at,
        ))
        inserted += 1

    conn.commit()
    logger.info(f"Inserted {inserted} jobs into staging_jobs for {company_name}")
    return inserted

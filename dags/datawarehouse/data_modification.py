"""
Helper functions for inserting/modifying data in Supabase tables.
"""

import json
import logging
from datetime import datetime
from urllib.parse import urlparse, urlunparse

from extraction import EXTRACTION_VERSION
from extraction.salary import extract_salary

logger = logging.getLogger(__name__)


def clean_url(url: str) -> str:
    """Strip query parameters and fragments from a URL."""
    if not url:
        return url
    parsed = urlparse(url)
    return urlunparse((parsed.scheme, parsed.netloc, parsed.path, "", "", ""))


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
        "source_url": clean_url(raw_data.get("url")),
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
    """Process all unprocessed staging_jobs rows into the jobs table."""
    cur.execute("""
        SELECT job_id, company_id, scraper_type, source_job_id, raw_data, scraped_at
        FROM staging_jobs
        WHERE processed = FALSE
        ORDER BY company_id, job_id
    """)
    staging_rows = cur.fetchall()

    if not staging_rows:
        logger.info("No unprocessed staging rows found.")
        return {"inserted": 0, "updated": 0, "unchanged": 0, "closed": 0}

    inserted = 0
    updated = 0
    unchanged = 0
    companies_seen = set()

    for row in staging_rows:
        staging_id = row["job_id"]
        company_id = row["company_id"]
        scraper_type = row["scraper_type"]
        raw_data = row["raw_data"]
        if isinstance(raw_data, str):
            raw_data = json.loads(raw_data)

        companies_seen.add((company_id, scraper_type))

        normalizer = NORMALIZERS.get(scraper_type)
        if not normalizer:
            logger.warning(f"No normalizer for scraper_type '{scraper_type}', skipping staging_id={staging_id}")
            continue

        normalized = normalizer(raw_data)

        cur.execute("""
            SELECT job_id, source_url, title, location, departments, offices,
                   language, description_text, description_html, first_published_at,
                   is_active
            FROM jobs
            WHERE company_id = %s AND source_job_id = %s
        """, (company_id, normalized["source_job_id"]))
        existing = cur.fetchone()

        if existing:
            changes = {}
            compare_fields = [
                "source_url", "title", "location", "departments", "offices",
                "language", "description_text", "description_html", "first_published_at",
            ]
            for field in compare_fields:
                new_val = normalized.get(field)
                old_val = existing.get(field)
                if new_val != old_val:
                    changes[field] = new_val

            if not existing["is_active"]:
                changes["is_active"] = True
                changes["date_closed"] = None
                logger.info(f"Reactivated job {normalized['source_job_id']}")

            if changes:
                changes["extracted_at"] = None
                changes["extraction_version"] = None
                set_clauses = [f"{f} = %s" for f in changes]
                set_clauses.append("last_seen = NOW()")
                set_clauses.append("updated_at = NOW()")
                values = list(changes.values())
                values.append(existing["job_id"])
                cur.execute(
                    f"UPDATE jobs SET {', '.join(set_clauses)} WHERE job_id = %s",
                    values,
                )
                updated += 1
                logger.info(f"Updated job {normalized['source_job_id']}: {list(changes.keys())}")
            else:
                cur.execute(
                    "UPDATE jobs SET last_seen = NOW() WHERE job_id = %s",
                    (existing["job_id"],)
                )
                unchanged += 1
        else:
            cur.execute("""
                INSERT INTO jobs (
                    company_id, scraper_type, source_job_id, source_url,
                    title, location, departments, offices, language,
                    description_text, description_html, first_published_at,
                    first_seen, last_seen, is_active
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), NOW(), TRUE)
            """, (
                company_id, scraper_type,
                normalized["source_job_id"], normalized["source_url"],
                normalized["title"], normalized["location"],
                normalized["departments"], normalized["offices"],
                normalized["language"], normalized["description_text"],
                normalized["description_html"], normalized["first_published_at"],
            ))
            inserted += 1

        cur.execute("""
            UPDATE staging_jobs SET processed = TRUE, processed_at = NOW()
            WHERE job_id = %s
        """, (staging_id,))

    # Mark closed jobs: active jobs not seen today for each company scraped
    closed = 0
    for company_id, scraper_type in companies_seen:
        cur.execute("""
            UPDATE jobs
            SET is_active = FALSE, date_closed = NOW(), updated_at = NOW()
            WHERE company_id = %s
              AND scraper_type = %s
              AND is_active = TRUE
              AND last_seen < CURRENT_DATE
            RETURNING job_id
        """, (company_id, scraper_type))
        closed += cur.rowcount

    conn.commit()

    summary = {"inserted": inserted, "updated": updated, "unchanged": unchanged, "closed": closed}
    logger.info(f"Staging processed: {summary}")
    return summary


def extract_fields_from_jobs(conn, cur) -> dict:
    """Run field extraction on all jobs that haven't been extracted yet."""
    cur.execute("""
        SELECT job_id, description_text
        FROM jobs
        WHERE extracted_at IS NULL
        ORDER BY job_id
    """)
    rows = cur.fetchall()

    if not rows:
        logger.info("No unextracted jobs found.")
        return {"processed": 0, "salary_found": 0}

    processed = 0
    salary_found = 0

    for row in rows:
        job_id = row["job_id"]
        salary = extract_salary(row["description_text"])

        if salary:
            cur.execute("""
                UPDATE jobs
                SET salary_min = %s, salary_max = %s, salary_currency = %s,
                    salary_period = %s, extracted_at = NOW(),
                    extraction_version = %s, updated_at = NOW()
                WHERE job_id = %s
            """, (
                salary.salary_min, salary.salary_max,
                salary.salary_currency, salary.salary_period,
                EXTRACTION_VERSION, job_id,
            ))
            salary_found += 1
        else:
            cur.execute("""
                UPDATE jobs
                SET extracted_at = NOW(), extraction_version = %s,
                    updated_at = NOW()
                WHERE job_id = %s
            """, (EXTRACTION_VERSION, job_id))

        processed += 1

    conn.commit()

    summary = {"processed": processed, "salary_found": salary_found}
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

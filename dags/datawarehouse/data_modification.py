"""
Helper functions for inserting/modifying data in Supabase tables.
"""

import json
import logging
from datetime import datetime
from urllib.parse import urlparse, urlunparse

from extraction import EXTRACTION_VERSION
from extraction.salary import extract_salary
from extraction.remote_policy import extract_remote_policy
from extraction.skills import build_skill_matchers, extract_skills

logger = logging.getLogger(__name__)


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

    for row in company_metrics:
        cur.execute(
            """
            INSERT INTO company_run_metrics (
                dag_id, run_id, run_started_at, company_id, company_name,
                scraped_jobs, staged_jobs, new_jobs, updated_jobs, unchanged_jobs,
                closed_jobs, extraction_attempted, salary_found, remote_policy_found,
                skills_found, updated_at
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW()
            )
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
                updated_at = NOW();
            """,
            (
                row["dag_id"],
                row["run_id"],
                row["run_started_at"],
                row["company_id"],
                row["company_name"],
                row["scraped_jobs"],
                row["staged_jobs"],
                row["new_jobs"],
                row["updated_jobs"],
                row["unchanged_jobs"],
                row["closed_jobs"],
                row["extraction_attempted"],
                row["salary_found"],
                row["remote_policy_found"],
                row["skills_found"],
            ),
        )

    conn.commit()


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
        return {"inserted": 0, "updated": 0, "unchanged": 0, "closed": 0, "company_metrics": []}

    inserted = 0
    updated = 0
    unchanged = 0
    companies_seen = set()
    company_metrics = {}

    for row in staging_rows:
        staging_id = row["job_id"]
        company_id = row["company_id"]
        scraper_type = row["scraper_type"]
        raw_data = row["raw_data"]
        if isinstance(raw_data, str):
            raw_data = json.loads(raw_data)

        companies_seen.add((company_id, scraper_type))
        company_metrics.setdefault(company_id, _empty_company_metrics())

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
                company_metrics[company_id]["updated"] += 1
                logger.info(f"Updated job {normalized['source_job_id']}: {list(changes.keys())}")
            else:
                cur.execute(
                    "UPDATE jobs SET last_seen = NOW() WHERE job_id = %s",
                    (existing["job_id"],)
                )
                unchanged += 1
                company_metrics[company_id]["unchanged"] += 1
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
            company_metrics[company_id]["inserted"] += 1

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
        company_metrics.setdefault(company_id, _empty_company_metrics())
        company_metrics[company_id]["closed"] += cur.rowcount

    conn.commit()

    summary = {
        "inserted": inserted,
        "updated": updated,
        "unchanged": unchanged,
        "closed": closed,
        "company_metrics": [
            {
                "company_id": company_id,
                "inserted": metrics["inserted"],
                "updated": metrics["updated"],
                "unchanged": metrics["unchanged"],
                "closed": metrics["closed"],
            }
            for company_id, metrics in company_metrics.items()
        ],
    }
    logger.info(f"Staging processed: {summary}")
    return summary


def extract_fields_from_jobs(conn, cur) -> dict:
    """Run field extraction on all jobs that haven't been extracted yet."""
    skill_matchers = build_skill_matchers(cur)

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
    company_metrics = {}

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
        skills = extract_skills(" ".join(part for part in [title, desc] if part), skill_matchers)

        fields = {
            "extracted_at": "NOW()",
            "extraction_version": EXTRACTION_VERSION,
            "updated_at": "NOW()",
        }
        params = []

        if salary:
            fields["salary_min"] = salary.salary_min
            fields["salary_max"] = salary.salary_max
            fields["salary_currency"] = salary.salary_currency
            fields["salary_period"] = salary.salary_period
            salary_found += 1
            company_metrics[company_id]["salary_found"] += 1

        if remote_policy:
            fields["remote_policy"] = remote_policy
            remote_policy_found += 1
            company_metrics[company_id]["remote_policy_found"] += 1

        fields["skills"] = skills
        if skills:
            skills_found += 1
            company_metrics[company_id]["skills_found"] += 1

        set_clauses = []
        for col, val in fields.items():
            if val == "NOW()":
                set_clauses.append(f"{col} = NOW()")
            else:
                set_clauses.append(f"{col} = %s")
                params.append(val)

        params.append(job_id)
        cur.execute(
            f"UPDATE jobs SET {', '.join(set_clauses)} WHERE job_id = %s",
            params,
        )
        processed += 1

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

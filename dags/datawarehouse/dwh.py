"""
Airflow task functions for loading data into Supabase tables.
"""

from datetime import datetime, timezone

try:
    from airflow.decorators import task
    from airflow.operators.python import get_current_context
except ImportError:
    def task(func):
        func.function = func
        return func

    def get_current_context():
        return {}

from datawarehouse.data_utils import (
    get_conn_cursor,
    close_conn_cursor,
    run_with_db,
)
from datawarehouse.data_loading import load_s3_json
from datawarehouse.data_modification import (
    insert_staging_jobs,
    process_staging_to_jobs,
    extract_fields_from_jobs,
    purge_processed_staging,
    snapshot_changed_jobs,
    upsert_run_monitoring,
    refresh_company_analytics,
)


@task
def update_staging_jobs(s3_path: str | None) -> dict | None:
    """Load a JSON file from S3 and insert its jobs into the staging_jobs table.

    `s3_path` is `None` when the upstream `scrape_all_companies` returned
    a sentinel (the scrape was skipped) — in that case we no-op and return
    None so the rest of the pipeline keeps moving with the companies that
    DID succeed.
    """
    if s3_path is None:
        print("  Skipping staging load — upstream scrape was skipped.")
        return None

    data = load_s3_json(s3_path)

    inserted = run_with_db(lambda conn, cur: insert_staging_jobs(conn, cur, data))

    company = data["company"]
    scraped_jobs = int(data.get("total_jobs", inserted))
    print(f"  Loaded {inserted} jobs into staging_jobs for {company}")
    return {"company": company, "staged_jobs": inserted, "scraped_jobs": scraped_jobs}


@task
def update_jobs_table() -> dict:
    """Process all unprocessed staging_jobs rows into the jobs table."""
    summary = run_with_db(process_staging_to_jobs)
    print(f"  Jobs table updated: {summary}")
    return summary


@task
def snapshot_job_changes(jobs_summary: dict) -> dict:
    """Write history snapshots for all jobs that changed in this run."""
    changed_jobs = (jobs_summary or {}).get("changed_jobs", [])
    if not changed_jobs:
        print("  No job changes to snapshot.")
        return {"snapshots_written": 0}

    summary = run_with_db(
        lambda conn, cur: snapshot_changed_jobs(conn, cur, changed_jobs)
    )
    print(f"  Job history snapshots: {summary}")
    return summary


@task
def extract_fields() -> dict:
    """Extract structured fields (salary, etc.) from unprocessed job descriptions."""
    summary = run_with_db(extract_fields_from_jobs)
    print(f"  Fields extracted: {summary}")
    return summary


@task
def clean_staging() -> dict:
    """Delete processed rows from staging_jobs."""
    deleted = run_with_db(purge_processed_staging)
    print(f"  Purged {deleted} processed staging rows")
    return {"deleted": deleted}


@task
def refresh_analytics() -> dict:
    """Snapshot precomputed company analytics from the current jobs table."""
    summary = run_with_db(refresh_company_analytics)
    print(f"  Company analytics refreshed: {summary}")
    return summary


@task
def finalize_run_metrics(
    sync_summary: dict,
    staging_summaries: list[dict],
    jobs_summary: dict,
    extraction_summary: dict,
    cleanup_summary: dict,
) -> dict:
    """Aggregate task summaries and persist run/company metrics to Supabase."""
    _ = cleanup_summary  # Placeholder if cleanup metrics are needed later.

    context = get_current_context()
    dag = context.get("dag")
    dag_run = context.get("dag_run")
    dag_id = dag.dag_id if dag else "unknown_dag"
    run_id = dag_run.run_id if dag_run else f"manual_{datetime.now(timezone.utc).isoformat()}"
    run_started_at = dag_run.start_date if dag_run and dag_run.start_date else datetime.now(timezone.utc)
    run_finished_at = datetime.now(timezone.utc)

    # Skipped mapped task instances (from scrape failures upstream) can show
    # up as None entries in the list, depending on Airflow's mapped-task
    # XCom resolution. Filter them out so we don't crash on `.get`.
    staging_summaries = [s for s in (staging_summaries or []) if s]
    jobs_summary = jobs_summary or {}
    extraction_summary = extraction_summary or {}
    sync_summary = sync_summary or {}

    conn, cur = get_conn_cursor()

    cur.execute("SELECT company_id, company_name FROM companies")
    company_rows = cur.fetchall()
    id_to_name = {row["company_id"]: row["company_name"] for row in company_rows}
    name_to_id = {row["company_name"]: row["company_id"] for row in company_rows}

    per_company = {}

    for row in staging_summaries:
        company_name = row.get("company")
        if not company_name:
            continue
        company_id = name_to_id.get(company_name)
        if not company_id:
            continue
        per_company.setdefault(
            company_id,
            {
                "company_id": company_id,
                "company_name": company_name,
                "scraped_jobs": 0,
                "staged_jobs": 0,
                "new_jobs": 0,
                "updated_jobs": 0,
                "unchanged_jobs": 0,
                "closed_jobs": 0,
                "extraction_attempted": 0,
                "salary_found": 0,
                "remote_policy_found": 0,
                "skills_found": 0,
            },
        )
        per_company[company_id]["scraped_jobs"] += int(row.get("scraped_jobs", 0))
        per_company[company_id]["staged_jobs"] += int(row.get("staged_jobs", 0))

    for row in jobs_summary.get("company_metrics", []):
        company_id = row.get("company_id")
        if not company_id:
            continue
        per_company.setdefault(
            company_id,
            {
                "company_id": company_id,
                "company_name": id_to_name.get(company_id, f"company_{company_id}"),
                "scraped_jobs": 0,
                "staged_jobs": 0,
                "new_jobs": 0,
                "updated_jobs": 0,
                "unchanged_jobs": 0,
                "closed_jobs": 0,
                "extraction_attempted": 0,
                "salary_found": 0,
                "remote_policy_found": 0,
                "skills_found": 0,
            },
        )
        per_company[company_id]["new_jobs"] += int(row.get("inserted", 0))
        per_company[company_id]["updated_jobs"] += int(row.get("updated", 0))
        per_company[company_id]["unchanged_jobs"] += int(row.get("unchanged", 0))
        per_company[company_id]["closed_jobs"] += int(row.get("closed", 0))

    for row in extraction_summary.get("company_metrics", []):
        company_id = row.get("company_id")
        if not company_id:
            continue
        per_company.setdefault(
            company_id,
            {
                "company_id": company_id,
                "company_name": id_to_name.get(company_id, f"company_{company_id}"),
                "scraped_jobs": 0,
                "staged_jobs": 0,
                "new_jobs": 0,
                "updated_jobs": 0,
                "unchanged_jobs": 0,
                "closed_jobs": 0,
                "extraction_attempted": 0,
                "salary_found": 0,
                "remote_policy_found": 0,
                "skills_found": 0,
            },
        )
        per_company[company_id]["extraction_attempted"] += int(row.get("extraction_attempted", 0))
        per_company[company_id]["salary_found"] += int(row.get("salary_found", 0))
        per_company[company_id]["remote_policy_found"] += int(row.get("remote_policy_found", 0))
        per_company[company_id]["skills_found"] += int(row.get("skills_found", 0))

    company_rows_for_insert = []
    for metric in per_company.values():
        company_rows_for_insert.append(
            {
                "dag_id": dag_id,
                "run_id": run_id,
                "run_started_at": run_started_at,
                "company_id": metric["company_id"],
                "company_name": metric["company_name"],
                "scraped_jobs": metric["scraped_jobs"],
                "staged_jobs": metric["staged_jobs"],
                "new_jobs": metric["new_jobs"],
                "updated_jobs": metric["updated_jobs"],
                "unchanged_jobs": metric["unchanged_jobs"],
                "closed_jobs": metric["closed_jobs"],
                "extraction_attempted": metric["extraction_attempted"],
                "salary_found": metric["salary_found"],
                "remote_policy_found": metric["remote_policy_found"],
                "skills_found": metric["skills_found"],
            }
        )

    run_metrics = {
        "dag_id": dag_id,
        "run_id": run_id,
        "run_started_at": run_started_at,
        "run_finished_at": run_finished_at,
        "status": "success",
        "total_companies": len(per_company),
        "total_scraped": sum(row["scraped_jobs"] for row in company_rows_for_insert),
        "total_staged": sum(row["staged_jobs"] for row in company_rows_for_insert),
        "total_new": int(jobs_summary.get("inserted", 0)),
        "total_updated": int(jobs_summary.get("updated", 0)),
        "total_unchanged": int(jobs_summary.get("unchanged", 0)),
        "total_closed": int(jobs_summary.get("closed", 0)),
        "total_extracted": int(extraction_summary.get("processed", 0)),
        "salary_found": int(extraction_summary.get("salary_found", 0)),
        "remote_policy_found": int(extraction_summary.get("remote_policy_found", 0)),
        "skills_found": int(extraction_summary.get("skills_found", 0)),
    }

    upsert_run_monitoring(conn, cur, run_metrics, company_rows_for_insert)
    close_conn_cursor(conn, cur)

    output = {
        "dag_id": dag_id,
        "run_id": run_id,
        "total_companies": run_metrics["total_companies"],
        "totals": {
            "scraped": run_metrics["total_scraped"],
            "staged": run_metrics["total_staged"],
            "new": run_metrics["total_new"],
            "updated": run_metrics["total_updated"],
            "unchanged": run_metrics["total_unchanged"],
            "closed": run_metrics["total_closed"],
            "extracted": run_metrics["total_extracted"],
        },
        "sync_summary": sync_summary,
    }
    print(f"  Run metrics persisted: {output}")
    return output

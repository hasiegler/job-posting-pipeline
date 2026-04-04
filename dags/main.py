from airflow import DAG
from datetime import datetime, timedelta, timezone

from api.scrape_jobs import load_companies, scrape_greenhouse, save_results
from datawarehouse.sync_companies import sync_companies
from datawarehouse.dwh import (
    update_staging_jobs,
    update_jobs_table,
    snapshot_job_changes,
    extract_fields,
    clean_staging,
    refresh_analytics,
    finalize_run_metrics,
)
COMPANIES_FILE = "companies.yaml"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1, tzinfo=timezone.utc),
    "email_on_failure": False,
    "email_on_retry": False,
    #"retries": 1,
    #"retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id='company_json_scraper',
    default_args=default_args,
    description='DAG to produce json files for each company in companies.yaml',
    schedule='0 12 * * *',  # 12:00 PM UTC
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=1),
    is_paused_upon_creation=True,
) as dag:

    # Step 0: Sync companies.yaml to Supabase
    sync = sync_companies(COMPANIES_FILE)

    # Step 1: Scrape and save to S3
    greenhouse_companies = load_companies(COMPANIES_FILE, "greenhouse")
    company_results = scrape_greenhouse.expand(company=greenhouse_companies)
    s3_paths = save_results.expand(result=company_results)

    # Step 2: Load from S3 into staging_jobs
    staging = update_staging_jobs.expand(s3_path=s3_paths)

    # Step 3: Process staging into jobs table, mark closed jobs
    jobs = update_jobs_table()

    # Step 4: Extract structured fields from descriptions
    extraction = extract_fields()

    # Step 5: Snapshot changed jobs into history table
    history = snapshot_job_changes(jobs)

    # Step 6: Purge processed staging rows
    cleanup = clean_staging()

    # Step 7: Snapshot precomputed company analytics
    analytics = refresh_analytics()

    # Step 8: Persist per-run monitoring metrics
    finalize_metrics = finalize_run_metrics(
        sync_summary=sync,
        staging_summaries=staging,
        jobs_summary=jobs,
        extraction_summary=extraction,
        cleanup_summary=cleanup,
    )

    # Dependencies
    sync >> greenhouse_companies
    staging >> jobs >> extraction >> history
    history >> cleanup
    history >> analytics
    [cleanup, analytics] >> finalize_metrics
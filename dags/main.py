from airflow import DAG
from datetime import datetime, timedelta, timezone

from api.scrape_jobs import load_companies, scrape_greenhouse, save_results
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
) as dag:

    #Define tasks
    greenhouse_companies = load_companies(COMPANIES_FILE, "greenhouse")
    company_results = scrape_greenhouse.expand(company=greenhouse_companies)
    save_results_task = save_results.expand(result=company_results)
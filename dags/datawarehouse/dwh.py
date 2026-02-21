"""
Airflow task functions for loading data into Supabase tables.
"""

try:
    from airflow.decorators import task
except ImportError:
    def task(func):
        func.function = func
        return func

from datawarehouse.data_utils import get_conn_cursor, close_conn_cursor
from datawarehouse.data_loading import load_s3_json
from datawarehouse.data_modification import insert_staging_jobs


@task
def update_staging_jobs(s3_path: str) -> dict:
    """Load a JSON file from S3 and insert its jobs into the staging_jobs table."""
    data = load_s3_json(s3_path)

    conn, cur = get_conn_cursor()
    inserted = insert_staging_jobs(conn, cur, data)
    close_conn_cursor(conn, cur)

    company = data["company"]
    print(f"  Loaded {inserted} jobs into staging_jobs for {company}")
    return {"company": company, "inserted": inserted}

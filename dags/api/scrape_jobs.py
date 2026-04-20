"""
Scrapes job postings for each enabled company defined in companies.yaml.
Outputs results to a JSON file per company in the ./output/ directory.
"""

import json
import os
from datetime import datetime, timezone
from urllib.parse import urlparse

from bs4 import BeautifulSoup
import requests
import yaml

try:
    from airflow.decorators import task
except ImportError:
    def task(func):
        func.function = func
        return func

COMPANIES_FILE = "companies.yaml"



@task
def load_companies(path: str, scraper_type: str = None) -> list[dict]:
    """Load and return enabled companies from the YAML config, optionally filtered by scraper_type."""
    with open(path, "r") as f:
        data = yaml.safe_load(f)

    companies = data.get("companies", [])

    enabled = [c for c in companies if c.get("enabled", False)]

    if scraper_type:
        enabled = [c for c in enabled if c.get("scraper_type") == scraper_type]

    if not enabled:
        print("No enabled companies found in config.")
    return enabled


def extract_board_token(url: str) -> str:
    """Extract the board token (last path segment) from a Greenhouse URL.

    e.g. https://boards.greenhouse.io/anthropic -> 'anthropic'
    """
    path = urlparse(url).path.rstrip("/")
    return path.split("/")[-1]


@task
def scrape_greenhouse(company: dict) -> dict:
    """Scrape all jobs from a Greenhouse job board using their public JSON API."""
    board_token = extract_board_token(company["url"])
    api_url = f"https://boards-api.greenhouse.io/v1/boards/{board_token}/jobs"

    print(f"  Fetching job list from {api_url} ...")
    resp = requests.get(api_url, params={"content": "true"}, timeout=60)
    resp.raise_for_status()

    body = resp.json()
    raw_jobs = body.get("jobs", [])
    # Greenhouse's /jobs endpoint is unpaginated and returns meta.total. If the
    # response we got has fewer jobs than meta.total, treat it as a partial
    # response and fail loud — silently accepting it would mass-close the
    # missing jobs downstream and pollute job_history with phantom
    # close+reactivate cycles.
    meta_total = body.get("meta", {}).get("total")
    if meta_total is not None and meta_total != len(raw_jobs):
        raise RuntimeError(
            f"{company['name']}: Greenhouse reported meta.total={meta_total} "
            f"but returned {len(raw_jobs)} jobs — refusing partial response."
        )

    print(f"  Found {len(raw_jobs)} jobs (meta.total={meta_total}).")

    jobs = []
    for raw in raw_jobs:
        location = raw.get("location", {}).get("name", "Unknown")
        departments = [d["name"] for d in raw.get("departments", []) if "name" in d]
        offices = [o["name"] for o in raw.get("offices", []) if "name" in o]

        jobs.append(
            {
                "id": raw.get("id"),
                "title": raw.get("title"),
                "url": raw.get("absolute_url"),
                "location": location,
                "departments": departments,
                "offices": offices,
                "content_html": raw.get("content"),
                "content_text": BeautifulSoup(raw.get("content", ""), "html.parser").get_text(separator="\n", strip=True),
                "language": raw.get("language"),
                "first_published": raw.get("first_published"),
                "updated_at": raw.get("updated_at"),
            }
        )

    return {"company_name": company["name"], "jobs": jobs, "meta_total": meta_total}


@task
def save_results(result: dict) -> str:
    """Write scraped jobs to S3 as JSON. Path: company/year/month/day/filename.json"""
    import boto3

    company_name = result["company_name"]
    jobs = result["jobs"]

    now = datetime.now(timezone.utc)
    timestamp = now.strftime("%Y%m%d_%H%M%S")
    filename = f"{company_name}_{timestamp}.json"
    s3_key = f"{company_name}/{now.year}/{now.month:02d}/{now.day:02d}/{filename}"

    output = {
        "company": company_name,
        "scraped_at": now.isoformat(),
        "total_jobs": len(jobs),
        "meta_total": result.get("meta_total"),
        "jobs": jobs,
    }

    json_data = json.dumps(output, indent=2, ensure_ascii=False)

    bucket = os.environ["S3_BUCKET_NAME"]
    s3_client = boto3.client("s3")
    s3_client.put_object(
        Bucket=bucket,
        Key=s3_key,
        Body=json_data.encode("utf-8"),
        ContentType="application/json",
    )

    s3_path = f"s3://{bucket}/{s3_key}"
    print(f"  Saved {len(jobs)} jobs -> {s3_path}")
    return s3_path


if __name__ == "__main__":
    import time

    companies = load_companies.function(COMPANIES_FILE)

    if not companies:
        print("No enabled companies found.")
    else:
        for company in companies:
            print(f"\n[{company['name']}]")
            result = scrape_greenhouse.function(company)
            save_results.function(result)
            time.sleep(1)

        print("\nDone.")
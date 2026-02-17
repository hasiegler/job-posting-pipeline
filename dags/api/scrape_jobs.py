"""
Scrapes job postings for each enabled company defined in companies.yaml.
Outputs results to a JSON file per company in the ./output/ directory.
"""

import json
from datetime import datetime, timezone
from pathlib import Path
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
    resp = requests.get(api_url, params={"content": "true"}, timeout=30)
    resp.raise_for_status()

    raw_jobs = resp.json().get("jobs", [])
    print(f"  Found {len(raw_jobs)} jobs.")

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

    return {"company_name": company["name"], "jobs": jobs}


@task
def save_results(result: dict) -> str:
    """Write scraped jobs to a JSON file and return the file path."""
    company_name = result["company_name"]
    jobs = result["jobs"]

    output_dir = Path("/opt/airflow/output")
    output_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    file_path = output_dir / f"{company_name}_{timestamp}.json"

    output = {
        "company": company_name,
        "scraped_at": datetime.now(timezone.utc).isoformat(),
        "total_jobs": len(jobs),
        "jobs": jobs,
    }

    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    print(f"  Saved {len(jobs)} jobs -> {file_path}")
    return str(file_path)


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
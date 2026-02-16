"""
Scrapes job postings for each enabled company defined in companies.yaml.
Outputs results to a JSON file per company in the ./output/ directory.
"""

import json
import os
import sys
import time
from datetime import datetime, timezone
from urllib.parse import urlparse

from bs4 import BeautifulSoup
import requests
import yaml

COMPANIES_FILE = "companies.yaml"
OUTPUT_DIR = "output"


def load_companies(path: str) -> list[dict]:
    """Load and return enabled companies from the YAML config."""
    with open(path, "r") as f:
        data = yaml.safe_load(f)

    companies = data.get("companies", [])

    enabled = [c for c in companies if c.get("enabled", False)]

    if not enabled:
        print("No enabled companies found in config.")
    return enabled


def extract_board_token(url: str) -> str:
    """Extract the board token (last path segment) from a Greenhouse URL.

    e.g. https://boards.greenhouse.io/anthropic -> 'anthropic'
    """
    path = urlparse(url).path.rstrip("/")
    return path.split("/")[-1]


def scrape_greenhouse(company: dict) -> list[dict]:
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

    return jobs


# Map scraper_type values to their handler functions
SCRAPERS = {
    "greenhouse": scrape_greenhouse,
}


def save_results(company_name: str, jobs: list[dict]) -> str:
    """Write scraped jobs to a JSON file and return the file path."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    filepath = os.path.join(OUTPUT_DIR, f"{company_name}_{timestamp}.json")

    output = {
        "company": company_name,
        "scraped_at": datetime.now(timezone.utc).isoformat(),
        "total_jobs": len(jobs),
        "jobs": jobs,
    }

    with open(filepath, "w") as f:
        json.dump(output, f, indent=2)

    return filepath


if __name__ == "__main__":
    print(f"Loading companies from {COMPANIES_FILE} ...")
    companies = load_companies(COMPANIES_FILE)

    if not companies:
        sys.exit(1)

    for company in companies:
        name = company["name"]
        scraper_type = company.get("scraper_type")

        print(f"\n[{name}] scraper_type={scraper_type}")

        scraper_fn = SCRAPERS.get(scraper_type)
        if scraper_fn is None:
            print(f"  WARNING: Unknown scraper_type '{scraper_type}' — skipping.")
            continue

        try:
            jobs = scraper_fn(company)
            filepath = save_results(name, jobs)
            print(f"  Saved {len(jobs)} jobs -> {filepath}")
        except requests.RequestException as e:
            print(f"  ERROR scraping {name}: {e}")

        time.sleep(1)

    print("\nDone.")

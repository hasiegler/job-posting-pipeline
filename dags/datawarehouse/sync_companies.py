"""
Syncs the companies table in Supabase with the local companies.yaml config.
New companies are inserted, existing companies are updated,
and companies in the DB but not in YAML are logged but kept.
"""

import yaml

try:
    from airflow.decorators import task
except ImportError:
    def task(func):
        func.function = func
        return func

from psycopg2.extras import execute_values, execute_batch

from datawarehouse.data_utils import get_conn_cursor, close_conn_cursor

COMPANIES_FILE = "companies.yaml"


@task
def sync_companies(path: str = COMPANIES_FILE) -> dict:
    """Sync companies table with companies.yaml. Returns a summary of changes."""
    with open(path, "r") as f:
        data = yaml.safe_load(f)

    yaml_companies = data.get("companies", [])
    if not yaml_companies:
        print("No companies found in YAML.")
        return {"inserted": 0, "updated": 0, "warnings": 0}

    conn, cur = get_conn_cursor()

    cur.execute(
        "ALTER TABLE companies ADD COLUMN IF NOT EXISTS "
        "consecutive_zero_scrapes INTEGER NOT NULL DEFAULT 0"
    )
    cur.execute(
        "ALTER TABLE companies ADD COLUMN IF NOT EXISTS "
        "consecutive_permanent_failures INTEGER NOT NULL DEFAULT 0"
    )

    # Fetch all existing companies in a single query
    cur.execute(
        "SELECT company_name, scraper_type, base_url, enabled, canonical_name "
        "FROM companies"
    )
    existing = {row["company_name"]: row for row in cur.fetchall()}

    yaml_names = set()
    to_insert: list[tuple] = []
    to_update: list[tuple] = []

    for company in yaml_companies:
        company_name = company["name"]
        scraper_type = company.get("scraper_type", "")
        base_url = company.get("url", "")
        enabled = company.get("enabled", False)
        canonical_name = company_name.replace(f"_{scraper_type}", "")
        yaml_names.add(company_name)

        if company_name in existing:
            ex = existing[company_name]
            changed = (
                ex["scraper_type"] != scraper_type
                or ex["base_url"] != base_url
                or ex["enabled"] != enabled
                or ex["canonical_name"] != canonical_name
            )
            if changed:
                to_update.append(
                    (scraper_type, base_url, enabled, canonical_name, company_name)
                )
                print(f"  Updated: {company_name}")
            else:
                print(f"  No changes: {company_name}")
        else:
            to_insert.append(
                (company_name, scraper_type, base_url, enabled, canonical_name)
            )
            print(f"  Inserted: {company_name}")

    if to_insert:
        execute_values(cur, """
            INSERT INTO companies
                (company_name, scraper_type, base_url, enabled, canonical_name)
            VALUES %s
        """, to_insert)

    if to_update:
        execute_batch(cur, """
            UPDATE companies
            SET scraper_type = %s, base_url = %s, enabled = %s,
                canonical_name = %s, updated_at = NOW()
            WHERE company_name = %s
        """, to_update)

    # Companies in DB but removed from YAML — disable them rather than leaving
    # them enabled, which would cause stale scraping.
    orphaned = set(existing.keys()) - yaml_names
    to_disable = [
        name for name in orphaned
        if existing[name]["enabled"]
    ]
    if to_disable:
        execute_batch(cur, """
            UPDATE companies
            SET enabled = FALSE, updated_at = NOW()
            WHERE company_name = %s
        """, [(name,) for name in to_disable])
        for name in to_disable:
            print(f"  Disabled (removed from YAML): {name}")

    already_disabled = orphaned - set(to_disable)
    for name in already_disabled:
        print(f"  Already disabled (not in YAML): {name}")

    conn.commit()
    close_conn_cursor(conn, cur)

    inserted = len(to_insert)
    updated = len(to_update)
    disabled = len(to_disable)
    summary = {
        "inserted": inserted,
        "updated": updated,
        "disabled": disabled,
        "warnings": len(already_disabled),
    }
    print(
        f"\nSync complete: {inserted} inserted, {updated} updated, "
        f"{disabled} disabled, {len(already_disabled)} already-disabled orphans"
    )
    return summary


if __name__ == "__main__":
    import sys
    import os
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
    sync_companies.function()

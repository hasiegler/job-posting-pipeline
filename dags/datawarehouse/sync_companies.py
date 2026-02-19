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

    inserted = 0
    updated = 0

    yaml_names = set()

    for company in yaml_companies:
        company_name = company["name"]
        scraper_type = company.get("scraper_type", "")
        base_url = company.get("url", "")
        enabled = company.get("enabled", False)
        canonical_name = company_name.replace(f"_{scraper_type}", "")

        yaml_names.add(company_name)

        cur.execute(
            """SELECT company_id, scraper_type, base_url, enabled, canonical_name
               FROM companies WHERE company_name = %s""",
            (company_name,)
        )
        existing = cur.fetchone()

        if existing:
            changed = (
                existing["scraper_type"] != scraper_type
                or existing["base_url"] != base_url
                or existing["enabled"] != enabled
                or existing["canonical_name"] != canonical_name
            )
            if changed:
                cur.execute("""
                    UPDATE companies
                    SET scraper_type = %s,
                        base_url = %s,
                        enabled = %s,
                        canonical_name = %s,
                        updated_at = NOW()
                    WHERE company_name = %s
                """, (scraper_type, base_url, enabled, canonical_name, company_name))
                updated += 1
                print(f"  Updated: {company_name}")
            else:
                print(f"  No changes: {company_name}")
        else:
            cur.execute("""
                INSERT INTO companies (company_name, scraper_type, base_url, enabled, canonical_name)
                VALUES (%s, %s, %s, %s, %s)
            """, (company_name, scraper_type, base_url, enabled, canonical_name))
            inserted += 1
            print(f"  Inserted: {company_name}")

    # Check for companies in DB but not in YAML
    cur.execute("SELECT company_name FROM companies")
    db_companies = {row["company_name"] for row in cur.fetchall()}
    orphaned = db_companies - yaml_names
    for name in orphaned:
        print(f"  WARNING: '{name}' exists in DB but not in companies.yaml")

    conn.commit()
    close_conn_cursor(conn, cur)

    summary = {"inserted": inserted, "updated": updated, "warnings": len(orphaned)}
    print(f"\nSync complete: {inserted} inserted, {updated} updated, {len(orphaned)} warnings")
    return summary


if __name__ == "__main__":
    import sys
    import os
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
    sync_companies.function()

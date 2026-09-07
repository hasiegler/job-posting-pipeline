"""
readme_stats.py
---------------

Regenerate the "Volume" table in the top-level README.

The README's Scale section is a dated snapshot, not a live counter. Rather than
hand-editing six numbers whenever it drifts, run this and paste the markdown it
prints over the existing table:

    set -a && source .env && set +a
    python tools/analysis/readme_stats.py

Read-only, like everything else in this folder — the connection is opened with
`default_transaction_read_only = on` (see db.py), so a mistake here can't write.

NOT executed by run_all.py; this is a maintenance helper, not an analysis query.
"""

from __future__ import annotations

import datetime as dt
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from db import readonly_cursor

# Postgres has no round(double precision, int), and PERCENTILE_CONT returns
# double precision — hence the ::numeric casts on every rounded expression.
QUERIES: dict[str, str] = {
    "jobs": """
        SELECT COUNT(*) AS total, COUNT(*) FILTER (WHERE is_active) AS active
        FROM jobs
    """,
    "history": """
        SELECT COUNT(*) AS events,
               MAX(recorded_at)::date - MIN(recorded_at)::date AS days,
               COUNT(*) FILTER (WHERE change_type = 'inserted') AS opens,
               COUNT(*) FILTER (WHERE change_type = 'closed')   AS closes
        FROM job_history
    """,
    "throughput": """
        SELECT ROUND(AVG(total_scraped)::numeric) AS scraped,
               ROUND(AVG(total_new)::numeric)     AS opened,
               ROUND(AVG(total_closed)::numeric)  AS closed,
               ROUND(AVG(salary_coverage_pct)::numeric) AS salary_cov,
               ROUND(AVG(remote_coverage_pct)::numeric) AS remote_cov,
               ROUND(AVG(skills_coverage_pct)::numeric) AS skills_cov
        FROM (SELECT * FROM pipeline_runs ORDER BY run_started_at DESC LIMIT 7) recent
    """,
    "runs": """
        SELECT COUNT(*) AS runs,
               ROUND((PERCENTILE_CONT(0.5) WITHIN GROUP (
                   ORDER BY EXTRACT(EPOCH FROM (run_finished_at - run_started_at))
               ))::numeric / 60) AS median_min,
               ROUND(MAX(EXTRACT(EPOCH FROM (run_finished_at - run_started_at)))::numeric / 60) AS max_min
        FROM pipeline_runs
        WHERE run_finished_at IS NOT NULL
    """,
    "marts": """
        SELECT (SELECT COUNT(*) FROM company_stats)       AS stats_rows,
               (SELECT COUNT(*) FROM company_skills)      AS skill_rows,
               (SELECT COUNT(*) FROM company_departments) AS dept_rows
    """,
}


def main() -> int:
    today = dt.date.today().isoformat()
    with readonly_cursor() as cur:
        r = {}
        for name, sql in QUERIES.items():
            cur.execute(sql)
            r[name] = cur.fetchone()

    j, h, t, runs, m = r["jobs"], r["history"], r["throughput"], r["runs"], r["marts"]

    print(f"\nPaste over the Volume table in README.md (and update the date to {today}):\n")
    print(f"| Volume (as of {today}) | |")
    print("|---|---|")
    print(f"| Postings tracked | {j['total']:,} total, {j['active']:,} currently active |")
    print(
        f"| Change history | {h['events']:,} events over {h['days']} consecutive days "
        f"— {h['opens']:,} opens, {h['closes']:,} closes |"
    )
    print(
        f"| Daily throughput | ~{t['scraped']:,} postings re-checked per run; "
        f"~{t['opened']:,} opened, ~{t['closed']:,} closed |"
    )
    print(
        f"| Run duration | {runs['median_min']} min median, {runs['max_min']} min worst, "
        f"over {runs['runs']} runs — against a 6-hour timeout |"
    )
    print(
        f"| Extraction coverage | {t['salary_cov']}% salary, {t['remote_cov']}% remote policy, "
        f"{t['skills_cov']}% skills, of active postings |"
    )
    print(
        f"| Mart size | {m['stats_rows']:,} company-day snapshots, "
        f"{m['skill_rows']:,} skill rows, {m['dept_rows']:,} department rows |"
    )
    print()
    return 0


if __name__ == "__main__":
    sys.exit(main())

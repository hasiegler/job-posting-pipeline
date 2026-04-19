"""
scratch.py
----------

Disposable one-off queries — a lightweight alternative to typing into
`psql`. Edit freely, commit if you find a useful angle worth keeping (and
then promote it to its own dedicated script under analysis/).

NOT executed by run_all.py.

The connection is read-only at the server level (see analysis/db.py),
so any accidental write will be rejected by Postgres.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from db import print_table, readonly_cursor


def main() -> int:
    with readonly_cursor() as cur:
        cur.execute(
            """
            SELECT
                (SELECT COUNT(*) FROM companies)                       AS companies,
                (SELECT COUNT(*) FROM jobs)                            AS jobs,
                (SELECT COUNT(*) FROM jobs WHERE is_active)            AS active_jobs,
                (SELECT COUNT(*) FROM job_history)                     AS history_events,
                (SELECT MIN(recorded_at)::DATE FROM job_history)       AS history_start,
                (SELECT MAX(snapshot_date)     FROM company_stats)     AS latest_snapshot
            """
        )
        rows = cur.fetchall()

    print("Quick database snapshot:")
    print_table(rows, [
        "companies", "jobs", "active_jobs", "history_events",
        "history_start", "latest_snapshot",
    ])

    return 0


if __name__ == "__main__":
    sys.exit(main())

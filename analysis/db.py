"""
Connection helper + small I/O utilities for ad-hoc analysis scripts.

Mirrors the Supabase env-var pattern used by dags/datawarehouse/data_utils.py
(SUPABASE_HOST / SUPABASE_PORT / SUPABASE_DB / SUPABASE_USER / SUPABASE_PASSWORD)
but adds two safety guarantees that the production pipeline does NOT need:

  1. The session is forced into read-only mode at the server side
     (`SET default_transaction_read_only = on`), so a stray INSERT/UPDATE/
     DELETE/DDL statement issued by an analysis script will be rejected
     by Postgres itself.
  2. The connection's `autocommit` is left off so every implicit transaction
     starts inside the read-only default and rolls back on close.

Run scripts directly from your shell after `source venv/bin/activate &&
set -a && source .env && set +a` (same pattern as the pipeline scripts).
A `.env` file in the repo root is also loaded automatically via python-dotenv
so the scripts work without remembering to `source` first.
"""

from __future__ import annotations

import csv
import datetime as _dt
import os
import sys
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterable, Iterator, Sequence

import psycopg2
from psycopg2.extras import RealDictCursor

try:
    from dotenv import load_dotenv

    _REPO_ROOT = Path(__file__).resolve().parent.parent
    load_dotenv(_REPO_ROOT / ".env", override=False)
except ImportError:
    pass


RESULTS_ROOT = Path(__file__).resolve().parent / "results"
_RUN_DATE_ENV = "JOBPULSE_ANALYSIS_DATE"


def run_date() -> str:
    """Date string used for the per-run results subfolder.

    Reads `JOBPULSE_ANALYSIS_DATE` if set (used by `run_all.py` to keep the
    whole batch in one folder), otherwise falls back to today's date in
    YYYY-MM-DD format.
    """
    override = os.getenv(_RUN_DATE_ENV)
    if override:
        return override
    return _dt.date.today().isoformat()


def results_dir() -> Path:
    """Per-run results directory, e.g. analysis/results/2026-04-18/."""
    return RESULTS_ROOT / run_date()


def _require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        print(
            f"[analysis] Missing required env var {name}. "
            "Source the repo .env file or export it manually.",
            file=sys.stderr,
        )
        sys.exit(2)
    return value


@contextmanager
def readonly_cursor() -> Iterator[Any]:
    """Yield a RealDictCursor on a read-only Supabase connection.

    Usage:
        with readonly_cursor() as cur:
            cur.execute("SELECT 1")
            rows = cur.fetchall()
    """
    conn = psycopg2.connect(
        host=_require_env("SUPABASE_HOST"),
        port=int(os.getenv("SUPABASE_PORT", "5432")),
        dbname=os.getenv("SUPABASE_DB", "postgres"),
        user=_require_env("SUPABASE_USER"),
        password=_require_env("SUPABASE_PASSWORD"),
        sslmode="require",
        cursor_factory=RealDictCursor,
        application_name="jobpulse-analysis",
    )
    try:
        cur = conn.cursor()
        cur.execute("SET default_transaction_read_only = on")
        cur.execute("SET statement_timeout = '120s'")
        yield cur
    finally:
        try:
            conn.rollback()
        except Exception:
            pass
        conn.close()


def pipeline_history_age_days(cur) -> int | None:
    """How many days of `job_history` events we've actually observed.

    Several analyses (ghost detection, churn, trend comparisons) are only
    valid once the pipeline has been collecting events for at least the
    lookback window they claim to measure. Anchoring to this single
    definition keeps every script's gate consistent.
    """
    cur.execute("SELECT MIN(recorded_at)::DATE AS first_event FROM job_history")
    row = cur.fetchone()
    if not row or row["first_event"] is None:
        return None
    cur.execute(
        "SELECT (CURRENT_DATE - %s::DATE)::INT AS age_days",
        (row["first_event"],),
    )
    return cur.fetchone()["age_days"]


def ensure_results_dir() -> Path:
    """Create (and return) the per-run results directory."""
    d = results_dir()
    d.mkdir(parents=True, exist_ok=True)
    return d


def write_csv(query_name: str, rows: Sequence[dict], fieldnames: Sequence[str] | None = None) -> Path:
    """Write rows (list of dicts) to results/<date>/<query_name>.csv. Always
    writes a header even when rows are empty so downstream tooling sees the schema."""
    out_dir = ensure_results_dir()
    path = out_dir / f"{query_name}.csv"
    if not rows:
        with path.open("w", newline="") as f:
            if fieldnames:
                csv.writer(f).writerow(fieldnames)
        return path

    if fieldnames is None:
        fieldnames = list(rows[0].keys())

    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({k: _csv_safe(row.get(k)) for k in fieldnames})
    return path


def _csv_safe(value: Any) -> Any:
    if value is None:
        return ""
    if isinstance(value, list):
        return "|".join(str(v) for v in value)
    return value


def write_md(
    query_name: str,
    title: str,
    bullets: Iterable[str],
    tables: Sequence[dict] | None = None,
    extra: Sequence[str] | None = None,
) -> Path:
    """Write a results/<date>/<query_name>.md with headline findings.

    Args:
        query_name: file stem (no extension).
        title:      H1 title.
        bullets:    headline bullets, rendered as a `- ...` list.
        tables:     optional list of {"caption": str, "headers": [str],
                    "rows": [[scalar, ...]]} dicts, each rendered as a
                    `### caption` heading + GitHub markdown table.
        extra:      optional list of raw markdown strings appended at the
                    bottom of the file (e.g. callouts, plain paragraphs).

    The summary builder in `run_all.py` includes the entire body below the
    H1, so any tables/extras here flow through into `summary.md` as well.
    """
    out_dir = ensure_results_dir()
    path = out_dir / f"{query_name}.md"

    lines = [f"# {title}", "", f"_Source: `analysis/{query_name}.py`_", ""]

    bullets = list(bullets)
    if not bullets:
        lines.append("- (No findings — query returned no rows.)")
    else:
        lines.append("### Headline")
        lines.append("")
        for b in bullets:
            lines.append(f"- {b}")
    lines.append("")

    for tbl in tables or []:
        caption = tbl.get("caption") or ""
        headers = list(tbl.get("headers") or [])
        rows = list(tbl.get("rows") or [])
        if not headers:
            continue
        if caption:
            lines.append(f"### {caption}")
            lines.append("")
        lines.append("| " + " | ".join(headers) + " |")
        lines.append("| " + " | ".join("---" for _ in headers) + " |")
        if not rows:
            lines.append("| " + " | ".join("_(no rows)_" for _ in headers) + " |")
        else:
            for row in rows:
                cells = [_md_cell(c) for c in row]
                lines.append("| " + " | ".join(cells) + " |")
        lines.append("")

    for block in extra or []:
        lines.append(block.rstrip())
        lines.append("")

    path.write_text("\n".join(lines))
    return path


def _md_cell(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, list):
        value = ", ".join(str(v) for v in value)
    s = str(value)
    return s.replace("|", r"\|").replace("\n", " ")


def fmt_int(value: Any) -> str:
    if value is None:
        return "n/a"
    try:
        return f"{int(value):,}"
    except (TypeError, ValueError):
        return str(value)


def fmt_pct(value: Any, digits: int = 1) -> str:
    if value is None:
        return "n/a"
    try:
        return f"{float(value):.{digits}f}%"
    except (TypeError, ValueError):
        return str(value)


def fmt_money(value: Any) -> str:
    if value is None:
        return "n/a"
    try:
        return f"${float(value):,.0f}"
    except (TypeError, ValueError):
        return str(value)


def print_table(rows: Sequence[dict], columns: Sequence[str], limit: int = 10) -> None:
    """Print a small fixed-width table to stdout for human reading."""
    if not rows:
        print("  (no rows)")
        return
    rows = list(rows)[:limit]
    widths = {c: max(len(c), max((len(str(r.get(c, ""))) for r in rows), default=0)) for c in columns}
    header = "  " + " | ".join(c.ljust(widths[c]) for c in columns)
    sep = "  " + "-+-".join("-" * widths[c] for c in columns)
    print(header)
    print(sep)
    for r in rows:
        print("  " + " | ".join(str(r.get(c, "")).ljust(widths[c]) for c in columns))

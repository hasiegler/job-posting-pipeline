# tools/ — local-only helpers, **not** part of the pipeline

> **Nothing in this folder is production code.** The Airflow DAG under `dags/`
> is the entire pipeline. No module here is imported by it, scheduled by it, or
> bundled into the Docker image — `tools/` is the first exclusion in
> `.dockerignore`. Every script is run by hand from a laptop.
>
> Read this folder as the workbench around the pipeline, not the pipeline
> itself: `validators/` vets prospective job boards *before* they are added to
> `companies.yaml`, and `analysis/` explores data the pipeline has *already*
> collected (over a strictly read-only database connection). Neither writes to
> the warehouse or changes its schema.

---

## tools/validators/

Scripts for verifying new company URLs before adding them to `companies.yaml`.
Run from the **project root** so the default `--yaml companies.yaml` path resolves.

| Script | Purpose |
|--------|---------|
| `verify_greenhouse_slugs.py` | Hits the Greenhouse public API to confirm each candidate slug returns a valid job board. Writes a ready-to-paste YAML snippet via `--out`. |
| `verify_ashby_tokens.py` | Hits the Ashby posting API to confirm each candidate board token is live and has at least one active posting. |

**Quick start:**

```bash
# Greenhouse
python tools/validators/verify_greenhouse_slugs.py
python tools/validators/verify_greenhouse_slugs.py --out tools/validators/new_companies.yaml

# Ashby
python tools/validators/verify_ashby_tokens.py
python tools/validators/verify_ashby_tokens.py --out tools/validators/new_ashby_companies.yaml
```

Each run writes a YAML snippet (`new_companies.yaml`, `new_ashby_companies.yaml`)
next to the script, to be eyeballed before its entries are merged into
`companies.yaml`. Those two files are generated output and are gitignored —
`companies.yaml` is the committed source of truth for which boards are scraped.

---

## tools/analysis/

Generates the daily summary and CSV outputs reviewed manually after each pipeline run.

| Entry point | What it does |
|------------|-------------|
| `run_all.py` | Runs every query script in sequence, writes `results/<date>/summary.md` and per-query CSVs, appends dated snapshots to `results/history/`. |
| `db.py` | Shared connection helper — opens a **read-only** Postgres session using the same `SUPABASE_*` env vars as the pipeline. Import this in any ad-hoc query script. |

**Quick start** (from the project root):

```bash
source venv/bin/activate
set -a && source .env && set +a
python tools/analysis/run_all.py
```

Results land in `tools/analysis/results/<today>/`:
- `summary.md` — all query findings concatenated, ready to paste into Claude
- `*.csv` — raw data per query
- `run_log.txt` — timestamps and any failure tracebacks

The `results/history/` subfolder holds dated per-run CSV snapshots used for
longitudinal diffs. It is gitignored and accumulates locally across runs.

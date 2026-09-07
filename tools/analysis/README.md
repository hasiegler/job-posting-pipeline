# tools/analysis/

Ad-hoc, exploratory queries against the **JobPulse** Supabase database.

This folder is a research notebook in script form, used to dig up story angles
for marketing posts (ghost jobs, hiring trends, salary outliers, etc.).

> **Not production code.** The Airflow pipeline under `dags/` is the source of
> truth for everything ingested into Supabase. Nothing in this folder writes
> to the database, mutates schema, or runs on a schedule. Connections are
> opened with `SET default_transaction_read_only = on` so any accidental
> write would be rejected by Postgres itself.

## Layout

```
tools/analysis/
├── README.md                         # this file
├── db.py                             # connection helper + CSV/MD writers
├── requirements.txt                  # extra deps for analysis only
├── run_all.py                        # run every query, build summary.md
├── readme_stats.py                   # regenerate the root README's Volume table
├── ghost_jobs_by_inactivity.py
├── job_age_distribution.py
├── hiring_velocity_leaders.py
├── freshest_vs_stalest_companies.py
├── posting_cadence_over_time.py
├── skills_demand_signals.py
├── salary_outliers.py
├── remote_hybrid_onsite_split.py
├── close_reactivation_patterns.py
└── results/                          # gitignored — one subfolder per run-date
    ├── YYYY-MM-DD/
    │   ├── <query_name>.csv
    │   ├── <query_name>.md
    │   ├── summary.md                # written by run_all.py
    │   └── run_log.txt               # written by run_all.py
    └── history/                      # gitignored — additive longitudinal archive
        ├── company_metrics_YYYY-MM-DD.csv
        └── overall_metrics_YYYY-MM-DD.csv
```

`results/history/` accumulates one CSV pair per `run_all.py` invocation
and **never overwrites prior days**. This is the dataset the eventual
"April week 1 vs April week 4" trajectory analysis will read from. The
two files are written in addition to (not instead of) the dated
`summary.md`, so day-to-day workflows are unchanged.

Results are organized by date so re-running on a different day doesn't clobber
yesterday's findings. The folder name is today's date (`YYYY-MM-DD`) by default;
override with the `JOBPULSE_ANALYSIS_DATE` env var if you want to back-date a
batch (e.g. `JOBPULSE_ANALYSIS_DATE=2026-04-01 python tools/analysis/run_all.py`).
A single `run_all.py` invocation pins one date for the whole batch, so every
per-query script in that run lands in the same dated folder.

## Setup

The scripts reuse the repo's existing Supabase env-var pattern
(`SUPABASE_HOST`, `SUPABASE_PORT`, `SUPABASE_DB`, `SUPABASE_USER`,
`SUPABASE_PASSWORD`) — same vars `dags/datawarehouse/data_utils.py` reads.
A `.env` at the repo root is auto-loaded via `python-dotenv`, so you don't
need to `source` it manually.

```bash
# from the repo root
python -m venv venv && source venv/bin/activate
pip install -r tools/analysis/requirements.txt
```

If you already have the pipeline `requirements.txt` installed, you have
everything you need (psycopg2-binary + python-dotenv are already there).

## Running

### One query at a time

```bash
python tools/analysis/ghost_jobs_by_inactivity.py
```

Each script:

1. Prints a human-readable summary to stdout (headline numbers, top 10 rows).
2. Writes the full result set to `tools/analysis/results/<YYYY-MM-DD>/<query_name>.csv`.
3. Writes a short `tools/analysis/results/<YYYY-MM-DD>/<query_name>.md` with 3–8
   plain-English bullet points — the kind of findings worth quoting in a
   marketing post.

### Everything at once

```bash
python tools/analysis/run_all.py
```

This runs every analysis script in sequence into a single dated folder
(`tools/analysis/results/YYYY-MM-DD/`) and then writes:

- `summary.md` — every per-query markdown summary concatenated under clear
  headers, ready to paste into a Claude conversation.
- `run_log.txt` — start/finish timestamps and any failures (so a broken
  script never gets silently skipped).

## Workflow with Claude

1. Run `python tools/analysis/run_all.py`.
2. Open `tools/analysis/results/<today>/summary.md` and skim the bullets.
3. Drag interesting CSVs (or the whole dated folder) into a Claude chat
   for deeper analysis and angle-finding.

## Conventions

- **Job age is anchored to `jobs.first_published_at`** — the ATS-reported
  post date, i.e. the date a candidate would actually see on the listing.
  We never use `MIN(job_history.recorded_at)` ("when the pipeline first saw
  the job") as an age proxy, because that is bounded by the pipeline's
  lifetime and would silently cap every job's age at a few weeks.
  Jobs without a `first_published_at` are excluded from age math and
  counted separately so the headline numbers can't be skewed.
- **Salary aggregates are USD / yearly only**, matching how `company_stats`
  is populated by the pipeline.
- **Active-only by default.** Most analyses scope to `is_active = TRUE`
  unless the question is specifically about churn or closes.
- **Boilerplate skill suppression.** A skill mentioned in (almost) every
  active posting at a single company is almost always sitting in the
  "About \<Company\>" intro, not a real per-role requirement (e.g. MLflow
  fired on every Databricks JD). `skills_demand_signals.py` excludes those
  `(skill, company)` pairs from the top, trending, and concentration
  tables. The threshold is `>20 mentions AND ≥90% of that company's
  active jobs` and **must stay in sync with**
  `BOILERPLATE_SKILL_MIN_MENTIONS` / `BOILERPLATE_SKILL_MIN_PERCENTAGE`
  in `dags/datawarehouse/data_modification.py`, which applies the same
  rule when building the production `company_skills` snapshot. If you
  change one, change the other — otherwise the analysis output and the
  frontend will disagree. The self-vendor audit table is intentionally
  left raw so the diagnostic can still surface bias.

## Caveats (data maturity)

These all degrade gracefully — each script gates on the relevant data and
explains skips in its `.md` summary rather than crashing or silently
reporting misleading numbers.

- **Pipeline history age limits some signals.** Until `job_history` has
  been collecting for at least the lookback window:
  - `ghost_jobs_by_inactivity` skips its 60d / 90d windows.
  - `close_reactivation_patterns` flags reactivation counts as
    under-reported (the close→reopen cycle takes time to observe).
  - `skills_demand_signals` skips the 30d trend section. The prior
    31–120d window can only contain jobs that were *still open* when
    scraping started, which biases evergreen skills (Python, R, etc.)
    upward in that window and makes them look spuriously "declining"
    in the recent window. The trend gate requires ≥120 days of pipeline
    history before the comparison is honest.
  - `hiring_velocity_leaders` adds a caveat bullet because jobs posted
    in the last 30 days that closed before scraping started won't be
    counted, so the 30d count is a lower bound when history is young.
    The new **net change** tables (posts − closes) and **Δ active jobs**
    columns are surfaced regardless, but each section carries a banner
    declaring "this window is N days but the pipeline only has M days
    of history — treat as a floor estimate" whenever M < N. There's
    also a sanity-check footnote that flags companies where
    `(posted − closed) ≠ Δactive` by both >10 jobs and >10%; a
    persistent gap usually means closures are being missed by the
    closure-detection logic.
  - `posting_cadence_over_time` is intentionally early-stage: it builds
    the per-company weekly-cadence tables now so that the longitudinal
    archive (see below) accumulates a real series. Its banner makes the
    "don't quote me yet" posture explicit.

### Longitudinal archive (`results/history/`)

`run_all.py` writes two additional CSVs every run that **never
overwrite**, only accumulate:

- `company_metrics_<date>.csv` — one row per active company with
  `active_jobs`, `posted_7d`, `posted_30d`, `closed_7d`, `closed_30d`,
  `net_7d`, `net_30d`, `median_job_age_days`, `top_3_skills`.
- `overall_metrics_<date>.csv` — single-row dataset-wide snapshot of the
  same metrics plus `pipeline_history_age_days`.

This is the foundation for cross-snapshot diffs. With ~13 days of data
today the diffs are mostly noise; once the pipeline crosses 30–60 days
of `job_history`, you can `pandas.concat` everything in `history/` and
compute real week-over-week / month-over-month trajectory.
- **Extraction coverage.** A non-trivial fraction of active jobs may have
  no extracted `remote_policy` or `salary_*`. Both relevant scripts
  surface that fraction explicitly so you don't quote a percentage built
  on partial data.

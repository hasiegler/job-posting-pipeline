# jobpulse dbt project

This project owns the **analytics marts** for the job-posting pipeline. dbt reads
the raw tables produced by the Airflow DAG (`jobs`, `companies`,
`company_run_metrics`) and builds the daily snapshot tables the frontend/tools
read from: `company_stats`, `company_skills`, `company_departments`.

It also holds the **data-quality tests** that used to live as ad-hoc Python
checks in `dags/datawarehouse/quality_checks.py`.

---

## Where things run (targets)

Connection config is in `profiles.yml`. Credentials are **not** stored there —
they come from the same env vars the Python pipeline uses (`SUPABASE_HOST`,
`SUPABASE_USER`, `SUPABASE_PASSWORD`, `SUPABASE_DB`, `SUPABASE_PORT`), so the file
is safe to commit.

| Target | Schema     | When it's used                                                                 |
|--------|------------|--------------------------------------------------------------------------------|
| `prod` | `public`   | **Default.** The real tables, read by the app and analysis scripts. Airflow always runs `--target prod`. A plain `dbt run` also writes here. |
| `dev`  | `dbt_dev`  | Optional personal sandbox. Only used when you explicitly pass `--target dev`. Safe to drop anytime. |

Both targets point at the **same Supabase database** — they only differ by
schema. Use `dev` when you want to test a model change without touching `public`.

---

## How to run / test

From this directory, with the pipeline env vars loaded (e.g. `set -a && . ../.env && set +a`):

```bash
# Build all marts into public (what Airflow does)
dbt run --target prod

# Try a change safely in the dbt_dev schema first
dbt run --target dev

# Run all data-quality tests (warn-only; see below)
dbt test --target prod

# Build/test a single model
dbt run  --select company_skills --target dev
dbt test --select company_skills --target dev

# Docs + lineage graph
dbt docs generate && dbt docs serve
```

In production this is orchestrated by Airflow, **not** by hand. The DAG calls
`dags/datawarehouse/dbt_runner.py`, which shells out to the dbt binary in the
isolated venv (`/opt/airflow/dbt_venv/bin/dbt`) against `--target prod`:

- `refresh_analytics` task -> `dbt run` (builds the marts)
- `run_dbt_tests` task -> `dbt test`, and feeds any warnings into the single
  Telegram QC summary produced by `run_quality_checks`.

---

## ⚠️ NEVER run `--full-refresh` in prod

`company_stats`, `company_skills`, and `company_departments` are
`incremental` models (`delete+insert`) that accumulate **one snapshot per day**.
The daily history lives **only** in these tables — there is no other copy.

`dbt run --full-refresh` DROPs and rebuilds an incremental model, which would
**destroy every historical snapshot**. Only ever full-refresh in the `dbt_dev`
sandbox.

---

## Models (`models/marts/`)

All three are daily snapshots keyed by `snapshot_date` (pinned to UTC via
`(now() at time zone 'utc')::date`) and only include **enabled** companies.

| Model                 | Grain                                   | Notes |
|-----------------------|-----------------------------------------|-------|
| `company_stats`       | one row per company per day             | active/posted/closed counts, remote-policy split, salary avg/median |
| `company_skills`      | one row per company × skill per day     | `mention_count`, `percentage`, salary avg/median; boilerplate skills suppressed via `boilerplate_skill_*` vars |
| `company_departments` | one row per company × department per day| `active_job_count`, `percentage`, salary avg/median |

### `salary_sample_size`

Each mart has a `salary_sample_size` column = **how many jobs actually carried a
usable USD/yearly salary** (min or max present) — the real N behind the
avg/median figures. This is deliberately different from `mention_count` /
`active_job_count`, which count *all* matching jobs regardless of whether they
posted pay. The frontend should show this next to the salary range (e.g.
`median $320K–$405K · 113 jobs`) so the pay figure isn't mistaken for being based
on every job.

History predating this column is `NULL` (couldn't be backfilled); it populates
going forward on every run.

`sources.yml` declares the raw tables dbt reads and carries a few source-level
tests (e.g. `accepted_values` on `remote_policy` / `salary_period`).

---

## Project variables (`dbt_project.yml`)

Overridable at runtime with `--vars '{name: value}'`:

| Var | Purpose |
|-----|---------|
| `boilerplate_skill_min_mentions` (20) | with the % below, suppresses boilerplate skills in `company_skills` |
| `boilerplate_skill_min_percentage` (90.0) | ↑ |
| `null_rate_threshold` (0.01) | warn if >1% of active jobs are null/empty on a never-null field |
| `salary_min_plausible` (1000) | yearly-salary plausibility floor |
| `salary_max_plausible` (10000000) | yearly-salary plausibility ceiling |
| `min_description_length` (50) | active jobs shorter than this look like parse failures |
| `active_jobs_regression_factor` (0.6) | warn if today's active jobs < 60% of trailing average |
| `baseline_snapshots` (7) | trailing snapshot window for the regression check |

---

## Where each QC check lives (`tests/`)

All tests default to **`+severity: warn`** (`dbt_project.yml`) — they surface
problems without failing the DAG, matching the Python QC philosophy. Warnings are
routed into the Telegram summary by `run_dbt_tests`.

| Test file | What it catches |
|-----------|-----------------|
| `jobs_no_duplicate_source_job_id.sql` | duplicate `(company_id, source_job_id)` in `jobs` |
| `jobs_salary_plausible.sql` | yearly salaries outside `salary_min_plausible`..`salary_max_plausible` |
| `jobs_never_null_fields.sql` | null/empty rate above `null_rate_threshold` on fields that should never be null |
| `jobs_active_closed_consistent.sql` | a job can't be both active and have a `date_closed` |
| `jobs_timestamps_ordered.sql` | `first_published_at` / `date_closed` ordering sanity |
| `jobs_first_published_not_future.sql` | `first_published_at` in the future |
| `jobs_salary_currency_period_complete.sql` | salary present but currency/period missing |
| `jobs_active_rows_extracted.sql` | active jobs missing extracted fields (skills/departments) |
| `jobs_description_min_length.sql` | active jobs with descriptions shorter than `min_description_length` |
| `company_stats_snapshot_complete.sql` | every enabled company got today's `company_stats` snapshot |
| `total_active_jobs_not_regressed.sql` | today's total active jobs dropped below `active_jobs_regression_factor` × trailing avg |

Per-company regression alerting (e.g. a company dropping to 0 postings) still
lives in `dags/datawarehouse/quality_checks.py`, since it needs the run-time
baseline logic; dbt owns the absolute/consistency checks above.

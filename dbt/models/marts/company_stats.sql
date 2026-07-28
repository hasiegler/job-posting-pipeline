-- Daily per-company KPI snapshot.
--
-- Ported from refresh_company_analytics() in
-- dags/datawarehouse/data_modification.py (the company_stats INSERT).
--
-- NOTE ON DISABLED COMPANIES: the Python version ran a preceding DELETE of
-- today's rows for companies that became disabled, because it used a plain
-- upsert with no delete. The incremental delete+insert strategy only deletes
-- keys present in this run's batch (enabled companies), so a company disabled
-- *after* a same-day snapshot could keep one stale row until the date rolls
-- over. With a once-daily schedule this is effectively a non-issue; flag it if
-- intra-day disabling ever matters.
--
-- WARNING: never run `dbt run --full-refresh` against this model in prod — it
-- DROPs and rebuilds the table, destroying every historical snapshot.
{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['company_id', 'snapshot_date']
  )
}}

with first_run as (
    -- Each company's first pipeline run date. The 7d/30d closed & net-change
    -- columns stay NULL until enough history exists to compute them honestly.
    select
        company_id,
        min(run_started_at)::date as first_run_date
    from {{ source('jobpulse', 'company_run_metrics') }}
    group by company_id
),

snap as (
    -- Snapshot date pinned to UTC (see company_departments for rationale).
    select (now() at time zone 'utc')::date as d
)

select
    j.company_id,
    snap.d as snapshot_date,
    count(*) filter (where j.is_active) as active_jobs,
    count(*) filter (where j.first_published_at >= snap.d - interval '7 days') as posted_7d,
    count(*) filter (where j.first_published_at >= snap.d - interval '30 days') as posted_30d,
    case when fr.first_run_date <= snap.d - interval '7 days'
         then count(*) filter (where j.date_closed >= snap.d - interval '7 days')
    end as closed_7d,
    case when fr.first_run_date <= snap.d - interval '30 days'
         then count(*) filter (where j.date_closed >= snap.d - interval '30 days')
    end as closed_30d,
    case when fr.first_run_date <= snap.d - interval '7 days'
         then count(*) filter (where j.first_published_at >= snap.d - interval '7 days')
            - count(*) filter (where j.date_closed >= snap.d - interval '7 days')
    end as net_change_7d,
    case when fr.first_run_date <= snap.d - interval '30 days'
         then count(*) filter (where j.first_published_at >= snap.d - interval '30 days')
            - count(*) filter (where j.date_closed >= snap.d - interval '30 days')
    end as net_change_30d,
    count(*) filter (where j.is_active and j.remote_policy = 'Remote') as remote_count,
    count(*) filter (where j.is_active and j.remote_policy = 'Hybrid') as hybrid_count,
    count(*) filter (where j.is_active and j.remote_policy = 'On-Site') as onsite_count,
    -- How many active jobs actually carried a usable USD/yearly salary — i.e.
    -- the sample size behind the avg/median figures below. Lets the UI show
    -- "median $X–$Y (based on N jobs)" instead of implying every active job.
    count(*) filter (
        where j.is_active
          and (j.salary_min is not null or j.salary_max is not null)
          and j.salary_currency = 'USD' and j.salary_period = 'yearly'
    ) as salary_sample_size,
    avg(j.salary_min) filter (
        where j.is_active and j.salary_min is not null
          and j.salary_currency = 'USD' and j.salary_period = 'yearly'
    ) as avg_salary_min,
    avg(j.salary_max) filter (
        where j.is_active and j.salary_max is not null
          and j.salary_currency = 'USD' and j.salary_period = 'yearly'
    ) as avg_salary_max,
    percentile_cont(0.5) within group (order by j.salary_min) filter (
        where j.is_active and j.salary_min is not null
          and j.salary_currency = 'USD' and j.salary_period = 'yearly'
    ) as median_salary_min,
    percentile_cont(0.5) within group (order by j.salary_max) filter (
        where j.is_active and j.salary_max is not null
          and j.salary_currency = 'USD' and j.salary_period = 'yearly'
    ) as median_salary_max
from {{ source('jobpulse', 'jobs') }} j
join {{ source('jobpulse', 'companies') }} c
    on c.company_id = j.company_id and c.enabled = true
left join first_run fr on fr.company_id = j.company_id
cross join snap
group by j.company_id, fr.first_run_date, snap.d

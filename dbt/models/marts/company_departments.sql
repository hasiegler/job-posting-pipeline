-- Daily per-company department breakdown.
--
-- Ported from refresh_company_analytics() in
-- dags/datawarehouse/data_modification.py (the company_departments INSERT).
--
-- Incremental snapshot table. The SELECT below always computes exactly ONE
-- snapshot (current_date). The `delete+insert` strategy keyed on
-- (company_id, snapshot_date, department_name) means each run:
--   * deletes any existing rows for TODAY's keys, then inserts today's rows,
--   * leaves every PRIOR snapshot_date untouched.
-- That reproduces the Python "DELETE today / INSERT today" pattern exactly,
-- so history accumulates one snapshot per day.
--
-- WARNING: never run `dbt run --full-refresh` against this model in prod — it
-- DROPs and rebuilds the table, destroying every historical snapshot. The
-- daily history lives only in these tables; there is no other copy.
{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['company_id', 'snapshot_date', 'department_name']
  )
}}

with active_totals as (
    -- Denominator for `percentage`: active postings per company.
    select
        company_id,
        count(*) as total
    from {{ source('jobpulse', 'jobs') }}
    where is_active
    group by company_id
)

select
    j.company_id,
    -- Snapshot date pinned to UTC, independent of the DB session timezone
    -- (current_date alone would follow whatever TimeZone the session has set).
    (now() at time zone 'utc')::date as snapshot_date,
    d.dept as department_name,
    count(*) as active_job_count,
    round(100.0 * count(*) / ac.total, 2) as percentage,
    -- Of the jobs in this department, how many carried a usable USD/yearly
    -- salary — the sample size behind the avg/median below. `active_job_count`
    -- is NOT that number, so the UI should show this next to the salary range.
    count(*) filter (
        where (j.salary_min is not null or j.salary_max is not null)
          and j.salary_currency = 'USD'
          and j.salary_period = 'yearly'
    ) as salary_sample_size,
    avg(j.salary_min) filter (
        where j.salary_min is not null
          and j.salary_currency = 'USD'
          and j.salary_period = 'yearly'
    ) as avg_salary_min,
    avg(j.salary_max) filter (
        where j.salary_max is not null
          and j.salary_currency = 'USD'
          and j.salary_period = 'yearly'
    ) as avg_salary_max,
    percentile_cont(0.5) within group (order by j.salary_min) filter (
        where j.salary_min is not null
          and j.salary_currency = 'USD'
          and j.salary_period = 'yearly'
    ) as median_salary_min,
    percentile_cont(0.5) within group (order by j.salary_max) filter (
        where j.salary_max is not null
          and j.salary_currency = 'USD'
          and j.salary_period = 'yearly'
    ) as median_salary_max
from {{ source('jobpulse', 'jobs') }} j,
     lateral unnest(j.departments) as d(dept),
     active_totals ac
where j.is_active
  and j.company_id = ac.company_id
  and j.company_id in (
      select company_id
      from {{ source('jobpulse', 'companies') }}
      where enabled = true
  )
group by j.company_id, d.dept, ac.total

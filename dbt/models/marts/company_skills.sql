-- Daily per-company skill demand.
--
-- Ported from refresh_company_analytics() in
-- dags/datawarehouse/data_modification.py (the company_skills INSERT).
-- Same incremental snapshot pattern as company_departments.
--
-- WARNING: never run `dbt run --full-refresh` against this model in prod — it
-- DROPs and rebuilds the table, destroying every historical snapshot.
{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['company_id', 'snapshot_date', 'skill_name']
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
    (now() at time zone 'utc')::date as snapshot_date,
    s.skill as skill_name,
    count(*) as mention_count,
    round(100.0 * count(*) / ac.total, 2) as percentage,
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
     lateral (select distinct unnest(j.skills)) as s(skill),
     active_totals ac
where j.is_active
  and j.company_id = ac.company_id
  and j.company_id in (
      select company_id
      from {{ source('jobpulse', 'companies') }}
      where enabled = true
  )
group by j.company_id, s.skill, ac.total
-- Drop boilerplate-shaped rows: a skill that fires on (almost) every active
-- posting at a company is almost always sitting in the "About <Company>" intro,
-- not a real per-role requirement. Raw jobs.skills is left untouched.
having not (
    count(*) > {{ var('boilerplate_skill_min_mentions') }}
    and 100.0 * count(*) / ac.total >= {{ var('boilerplate_skill_min_percentage') }}
)

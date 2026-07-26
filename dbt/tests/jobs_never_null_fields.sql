-- Never-null fields: warns when any field that should essentially always be
-- populated exceeds the null/empty-rate threshold across ACTIVE jobs.
-- `location` treats the Greenhouse 'Unknown' default as missing; `departments`
-- treats an empty array as missing. Returns one row per offending field.
-- (Was _check_null_rates in quality_checks.py.)
with active as (
    select *
    from {{ source('jobpulse', 'jobs') }}
    where is_active = true
),

totals as (
    select count(*)::numeric as total from active
),

rates as (
    select 'title'            as field, count(*) filter (where title is null or title = '') as n from active
    union all
    select 'location',            count(*) filter (where location is null or location = '' or location = 'Unknown') from active
    union all
    select 'source_job_id',       count(*) filter (where source_job_id is null or source_job_id = '') from active
    union all
    select 'source_url',          count(*) filter (where source_url is null or source_url = '') from active
    union all
    select 'description_text',     count(*) filter (where description_text is null or description_text = '') from active
    union all
    select 'departments',          count(*) filter (where departments is null or cardinality(departments) = 0) from active
)

select
    r.field,
    r.n,
    t.total,
    round(100.0 * r.n / nullif(t.total, 0), 2) as null_pct
from rates r
cross join totals t
where t.total > 0
  and r.n::numeric / t.total > {{ var('null_rate_threshold') }}

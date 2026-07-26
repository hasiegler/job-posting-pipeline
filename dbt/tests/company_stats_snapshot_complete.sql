-- Analytics completeness (#10): every enabled company that currently has
-- active jobs should have a company_stats row for today's snapshot. A gap
-- means the analytics build didn't produce output for that company.
-- (Companies with no active jobs are intentionally excluded — they legitimately
-- produce no stats row.)
with today as (
    select (now() at time zone 'utc')::date as d
)

select
    c.company_id,
    c.company_name
from {{ source('jobpulse', 'companies') }} c
cross join today
where c.enabled = true
  and exists (
      select 1 from {{ source('jobpulse', 'jobs') }} j
      where j.company_id = c.company_id and j.is_active
  )
  and not exists (
      select 1 from {{ ref('company_stats') }} s
      where s.company_id = c.company_id
        and s.snapshot_date = today.d
  )

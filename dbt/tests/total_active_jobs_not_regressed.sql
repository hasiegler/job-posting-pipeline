-- Total active-jobs regression (#11): warns if today's total active jobs
-- (summed across companies) collapses relative to the trailing baseline of
-- prior daily snapshots. Uses company_stats' own snapshot history as the time
-- series. Returns a row (→ warn) only when today < factor * trailing average.
with today as (
    select (now() at time zone 'utc')::date as d
),

daily as (
    select
        snapshot_date,
        sum(active_jobs) as total_active
    from {{ ref('company_stats') }}
    group by snapshot_date
),

current_day as (
    select d.total_active
    from daily d
    cross join today
    where d.snapshot_date = today.d
),

baseline as (
    select
        avg(b.total_active) as avg_active,
        count(*) as n
    from (
        select d.total_active
        from daily d
        cross join today
        where d.snapshot_date < today.d
        order by d.snapshot_date desc
        limit {{ var('baseline_snapshots') }}
    ) b
)

select
    current_day.total_active as today_active_jobs,
    round(baseline.avg_active, 0) as trailing_avg,
    baseline.n as baseline_snapshots
from current_day
cross join baseline
where baseline.n >= 1
  and current_day.total_active < baseline.avg_active * {{ var('active_jobs_regression_factor') }}

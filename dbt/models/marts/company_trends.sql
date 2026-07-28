-- Convenience READ view for the frontend: one row per company per day, keyed by
-- the URL slug (companies.canonical_name) so the app can select a company's full
-- time series from a single relation instead of re-joining company_stats to
-- companies every time.
--
-- Materialized as a view (no storage, no history of its own) — it just reshapes
-- the company_stats snapshot table. All the daily history still lives in
-- company_stats; this is purely an access convenience.
{{
  config(
    materialized = 'view',
    post_hook = "grant select on {{ this }} to anon, authenticated"
  )
}}

select
    c.canonical_name,
    c.company_name,
    cs.company_id,
    cs.snapshot_date,
    cs.active_jobs,
    cs.posted_7d,
    cs.posted_30d,
    cs.closed_7d,
    cs.closed_30d,
    cs.net_change_7d,
    cs.net_change_30d,
    cs.remote_count,
    cs.hybrid_count,
    cs.onsite_count,
    cs.avg_salary_min,
    cs.avg_salary_max,
    cs.median_salary_min,
    cs.median_salary_max,
    cs.salary_sample_size
from {{ ref('company_stats') }} cs
join {{ source('jobpulse', 'companies') }} c
    on c.company_id = cs.company_id

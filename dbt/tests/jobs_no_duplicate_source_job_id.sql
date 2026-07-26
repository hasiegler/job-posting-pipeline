-- Duplicate guard: jobs has UNIQUE(company_id, source_job_id), so this should
-- always be empty. It catches that constraint being dropped/violated.
-- (Was _check_duplicates in quality_checks.py.)
select
    company_id,
    source_job_id,
    count(*) as n
from {{ source('jobpulse', 'jobs') }}
group by company_id, source_job_id
having count(*) > 1

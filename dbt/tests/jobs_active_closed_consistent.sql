-- Lifecycle invariant (#1): an active job must have no close date, and a
-- closed job must have one. A violation means the close-detection logic in
-- process_staging_to_jobs has a bug.
select
    job_id,
    company_id,
    is_active,
    date_closed
from {{ source('jobpulse', 'jobs') }}
where (is_active = true and date_closed is not null)
   or (is_active = false and date_closed is null)

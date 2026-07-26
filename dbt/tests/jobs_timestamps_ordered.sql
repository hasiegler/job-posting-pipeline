-- Timestamp ordering (#2): last_seen must be >= first_seen, and a job can't be
-- closed before it was first published. Catches clock/logic errors.
select
    job_id,
    first_seen,
    last_seen,
    first_published_at,
    date_closed
from {{ source('jobpulse', 'jobs') }}
where last_seen < first_seen
   or (date_closed is not null
       and first_published_at is not null
       and date_closed < first_published_at)

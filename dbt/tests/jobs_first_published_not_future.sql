-- Future-date guard (#3): a posting first-published in the future signals a
-- parse or timezone bug. Compared against UTC now().
select
    job_id,
    company_id,
    first_published_at
from {{ source('jobpulse', 'jobs') }}
where first_published_at > now()

-- Extraction coverage (#7): after the extract step, every active job should
-- have been through field extraction (extracted_at set). Active rows with a
-- NULL extracted_at mean extraction silently skipped them.
select
    job_id,
    company_id,
    first_seen
from {{ source('jobpulse', 'jobs') }}
where is_active = true
  and extracted_at is null

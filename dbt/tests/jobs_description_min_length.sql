-- Description length (#8): active jobs with a suspiciously short (but non-NULL)
-- description usually indicate an HTML-parse failure in normalization. Empty /
-- NULL descriptions are covered separately by jobs_never_null_fields.
select
    job_id,
    company_id,
    length(description_text) as description_length
from {{ source('jobpulse', 'jobs') }}
where is_active = true
  and description_text is not null
  and length(description_text) < {{ var('min_description_length') }}

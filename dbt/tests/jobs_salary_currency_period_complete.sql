-- Salary completeness (#6): if a salary figure is present, its currency and
-- period must also be present — otherwise the value is unusable by the salary
-- analytics (which filter on salary_currency='USD' and salary_period='yearly').
select
    job_id,
    company_id,
    salary_min,
    salary_max,
    salary_currency,
    salary_period
from {{ source('jobpulse', 'jobs') }}
where (salary_min is not null or salary_max is not null)
  and (salary_currency is null or salary_period is null)

-- Salary sanity: flags active jobs with impossible salaries — non-positive
-- min, max < min, or a *yearly* figure outside the plausible band. The band
-- applies only to yearly salaries (an hourly $50 is legitimately < 1000).
-- (Was _check_salary_sanity in quality_checks.py.)
select
    job_id,
    company_id,
    salary_min,
    salary_max,
    salary_currency,
    salary_period
from {{ source('jobpulse', 'jobs') }}
where is_active = true
  and (
        (salary_min is not null and salary_min <= 0)
     or (salary_min is not null and salary_max is not null and salary_max < salary_min)
     or (salary_period = 'yearly' and salary_min is not null
         and (salary_min < {{ var('salary_min_plausible') }} or salary_min > {{ var('salary_max_plausible') }}))
     or (salary_period = 'yearly' and salary_max is not null
         and (salary_max < {{ var('salary_min_plausible') }} or salary_max > {{ var('salary_max_plausible') }}))
  )

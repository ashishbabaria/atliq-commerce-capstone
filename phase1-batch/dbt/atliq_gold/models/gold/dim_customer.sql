-- One row per customer, with the signup cohort month for new-vs-returning analysis.
select
    c.customer_id,
    c.customer_name,
    c.email,
    c.city,
    c.signup_date,
    date_trunc('month', c.signup_date) as signup_month
from {{ ref('stg_customers') }} c
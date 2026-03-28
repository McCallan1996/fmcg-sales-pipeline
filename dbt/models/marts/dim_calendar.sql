select distinct
    sale_date,
    year,
    month,
    quarter,
    week_of_year,
    day_of_week,
    is_weekend,
    format_date('%B', sale_date) as month_name,
    format_date('%A', sale_date) as day_name
from {{ ref('stg_fmcg_sales') }}
order by sale_date

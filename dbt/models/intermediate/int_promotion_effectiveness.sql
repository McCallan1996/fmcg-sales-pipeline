select
    category,
    brand,
    sales_channel,
    has_promotion,
    year,
    month,

    count(*) as num_days,
    sum(sales_quantity) as total_qty,
    avg(sales_quantity) as avg_daily_qty,
    sum(revenue) as total_revenue,
    avg(revenue) as avg_daily_revenue,
    avg(price) as avg_price

from {{ ref('stg_fmcg_sales') }}
group by 1, 2, 3, 4, 5, 6

select
    sale_date,
    sales_channel,
    region,
    category,

    count(*) as txn_count,
    sum(sales_quantity) as total_qty,
    sum(revenue) as total_revenue,
    avg(price) as avg_price,
    avg(delivery_lag) as avg_delivery_lag,

    countif(has_promotion) as promo_txns,
    sum(case when has_promotion then revenue else 0 end) as promo_revenue

from {{ ref('stg_fmcg_sales') }}
group by 1, 2, 3, 4

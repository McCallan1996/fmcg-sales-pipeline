{{
    config(
        materialized='table',
        partition_by={
            "field": "sale_date",
            "data_type": "date",
            "granularity": "month"
        },
        cluster_by=["sales_channel", "region"]
    )
}}

/*
  Partitioned by month because the dashboard queries monthly/weekly trends,
  so BQ can skip irrelevant months. Clustered by channel + region since those
  are the two most common filter dimensions in the dashboard.
*/

select
    sale_date,
    sales_channel,
    region,

    sum(total_qty) as total_quantity_sold,
    sum(total_revenue) as total_revenue,
    sum(txn_count) as total_transactions,
    sum(promo_txns) as promo_transactions,
    sum(promo_revenue) as promo_revenue,
    avg(avg_price) as avg_unit_price,
    avg(avg_delivery_lag) as avg_delivery_lag

from {{ ref('int_daily_channel_sales') }}
group by 1, 2, 3

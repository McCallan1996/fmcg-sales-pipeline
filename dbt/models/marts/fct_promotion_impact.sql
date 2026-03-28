{{
    config(
        materialized='table',
        partition_by={
            "field": "report_month",
            "data_type": "date",
            "granularity": "month"
        },
        cluster_by=["category", "sales_channel"]
    )
}}

with promo_stats as (
    select
        category,
        brand,
        sales_channel,
        year,
        month,
        avg(case when has_promotion then avg_daily_qty end) as avg_qty_with_promo,
        avg(case when not has_promotion then avg_daily_qty end) as avg_qty_without_promo,
        avg(case when has_promotion then avg_daily_revenue end) as avg_rev_with_promo,
        avg(case when not has_promotion then avg_daily_revenue end) as avg_rev_without_promo
    from {{ ref('int_promotion_effectiveness') }}
    group by 1, 2, 3, 4, 5
)

select
    *,
    date(year, month, 1) as report_month,
    safe_divide(avg_qty_with_promo - avg_qty_without_promo, avg_qty_without_promo) as quantity_lift_pct,
    safe_divide(avg_rev_with_promo - avg_rev_without_promo, avg_rev_without_promo) as revenue_lift_pct
from promo_stats

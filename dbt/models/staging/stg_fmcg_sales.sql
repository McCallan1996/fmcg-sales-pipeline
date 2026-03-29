with source as (
    select * from {{ source('fmcg_processed', 'daily_sales_enriched') }}
),

cleaned as (
    select
        {{ dbt_utils.generate_surrogate_key(['sale_date', 'sku', 'channel', 'region']) }} as sale_id,

        sale_date,
        sku,
        brand,
        segment,
        category,
        channel as sales_channel,
        region,
        pack_type,

        cast(price_unit as float64) as price,
        cast(units_sold as int64) as sales_quantity,
        cast(stock_available as int64) as stock_level,
        cast(delivery_days as int64) as delivery_lag,
        cast(delivered_qty as int64) as delivered_qty,
        coalesce(cast(promotion_flag as bool), false) as has_promotion,

        revenue,
        year,
        month,
        quarter,
        day_of_week,
        week_of_year,
        is_weekend

    from source
    where sale_date is not null
)

select * from cleaned

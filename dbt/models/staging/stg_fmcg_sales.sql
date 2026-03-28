with source as (
    select * from {{ source('fmcg_processed', 'daily_sales_enriched') }}
),

cleaned as (
    select
        {{ dbt_utils.generate_surrogate_key(['sale_date', 'SKU', 'Sales_Channel', 'Region']) }} as sale_id,

        sale_date,
        SKU as sku,
        Brand as brand,
        Segment as segment,
        Category as category,
        Sales_Channel as sales_channel,
        Region as region,
        Pack_Type as pack_type,

        cast(Sales_Quantity as int64) as sales_quantity,
        cast(Price as float64) as price,
        cast(Stock_Level as int64) as stock_level,
        cast(Delivery_Lag as int64) as delivery_lag,
        coalesce(cast(Promotion as bool), false) as has_promotion,

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

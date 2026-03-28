select
    sku,
    brand,
    segment,
    category,
    pack_type,
    min(sale_date) as first_seen,
    max(sale_date) as last_seen,
    count(distinct sale_date) as active_days
from {{ ref('stg_fmcg_sales') }}
group by 1, 2, 3, 4, 5

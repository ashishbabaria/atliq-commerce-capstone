-- One row per product, enriched with supplier cost (from the CSV source) and margin.
select
    p.product_id,
    p.product_name,
    p.category,
    p.unit_price,
    s.supplier_cost,
    (p.unit_price - s.supplier_cost) as unit_margin
from {{ ref('stg_products') }} p
left join {{ ref('stg_supplier_price_list') }} s
       on p.product_id = s.product_id
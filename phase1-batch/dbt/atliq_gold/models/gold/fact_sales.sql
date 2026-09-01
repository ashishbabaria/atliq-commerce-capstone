-- Grain: one row per order item. Measures: quantity, item_price, gross_revenue.
select
    oi.order_item_id,
    o.order_id,
    o.customer_id,
    oi.product_id,
    o.order_date,
    cast(date_format(o.order_date, 'yyyyMMdd') as int) as date_key,
    oi.quantity,
    oi.item_price,
    (oi.quantity * oi.item_price) as gross_revenue,
    o.status
from {{ ref('stg_order_items') }} oi
join {{ ref('stg_orders') }} o
  on oi.order_id = o.order_id
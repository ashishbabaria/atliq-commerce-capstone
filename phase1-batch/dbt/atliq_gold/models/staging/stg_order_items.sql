select order_item_id, order_id, product_id, quantity, item_price
from {{ source('silver', 'order_items') }}
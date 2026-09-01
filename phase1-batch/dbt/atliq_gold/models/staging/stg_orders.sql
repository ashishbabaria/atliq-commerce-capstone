select order_id, customer_id, order_date, status, order_amount
from {{ source('silver', 'orders') }}
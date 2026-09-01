select product_id, supplier_name, supplier_cost, effective_date
from {{ source('silver', 'supplier_price_list') }}
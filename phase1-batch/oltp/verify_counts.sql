-- ============================================================================
--  AtliQ Commerce  |  Load verification  (run after M1 seed load)
--  Expected: 40 customers, 25 products, 300 orders, 783 order_items, 246 payments
-- ============================================================================
SELECT 'customers'   AS tbl, COUNT(*) AS [rows] FROM dbo.customers
UNION ALL SELECT 'products',    COUNT(*) FROM dbo.products
UNION ALL SELECT 'orders',      COUNT(*) FROM dbo.orders
UNION ALL SELECT 'order_items', COUNT(*) FROM dbo.order_items
UNION ALL SELECT 'payments',    COUNT(*) FROM dbo.payments;

-- Revenue reconciliation: three independent sums must match.
-- (order_amount on the header) = (sum of its line items) = (payment amount)
SELECT
    (SELECT SUM(order_amount) FROM dbo.orders)                        AS header_total,
    (SELECT SUM(quantity * item_price) FROM dbo.order_items)          AS line_item_total,
    (SELECT SUM(amount) FROM dbo.payments)                            AS payment_total;

# Phase 2 Write-up: The Speed Lane

## Two lanes, two questions

The batch lane (Phase 1) answers *what happened yesterday?* Once a night it syncs the
OLTP database, reconciles every order and rebuilds Gold. It is complete and audited,
which makes it the system of record for finance and leadership reporting.

The speed lane (Phase 2) answers *what is happening right now?* Order events reach
Databricks within seconds of checkout, and Gold shows revenue per 5 minutes. It is fresh
but provisional, built for operations: a sudden drop in paid orders, a city that stops
converting, a payment method failing in the middle of a sale.

A business needs both. The lanes share business keys (order_id, customer_id, product_id,
city) but not tables, because the same order also reaches the batch lane overnight.
Merging them would count it twice.

## Why Gold windows appear late

Gold writes a 5-minute window only once it can no longer change. Spark tracks a
watermark, the newest event time seen minus 10 minutes, and closes a window when the
watermark passes its end. On serverless the stream runs with `availableNow`, so a window
is written on the run after the one that moved the watermark.

The test made this visible. With events still arriving at 20:27 UTC, the newest closed
window ended at 19:20. After the burst it ended at 20:25, with the clock at 20:46. The
delay is the price of correctness: every published window is final, and each one matched
Silver to the paisa.

## Why events are keyed by order_id

Kafka keeps order only within a partition, and equal keys always land in the same
partition. Keying by `order_id` keeps each order's lifecycle together and in sequence.
Order 1790841036 shows it: placed, paid and then cancelled, all on partition 2 at
offsets 2, 3 and 8.

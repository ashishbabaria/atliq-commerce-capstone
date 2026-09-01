# M1: OLTP Database (Azure SQL)

The operational source of the batch pipeline: a normalized (3NF) storefront
database that powers AtliQ Commerce. It is seeded with realistic data and
verified before any downstream work begins.

## What is here

| File | Purpose |
|------|---------|
| `01_schema_ddl.sql` | Creates the five OLTP tables (customers, products, orders, order_items, payments) with primary keys, foreign keys, and timestamp columns. Safe to re-run. |
| `02_insert_customers.sql` | Seeds 40 customers. |
| `03_insert_products.sql` | Seeds 25 products. |
| `04_insert_orders.sql` | Seeds 300 orders. |
| `05_insert_order_items.sql` | Seeds 783 order line items. |
| `06_insert_payments.sql` | Seeds 246 payments. |
| `07_etl_control_table.sql` | Creates `etl.control_table` (one row per source, with load type and watermark column) and the `usp_update_watermark` procedure. Drives the metadata-driven ingestion in M2. |
| `08_audit_run_log.sql` | Creates `etl.run_log` and logging procedures: the pipeline's run recorder for the reliability milestone (M7). |
| `verify_counts.sql` | Confirms exact row counts and reconciles revenue three independent ways. |
| `daily_order_simulator.py` | Injects new orders into the database to demonstrate incremental loading in M5. Reads DB credentials from a local `.env` (not committed). |
| `csv/` | Two flat-file sources (supplier price list, marketing spend) ingested alongside the SQL tables in M2. |

## Run order

Run the SQL scripts in order against the `atliq_commerce` database:

```
01_schema_ddl.sql
02_insert_customers.sql
03_insert_products.sql
04_insert_orders.sql
05_insert_order_items.sql
06_insert_payments.sql
07_etl_control_table.sql
08_audit_run_log.sql
verify_counts.sql
```

Order matters for `02`-`06`: parent tables load before child tables so every
foreign key resolves.

## Verification

`verify_counts.sql` confirms the seed load:

| Table | Rows |
|-------|------|
| customers | 40 |
| products | 25 |
| orders | 300 |
| order_items | 783 |
| payments | 246 |

It also reconciles revenue: the order header total equals the sum of the line
items, proving the seed data is internally consistent.

## Design notes

- Normalized 3NF mirrors a real transactional storefront (fast writes, no
  duplication). The heavy analytical work happens downstream in the lakehouse,
  not here, so the live database is never slowed by reporting queries.
- The `updated_at` and `created_at` columns are the watermarks the incremental
  nightly sync relies on.
- `etl.control_table` is the metadata that drives the whole ingestion pipeline:
  a new source is added by inserting a row, not by editing any pipeline.

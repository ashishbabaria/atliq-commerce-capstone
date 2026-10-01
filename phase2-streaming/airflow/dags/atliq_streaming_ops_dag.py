"""
AtliQ Phase 2 (LEARNER STARTER) — Streaming Ops DAG
====================================================
The stream never stops — but the SCHEDULED work around it is your job:
data-quality gate, table maintenance, daily rollup. Build an hourly DAG:

    check_fresh_events  ->  optimize_tables  ->  refresh_daily_summary

Setup:
1. In docker-compose.yaml:  _PIP_ADDITIONAL_REQUIREMENTS: apache-airflow-providers-databricks
   then: docker compose down && docker compose up -d
2. Airflow UI -> Admin -> Connections -> +
   Conn Id: databricks_default | Type: Databricks
   Host: https://<workspace>.cloud.databricks.com | Password: <PAT token>
3. Paste your SQL warehouse HTTP path below.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.databricks.operators.databricks_sql import DatabricksSqlOperator

SQL_WAREHOUSE_HTTP_PATH = "/sql/1.0/warehouses/xxxxxxxxxxxxxxxx"   # <-- yours

default_args = {
    "owner": "atliq-data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="atliq_streaming_ops",
    start_date=datetime(2026, 8, 1),
    schedule="@hourly",
    catchup=False,
    default_args=default_args,
    tags=["atliq", "phase2", "streaming"],
) as dag:

    # TODO 1 — Data-quality gate.
    # A DatabricksSqlOperator that FAILS when no events landed recently.
    # Hint: Databricks SQL has assert_true(condition, message) — write a SELECT
    # that asserts COUNT(*) > 0 over silver_order_events for the last 2 hours.
    # A DQ check that can never fail is worth zero marks — you must be able to
    # demo it failing when the producer is stopped.
    check_fresh_events = None  # replace with your operator

    # TODO 2 — Table maintenance.
    # Streaming writes create many small files. Run OPTIMIZE on
    # silver_order_events and gold_revenue_5min.
    optimize_tables = None  # replace with your operator

    # TODO 3 — Daily rollup.
    # CREATE OR REPLACE atliq.streaming.gold_daily_summary: per event_date —
    # orders_placed, orders_paid, orders_cancelled, revenue (from paid events).
    # Hint: COUNT_IF() and a CASE inside SUM().
    refresh_daily_summary = None  # replace with your operator

    # TODO 4 — Chain them in order:
    # check_fresh_events >> optimize_tables >> refresh_daily_summary

"""
AtliQ Phase 2: Streaming Ops DAG
The stream never stops, but the scheduled work around it runs hourly:

    check_fresh_events  ->  optimize_tables  ->  refresh_daily_summary

Connection: databricks_default (Databricks host + personal access token),
created in the Airflow UI under Admin -> Connections.
"""
from datetime import datetime, timedelta

from airflow.sdk import DAG   # Airflow 3 import, as taught in Session 15
from airflow.providers.databricks.operators.databricks_sql import DatabricksSqlOperator

SQL_WAREHOUSE_HTTP_PATH = "/sql/1.0/warehouses/af3166d1352e2862"   # <-- paste yours
CONN_ID = "databricks_default"
S = "atliq.streaming"

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

    # 1. Data-quality gate: FAIL when no events reached Silver in the last 2 hours.
    #    assert_true raises an error when its condition is false, which fails the task.
    check_fresh_events = DatabricksSqlOperator(
        task_id="check_fresh_events",
        databricks_conn_id=CONN_ID,
        http_path=SQL_WAREHOUSE_HTTP_PATH,
        sql=f"""
            SELECT assert_true(
                       COUNT(*) > 0,
                       'DQ FAIL: no events in {S}.silver_order_events in the last 2 hours'
                   ) AS freshness_ok
            FROM {S}.silver_order_events
            WHERE event_ts >= current_timestamp() - INTERVAL 2 HOURS
        """,
        retries=0,   # stale data will not fix itself in 5 minutes: fail fast, do not retry
    )

    # 2. Maintenance: streaming writes many small files; OPTIMIZE compacts them.
    optimize_tables = DatabricksSqlOperator(
        task_id="optimize_tables",
        databricks_conn_id=CONN_ID,
        http_path=SQL_WAREHOUSE_HTTP_PATH,
        sql=[
            f"OPTIMIZE {S}.silver_order_events",
            f"OPTIMIZE {S}.gold_revenue_5min",
        ],
    )

    # 3. Daily rollup: rebuilt from Silver each run, so it is always complete and idempotent.
    refresh_daily_summary = DatabricksSqlOperator(
        task_id="refresh_daily_summary",
        databricks_conn_id=CONN_ID,
        http_path=SQL_WAREHOUSE_HTTP_PATH,
        sql=f"""
            CREATE OR REPLACE TABLE {S}.gold_daily_summary AS
            SELECT
                to_date(event_ts)                                   AS event_date,
                COUNT_IF(event_type = 'order_placed')               AS orders_placed,
                COUNT_IF(event_type = 'payment_received')           AS orders_paid,
                COUNT_IF(event_type = 'order_cancelled')            AS orders_cancelled,
                SUM(CASE WHEN event_type = 'payment_received'
                         THEN order_amount ELSE 0 END)              AS revenue,
                current_timestamp()                                 AS refreshed_at
            FROM {S}.silver_order_events
            GROUP BY to_date(event_ts)
        """,
    )

    # 4. Gate first, so maintenance and rollup never run on stale data.
    check_fresh_events >> optimize_tables >> refresh_daily_summary
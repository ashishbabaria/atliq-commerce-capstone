# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # AtliQ Phase 2 (LEARNER STARTER) — Kafka → Delta with Structured Streaming
# MAGIC Complete the TODOs to build Bronze → Silver → Gold as **streams**.
# MAGIC
# MAGIC **Free Edition (serverless) rules:** checkpoints go in a Unity Catalog
# MAGIC **Volume** (no DBFS), streams write to **managed tables**, and every stream
# MAGIC needs its **own** checkpoint folder.

# COMMAND ----------

from pyspark.sql import functions as F, types as T

# Credentials from the secret scope, never hard-coded
SECRET_SCOPE     = "atliq-kafka"
KAFKA_BOOTSTRAP  = dbutils.secrets.get(SECRET_SCOPE, "bootstrap")
KAFKA_API_KEY    = dbutils.secrets.get(SECRET_SCOPE, "api-key")
KAFKA_API_SECRET = dbutils.secrets.get(SECRET_SCOPE, "api-secret")
TOPIC = "atliq.orders.events"

CATALOG, SCHEMA = "atliq", "streaming"
CKPT   = f"/Volumes/{CATALOG}/{SCHEMA}/checkpoints"      # one sub-folder per stream
BRONZE = f"{CATALOG}.{SCHEMA}.bronze_order_events"
SILVER = f"{CATALOG}.{SCHEMA}.silver_order_events"
GOLD   = f"{CATALOG}.{SCHEMA}.gold_revenue_5min"

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.checkpoints")
print("Config OK")

# COMMAND ----------

# MAGIC %md ## TASK 1 — Bronze: raw events off Kafka, no parsing
# MAGIC Read the topic with `spark.readStream.format("kafka")` and append the raw
# MAGIC records to `atliq.streaming.bronze_order_events`.
# MAGIC
# MAGIC Hints:
# MAGIC - Options you need: `kafka.bootstrap.servers`, `subscribe`,
# MAGIC   `startingOffsets = earliest`, `kafka.security.protocol = SASL_SSL`,
# MAGIC   `kafka.sasl.mechanism = PLAIN`, and `kafka.sasl.jaas.config`
# MAGIC   (on Databricks the login module class is
# MAGIC   `kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule`).
# MAGIC - Kafka gives you binary key/value — CAST both to STRING.
# MAGIC - Keep topic, partition, offset, timestamp columns too. Bronze keeps everything.
# MAGIC - writeStream: outputMode "append", checkpointLocation f"{CKPT}/bronze",
# MAGIC   .toTable(...)

# COMMAND ----------

# Confluent login. On Databricks, Kafka classes are shaded, hence the kafkashaded prefix.
JAAS = (
    "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required "
    f'username="{KAFKA_API_KEY}" password="{KAFKA_API_SECRET}";'
)

bronze_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "earliest")     # first run only; afterwards the checkpoint decides
    .option("kafka.security.protocol", "SASL_SSL")
    .option("kafka.sasl.mechanism", "PLAIN")
    .option("kafka.sasl.jaas.config", JAAS)
    .load()
    # Bronze keeps everything, no parsing: binary key/value become strings
    .select(
        F.col("key").cast("string").alias("key"),
        F.col("value").cast("string").alias("value"),
        "topic", "partition", "offset",
        F.col("timestamp").alias("kafka_ts"),
        F.current_timestamp().alias("ingested_at"),
    )
)

bronze_q = (
    bronze_df.writeStream
    .outputMode("append")
    .option("checkpointLocation", f"{CKPT}/bronze")   # Bronze's own checkpoint
    .trigger(availableNow=True)                      # serverless: process all new events, then stop
    .toTable(BRONZE)
)
bronze_q.awaitTermination()   # Silver reads this table, so let Bronze finish first
print("Bronze rows:", spark.table(BRONZE).count())


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT key, partition, offset, kafka_ts, ingested_at, left(value, 60) AS value_start
# MAGIC FROM atliq.streaming.bronze_order_events
# MAGIC ORDER BY kafka_ts DESC LIMIT 10;
# MAGIC

# COMMAND ----------

# MAGIC %md ## TASK 2 — Silver: parse, de-duplicate, handle late data
# MAGIC Stream FROM the Bronze table into `atliq.streaming.silver_order_events`:
# MAGIC 1. Parse the JSON value with an explicit schema (event_id, event_type,
# MAGIC    event_ts, order_id, customer_id, city, product_id, quantity,
# MAGIC    order_amount, payment_method).
# MAGIC 2. Convert event_ts to a real timestamp.
# MAGIC 3. Add a **10-minute watermark** on event_ts, then
# MAGIC    **dropDuplicates(["event_id"])** — so a replayed event can never land twice.
# MAGIC
# MAGIC Hint: `spark.readStream.table(...)`, `F.from_json`, `withWatermark`.

# COMMAND ----------

# Explicit schema: types are decided by us, not guessed from the data
event_schema = T.StructType([
    T.StructField("event_id",       T.StringType()),
    T.StructField("event_type",     T.StringType()),
    T.StructField("event_ts",       T.StringType()),
    T.StructField("order_id",       T.LongType()),
    T.StructField("customer_id",    T.IntegerType()),
    T.StructField("city",           T.StringType()),
    T.StructField("product_id",     T.IntegerType()),
    T.StructField("quantity",       T.IntegerType()),
    T.StructField("order_amount",   T.DecimalType(12, 2)),
    T.StructField("payment_method", T.StringType()),
])

silver_df = (
    spark.readStream.table(BRONZE)
    .select(F.from_json("value", event_schema).alias("e"), "partition", "offset", "kafka_ts")
    .select("e.*", "partition", "offset", "kafka_ts")
    .filter(F.col("event_id").isNotNull())                  # drop records that failed to parse
    .withColumn("event_ts", F.to_timestamp("event_ts"))     # ISO string -> timestamp (UTC)
    .withWatermark("event_ts", "10 minutes")                # tolerate events up to 10 min late
    .dropDuplicates(["event_id", "event_ts"])               # a replayed event never lands twice
)

silver_q = (
    silver_df.writeStream
    .outputMode("append")
    .option("checkpointLocation", f"{CKPT}/silver")   # Silver's own checkpoint
    .trigger(availableNow=True)
    .toTable(SILVER)
)
silver_q.awaitTermination()
print("Silver rows:", spark.table(SILVER).count())


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT event_type, COUNT(*) AS events
# MAGIC FROM atliq.streaming.silver_order_events GROUP BY event_type ORDER BY events DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ### DEDUP PROOF

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS silver_rows, COUNT(DISTINCT event_id) AS distinct_events
# MAGIC FROM atliq.streaming.silver_order_events;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Order Trace

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT order_id, event_type, event_ts, partition, offset
# MAGIC FROM atliq.streaming.silver_order_events
# MAGIC WHERE order_id = 1790841036
# MAGIC ORDER BY offset;

# COMMAND ----------

# MAGIC %md ## TASK 3 — Gold: the live revenue ticker
# MAGIC From the Silver stream, keep only `payment_received` events and aggregate
# MAGIC into **5-minute tumbling windows**: orders_paid = count, revenue = sum of
# MAGIC order_amount. Append closed windows to `atliq.streaming.gold_revenue_5min`.
# MAGIC
# MAGIC Hint: `F.window("event_ts", "5 minutes")` — and think about WHY a window
# MAGIC only appears after the watermark passes its end (you will explain this
# MAGIC in your write-up).

# COMMAND ----------

gold_df = (
    spark.readStream.table(SILVER)
    .filter(F.col("event_type") == "payment_received")   # revenue = paid events only
    .withWatermark("event_ts", "10 minutes")             # watermarks do not travel between tables, so set it again
    .groupBy(F.window("event_ts", "5 minutes"))           # tumbling: fixed, non-overlapping 5-min buckets
    .agg(
        F.count("*").alias("orders_paid"),
        F.sum("order_amount").alias("revenue"),
    )
    .select(
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        "orders_paid", "revenue",
    )
)

gold_q = (
    gold_df.writeStream
    .outputMode("append")                            # a window is written once, only after it closes
    .option("checkpointLocation", f"{CKPT}/gold")   # Gold's own checkpoint
    .trigger(availableNow=True)
    .toTable(GOLD)
)
gold_q.awaitTermination()
print("Gold windows:", spark.table(GOLD).count())


# COMMAND ----------

# MAGIC %md
# MAGIC ### Lag Query

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT MAX(window_end) AS latest_closed_window, current_timestamp() AS now
# MAGIC FROM atliq.streaming.gold_revenue_5min;

# COMMAND ----------

# MAGIC %md ## Verify (given)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM atliq.streaming.gold_revenue_5min ORDER BY window_start DESC LIMIT 12;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT event_type, COUNT(*) AS events
# MAGIC FROM atliq.streaming.silver_order_events GROUP BY event_type;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reconcile Gold Against Silver

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT g.window_start, g.orders_paid,
# MAGIC        g.revenue AS gold_revenue, s.revenue AS silver_revenue,
# MAGIC        g.revenue - s.revenue AS diff
# MAGIC FROM atliq.streaming.gold_revenue_5min g
# MAGIC JOIN (
# MAGIC     SELECT window(event_ts, '5 minutes').start AS window_start,
# MAGIC            SUM(order_amount) AS revenue
# MAGIC     FROM atliq.streaming.silver_order_events
# MAGIC     WHERE event_type = 'payment_received'
# MAGIC     GROUP BY 1
# MAGIC ) s ON g.window_start = s.window_start
# MAGIC ORDER BY g.window_start DESC;
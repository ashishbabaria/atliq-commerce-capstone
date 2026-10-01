# AtliQ Commerce Data Engineering Capstone

End-to-end data engineering for AtliQ Commerce, an online storefront, built as two
independent lanes that run side by side, the way batch and streaming coexist in real
companies.

| Lane | Question it answers | Status |
|---|---|---|
| [Phase 1: Batch](phase1-batch/) | What happened yesterday? | Complete |
| [Phase 2: Speed](phase2-streaming/) | What is happening right now? | Complete |

## Repository structure

```
atliq-commerce-capstone/
├── .github/workflows/    CI: dbt build + test on every pull request
├── phase1-batch/         Batch lane: nightly OLTP to lakehouse to Fabric
│   └── docs/             Milestone decks (PDF) and screenshots
└── phase2-streaming/     Speed lane: Kafka to Databricks streaming, Airflow ops
    └── docs/             Task decks (PDF), screenshots and write-up
```

## Phase 1: Batch Lane

The operational database syncs to an analytics lakehouse every night, so leadership
sees yesterday's numbers each morning. Medallion architecture, orchestrated as one
scheduled, idempotent nightly job, reported through Power BI.

```
Azure SQL (OLTP, 3NF)
   -> ADF pipeline      metadata-driven ingest, watermark incrementals
      -> Bronze         Parquet, ingest_date partitions
      -> Silver         Delta, PySpark MERGE           [Databricks]
      -> Gold           star schema, 16 dbt tests      [dbt on Databricks]
   -> Power BI dashboard on Gold                       [Microsoft Fabric]
```

Seven milestones, each documented step by step with screenshots. Full detail, results,
and design decisions: **[phase1-batch/README.md](phase1-batch/README.md)**.

## Phase 2: Speed Lane

Order events stream from a producer through Kafka into the same Medallion pattern within
seconds, processed continuously by Databricks Structured Streaming, while an hourly
Airflow DAG runs the scheduled work around the stream. Standalone: it shares business
keys with Phase 1, never tables, so no order is counted twice.

```
Python producer          order events, keyed by order_id
   -> Confluent Kafka    topic atliq.orders.events
      -> Bronze          raw Kafka records          [Databricks Structured Streaming]
      -> Silver          parsed, de-duplicated
      -> Gold            5-minute revenue windows
   -> Airflow, hourly    freshness gate, OPTIMIZE, daily rollup
```

Four tasks, each documented step by step with screenshots. Full detail, results,
design decisions and the write-up: **[phase2-streaming/README.md](phase2-streaming/README.md)**.

## Engineering notes

- **Secrets** are never committed. dbt reads its connection from environment variables;
  CI injects them from GitHub repository secrets; the nightly job reads its token from
  Azure Key Vault through a Databricks secret scope. In the speed lane, Kafka credentials
  sit in a git-ignored `.env` locally and a Databricks secret scope in the workspace, and
  Airflow's Databricks token is stored encrypted and scoped to SQL only.
- **Idempotency** at every layer: watermark ingestion, Silver MERGE on the business key,
  and dbt rebuilding Gold from Silver. Re-running the chain never double-counts. In the
  speed lane, one checkpoint per stream and de-duplication on `event_id` give the same
  guarantee on restart.
- **Build artifacts** (dbt target and packages, logs, virtual environments, Airflow
  runtime folders) are excluded via .gitignore.

# AtliQ Commerce Data Engineering Capstone

End-to-end data engineering for AtliQ Commerce, an online storefront, built as two
independent lanes that run side by side, the way batch and streaming coexist in real
companies.

| Lane | Question it answers | Status |
|---|---|---|
| [Phase 1: Batch](phase1-batch/) | What happened yesterday? | Complete |
| Phase 2: Speed | What is happening right now? | In progress |

## Repository structure

```
atliq-commerce-capstone/
├── .github/workflows/    CI: dbt build + test on every pull request
├── phase1-batch/         Batch lane: nightly OLTP to lakehouse to Fabric
│   └── docs/             Milestone decks (PDF) and screenshots
└── phase2-streaming/     Speed lane: real-time order events (Phase 2)
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

Real-time order events stream through Kafka, are processed continuously by Databricks
Structured Streaming into the same Medallion pattern, and Airflow runs the scheduled
work around the stream. Standalone: it does not touch the Phase 1 deployment.

## Engineering notes

- **Secrets** are never committed. dbt reads its connection from environment variables;
  CI injects them from GitHub repository secrets; the nightly job reads its token from
  Azure Key Vault through a Databricks secret scope.
- **Idempotency** at every layer: watermark ingestion, Silver MERGE on the business key,
  and dbt rebuilding Gold from Silver. Re-running the chain never double-counts.
- **Build artifacts** (dbt target and packages, logs, virtual environments) are excluded
  via .gitignore.

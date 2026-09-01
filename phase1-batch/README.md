# AtliQ Commerce Data Engineering Capstone

End-to-end data engineering for AtliQ Commerce, an online storefront, built as two
independent lanes that run side by side, the way batch and streaming coexist in
real companies.

## Repository structure

```
atliq-commerce-capstone/
├── phase1-batch/        Batch lane: nightly OLTP to lakehouse to Fabric
└── phase2-streaming/    Speed lane: real-time order events (added in Phase 2)
```

## Phase 1: Batch Lane

The operational database (Azure SQL) syncs to an analytics lakehouse every night, so
leadership sees yesterday's numbers each morning. Built on the Medallion architecture.

```
Azure SQL (OLTP)  ->  ADF (metadata-driven ingest)  ->  Bronze (Parquet)
                                                         Silver (Delta, PySpark MERGE)
                                                         Gold   (star schema, dbt)
                                                    ->  Microsoft Fabric dashboard
```

Milestones:

1. **OLTP database** (Azure SQL): normalized 3NF storefront schema, seeded and verified.
2. **Ingestion to Bronze** (Azure Data Factory): one generic, metadata-driven pipeline
   driven by a control table; incremental loads via watermarks.
3. **Silver layer** (Databricks, PySpark): cleaned, conformed Delta tables; transactional
   tables loaded with idempotent MERGE upserts.
4. **Gold star schema** (dbt): fact_sales at order-item grain with customer, product, and
   date dimensions; data-quality tests.
5. **Nightly automation**: the full chain orchestrated and scheduled, proven idempotent.
6. **Reporting** (Microsoft Fabric): OneLake shortcut to Gold, Power BI dashboard.
7. **Reliability and CI/CD**: version control, automated tests, run auditing.

### Phase 1 folders

```
phase1-batch/
├── oltp/            M1: schema, seed data, ETL control table, audit log, verification
├── ingestion_adf/   M2: the metadata-driven ADF pipeline (JSON)
├── databricks/      M3: PySpark notebooks building the Silver layer
├── dbt/atliq_gold/  M4: dbt project building the Gold star schema
├── orchestration/   M5: nightly job definition
└── docs/            architecture notes
```

## Phase 2: Speed Lane (added later)

Real-time order events stream through Kafka, are processed continuously by Databricks
Structured Streaming into the same Medallion pattern, with Airflow running the scheduled
work around the stream. Standalone: it does not touch the Phase 1 deployment.

## Notes

- Credentials are never committed. Connection profiles use environment variables, and
  local secret files are excluded via .gitignore.
- Build artifacts (dbt target and packages, logs, virtual environments) are excluded.

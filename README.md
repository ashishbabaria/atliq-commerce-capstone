# AtliQ Commerce Data Engineering Capstone

End-to-end data engineering for AtliQ Commerce, an online storefront, built as two
independent lanes that run side by side, the way batch and streaming coexist in real
companies.

## Repository structure

```
atliq-commerce-capstone/
├── .github/workflows/    CI pipeline (dbt build + test on every pull request)
├── phase1-batch/         Batch lane: nightly OLTP to lakehouse to Fabric
└── phase2-streaming/     Speed lane: real-time order events (added in Phase 2)
```

## Phase 1: Batch Lane

The operational database syncs to an analytics lakehouse every night, so leadership
sees yesterday's numbers each morning. Built on the Medallion architecture, orchestrated
as one scheduled, idempotent nightly job, and reported through Power BI.

```
Azure SQL (OLTP)
   -> ADF pipeline (metadata-driven ingest, nightly trigger)
      -> Bronze  (Parquet, dated partitions)
      -> Silver  (Delta, PySpark MERGE)     [Databricks notebooks]
      -> Gold    (star schema, dbt)         [dbt on Databricks]
   -> Power BI dashboard on Gold            [Microsoft Fabric]
```

### Milestones

1. **OLTP database** (Azure SQL): normalized 3NF storefront schema, seeded and verified,
   with an ETL control table and an audit run-log.
2. **Ingestion to Bronze** (ADF): one metadata-driven pipeline driven by a control table;
   incremental loads via watermarks.
3. **Silver layer** (Databricks, PySpark): cleaned, conformed Delta tables; transactional
   tables loaded with idempotent MERGE upserts.
4. **Gold star schema** (dbt): fact_sales at order-item grain with customer, product, and
   date dimensions; sixteen data-quality tests.
5. **Nightly automation**: the full chain orchestrated by one ADF pipeline and scheduled,
   proven idempotent (two runs, identical fact totals).
6. **Reporting** (Microsoft Fabric): a Power BI dashboard on Gold, including a first-order
   cohort retention matrix, with the revenue definition stated on the report.
7. **Reliability and CI/CD**: the whole project in Git, a GitHub Actions CI that builds and
   tests dbt on every pull request into a throwaway schema, a failure alert on the nightly
   job, and an audit table that records each run's start, end, and status.

### Phase 1 folders

```
phase1-batch/
├── oltp/            M1: schema, seed data, ETL control table, audit run-log, verification
├── adf/             M2 and M5: the ADF pipeline, datasets, linked services, trigger (JSON)
├── databricks/      M3: PySpark notebooks (UC setup, Silver full, Silver incremental, run_dbt)
├── dbt/atliq_gold/  M4: dbt project building the Gold star schema
├── fabric/          M6: the Power BI report (.pbix)
└── orchestration/   notes on the end-to-end nightly wiring
```

## Phase 2: Speed Lane (added later)

Real-time order events stream through Kafka, are processed continuously by Databricks
Structured Streaming into the same Medallion pattern, with Airflow running the scheduled
work around the stream. Standalone: it does not touch the Phase 1 deployment.

## Engineering notes

- **Secrets** are never committed. dbt reads its connection from environment variables;
  CI injects them from GitHub repository secrets; the nightly job reads its token from
  Azure Key Vault via a Databricks secret scope.
- **Idempotency** is built in at every layer: watermark ingestion, Silver MERGE on the
  business key, and dbt rebuilding Gold from Silver, so re-running the chain never
  double-counts.
- **Build artifacts** (dbt target and packages, logs, virtual environments) are excluded
  via .gitignore.

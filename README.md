# Catalog & Orders Lakehouse on AWS

A batch + streaming lakehouse that ingests real Amazon catalog data and 100M
synthetic-but-realistic order events into an Iceberg-backed dimensional
model on S3, serves it through Athena, and guards it with a data-quality
and CloudWatch/SNS alerting layer.

**Grain of `fact_order_item`**: one row per order line item per status
transition — a line item that goes `created -> approved -> shipped ->
delivered` produces four fact rows, not one (see `docs/DATA_MODEL.md` for
why). **Why Iceberg over Delta/Hudi**: ACID MERGE for the SCD2 dimension
history, snapshot isolation so a failed job never corrupts the table, and
native support across Athena/Glue without a conversion step. **Three
hardest problems solved**: (1) reconstructing two years of SCD Type 2
history for `dim_product` from a price-change feed in one bulk pass, with a
separate, genuinely different two-statement MERGE pattern for incremental
runs (see `DATA_MODEL.md`); (2) getting a Spark streaming job's
idempotent-insert path working at all — `MERGE INTO` is flatly disallowed
inside a streaming micro-batch's execution thread in this Spark version,
discovered via the actual traceback, not assumed; (3) two independent AWS
services (EMR Serverless, Kinesis) failing with the identical
`SubscriptionRequiredException` on a brand-new account, requiring a
same-day architecture pivot to Glue Jobs and a file-source streaming
substitute — see `docs/RUNBOOK.md`'s incident log for the full story.

## Status: Weeks 1-5 core deliverables complete and validated (2026-08-26)

Real numbers, not projections — see `RESULTS.md`. 100,134,278 events
generated and landed, full star schema built with 0 orphans on the
point-in-time SCD2 join, streaming idempotency proven live (replay produces
0 new rows), 488 data-quality checks run with real findings, and backfill
idempotency proven live (two runs, identical row counts). Real spend so
far: ~$5.5-5.8 of the ~$127 planned budget across this project and its
sibling perf-teardown project.

## What changed from the original plan, and why

Documented honestly rather than silently substituted — every one of these
is in `docs/RUNBOOK.md`'s incident log with the actual error that forced
the decision:

| Originally planned | Actually used | Why |
|---|---|---|
| EMR Serverless | AWS Glue ETL Jobs | `SubscriptionRequiredException` on this account — Glue Jobs was the spec's own pre-approved alternative |
| Kinesis Data Streams | Spark Structured Streaming file source | Same exception, different service — two independent failures pointed at a real account limitation, not bad luck |
| dbt-core for transformations | PySpark directly in the Glue jobs | The SCD2 reconstruction and point-in-time dimension joins needed logic dbt's templating doesn't naturally express; Spark was already the engine for everything else |
| Great Expectations | Custom Athena + boto3 CloudWatch checks | Lighter weight for aggregate-query-shaped checks; avoided standing up a GE suite for 8 checks that are each one SQL query |
| MWAA | Local Airflow (Docker Compose) | MWAA is ~$350/month — would exhaust the entire Free Plan credit budget in three weeks |

## Repo layout

```
retail-lakehouse-aws/
├── docs/
│   ├── AWS_SETUP.md       ← Free Plan account setup — do this first
│   ├── RUNBOOK.md         ← incident log with every real bug hit and fixed
│   ├── DATA_MODEL.md      ← grain, SCD strategy, the two-statement MERGE pattern
│   └── COST.md            ← real spend, itemized, including failed attempts
├── infra/                 ← Terraform (S3, Glue Catalog/Jobs, Athena, CloudWatch/SNS)
├── generator/             ← seeded synthetic order-event generator (runs locally)
├── jobs/
│   ├── batch/             ← raw->staging, staging->curated, reconcile, backfill (Glue/PySpark)
│   └── streaming/         ← Spark Structured Streaming (file-source, not Kinesis)
├── dags/                  ← Airflow backfill DAG + docker-compose.yml
├── quality/               ← the 8-check DQ suite + fault-injection + simulation driver
└── RESULTS.md             ← real numbers from real runs
```

## Data sources

- **Catalog metadata**: [Amazon Reviews 2023](https://amazon-reviews-2023.github.io/) (McAuley Lab), `raw_meta_*` configs — real ASINs, titles, prices, category hierarchies (4M products, capped from ~13M raw across 3 categories).
- **Seller/customer/order-lifecycle shape**: [Olist Brazilian E-Commerce dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) (Kaggle) — real geography, fanned out to the target scale.
- **Order events**: synthetic, seeded generator (`generator/`) — no public dataset provides 100M order lines against real ASINs, so this is generated deliberately, with documented realism properties (Zipfian product skew, late arrivals, duplicates, dirty records, price drift, schema-evolution-like population-rate transition). See `generator/README.md`.

## Quick start

1. `docs/AWS_SETUP.md` — create the AWS Free Plan account, guardrails first.
2. `cd infra && terraform init && terraform apply`.
3. `generator/README.md` — generate the dataset, sync to S3.
4. `make crawl-raw` then run the Glue jobs in order: `raw-to-staging` -> `staging-to-curated`.
5. `quality/README.md` — run the DQ simulation.
6. `dags/README.md` — the backfill DAG, and how its idempotency was actually proven.

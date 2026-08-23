# Catalog & Orders Lakehouse on AWS

A batch + streaming lakehouse that ingests real Amazon catalog data and high-volume
order events into an Iceberg-backed dimensional model on S3, serves it through Athena
(with a bounded Redshift Serverless session for distribution/sort-key experiments), and
guards it with an automated data-quality and alerting layer.

**Grain of `fact_order_item`**: one row per order line item per status transition.
**Why Iceberg over Delta/Hudi**: ACID MERGE, time travel, and schema evolution on a
format natively supported by Athena, EMR, Redshift Spectrum and Glue — the same catalog
serves every engine without a conversion step.

**Status**: scaffolding only (2026-08-23). No AWS account created yet, no data landed,
no code written. See `docs/AWS_SETUP.md` for the account setup this project depends on,
and the project's execution memory for the week-by-week build plan.

## Repo layout

```
retail-lakehouse-aws/
├── docs/
│   ├── AWS_SETUP.md      ← do this first
│   ├── RUNBOOK.md
│   └── DATA_MODEL.md
├── infra/                ← Terraform
├── generator/            ← seeded synthetic order-event generator (runs locally, not on AWS)
├── jobs/
│   ├── batch/            ← PySpark, EMR Serverless
│   └── streaming/        ← Spark Structured Streaming, Kinesis
├── dbt/                  ← models + tests
├── dags/                 ← Airflow (local Docker)
├── quality/              ← Great Expectations suites
└── .github/workflows/
```

## Data sources

- **Catalog metadata**: [Amazon Reviews 2023](https://amazon-reviews-2023.github.io/) (McAuley Lab), `raw_meta_*` configs — real ASINs, titles, prices, category hierarchies.
- **Seller/customer/order-lifecycle shape**: [Olist Brazilian E-Commerce dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) (Kaggle).
- **Order events**: synthetic, seeded generator (`generator/`) — no public dataset provides 100M order lines against real ASINs, so this is generated deliberately, with documented realism properties (Zipfian product skew, late arrivals, duplicates, dirty records, price drift, schema evolution). See `generator/README.md` once written.

## Three hardest problems (fill in once solved)

1. TBD
2. TBD
3. TBD

Results, SLA attainment, and data-quality catch stats will live in `RESULTS.md` once the
pipeline is running against real data.

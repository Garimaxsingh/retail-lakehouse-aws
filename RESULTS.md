# Results

Real numbers from real runs against the full dataset — updated as weeks complete.
See `docs/COST.md` for the itemized spend log and `docs/RUNBOOK.md` for the
incident log behind some of these numbers.

## Dataset

| Metric | Value |
|---|---|
| Order-item events generated | 100,134,278 |
| Date range | 2024-01-01 to 2025-12-31 (735 days) |
| Products | 4,000,000 (capped from ~13M combined raw Amazon metadata) |
| Sellers | 50,000 (fanned out from 3,095 real Olist sellers) |
| Customers | 2,000,000 (fanned out from ~99K real Olist customers) |
| Raw data size | 24GB (96,289 files pre-compaction, 736 post-compaction — one per date partition) |

## Batch pipeline (Week 2)

| Table | Rows | Notes |
|---|---|---|
| `staging.order_events` (clean) | 98,637,863 | after dedup (-499,245, ~0.5%) and quarantine (-997,170, ~1.0%) |
| `staging.order_events_quarantine` | 997,170 | null_product_id / negative_quantity / unknown_seller, ~0.33% each |
| `curated.dim_date` | 735 | |
| `curated.dim_product` | 6,880,000 | 4,000,000 current + 2,880,000 historical SCD2 versions — exactly matches 4M products × 3%/month × 24 months |
| `curated.dim_seller` | 50,000 | single-version (no attribute-change feed — documented gap in `docs/DATA_MODEL.md`) |
| `curated.dim_customer` | 2,000,000 | |
| `curated.fact_order_item` | 98,637,863 | **0 orphans** on the point-in-time SCD2 join to dim_product — matches staging's clean count exactly |

Independently cross-validated via Athena (`count(*)` matches the Glue job's own
reported count exactly, at every stage).

## Streaming path (Week 3)

Real, live-sent test batches through the actual running Glue streaming job —
not simulated:

| Test | Result |
|---|---|
| On-time/late watermark split (500 events, 10% designed late-fraction) | 452 on-time / 48 late — matches design |
| **Idempotency replay** (same 500 events sent twice) | **0 genuinely new rows** on replay (`452 already present`) |
| Duplicate event_ids across the whole multi-restart debugging session | **0** (distinct count == total count: 1,352 == 1,352) |

This is the spec's "replay a shard from TRIM_HORIZON, row count unchanged"
proof, done for real (via a file-source substitution — see `docs/RUNBOOK.md`
for why Kinesis itself wasn't available on this account).

## Data quality suite (Week 4)

Simulated 61 daily pipeline runs against the historical dataset (real data
slices, not fabricated — see `quality/run_simulation.py` for the honest
methodology note on why this is a legitimate way to get a 60-run sample
without needing 60 calendar days).

| Metric | Value |
|---|---|
| Total checks run | 488 (61 days × 8 checks) |
| Checks flagged | 126 |
| Injected faults caught | **3/3 (100%)** — referential integrity, duplicate, consistency |
| Organic incidents caught | 123 |

**Organic incident breakdown**:
- **Null-rate completeness** (product_id): flagged on effectively every simulated
  day (~61/61) — real rate is ~0.33-0.35%, which exceeds the spec's tight 0.1%
  threshold. Honest finding: the generator's designed ~1% dirty-record rate
  (split three ways) means completeness never meets a threshold this strict.
  Not a pipeline bug — the pipeline correctly quarantines every one of these.
- **Freshness SLA** (99% of events within 5 min of event_ts): flagged on
  effectively every simulated day — actual attainment is consistently
  **~98.0-98.1%**, short of the spec's 99% target. Per the project's own
  guidance ("report the real number even if it's 94%"), this is the honest
  result rather than a fabricated pass.
- **Schema drift**: correctly caught exactly once, on 2025-01-05 — the real
  `promotion_code` population-rate transition (see the correction in
  `docs/RUNBOOK.md`: this is a values-populated transition, not a physical
  column-presence change as originally assumed).

CloudWatch alarm mechanism verified working end-to-end: `schema_drift_vs_previous`
genuinely transitioned to `ALARM` state after the metric push. SNS email
subscription created but not yet confirmed (pending in your inbox).

## Backfill idempotency (Week 5)

`jobs/batch/backfill_range.py` (parameterized by `--start_date`/`--end_date`,
uses Iceberg `overwritePartitions()` rather than the initial load's
`createOrReplace()`) run twice over the same range (2024-06-01..2024-06-07):

| Run | Row count | Distinct event_id |
|---|---|---|
| After run 1 | 840,146 | 840,146 |
| After run 2 | 840,146 | 840,146 |

**Identical — the spec's "run it twice, show identical row counts" proof,
executed for real.** See `docs/RUNBOOK.md` for an honest note on why this
count (840,146) differs slightly from the original full-table load's count
for the same date range (840,190) — a real ingest_date-vs-event_ts boundary
effect with late-arriving data, not an idempotency failure.

## Cost

Real spend across both projects so far: **~$5.3-5.6 of the ~$120 planned
budget** (see `docs/COST.md` for the full itemized log, including the ~$2.40
spent on failed attempts before fixes landed — genuine Frugality-story
material, not hidden).

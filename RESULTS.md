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

## Revenue-safe fact: `fact_order_line` (added 2026-10-21)

`fact_order_item` repeats each line item's money columns on every status
transition, so summing them overcounts (see `docs/DATA_MODEL.md`).
`jobs/batch/build_fact_order_line.py` builds an accumulating-snapshot fact
with one row per line item (Glue G.1X x4, 290 s):

| Metric | Value |
|---|---|
| Line items | 26,151,177 = 26,149,825 batch + 1,352 streamed test lines |
| Batch lines vs generated | 26,150,000 generated − 175 lines whose every event was quarantined |
| Lines missing a `created` row (that event was quarantined as dirty) | 261,639 (1.0%) |
| Lines currently `delivered` | 20,711,603 |
| Average `hours_to_deliver` | 96.0 (the generator's delivery template) |
| `SUM(net_amount)`, one row per line | 2,097,487,840.53 |
| Naive `SUM(net_amount)` over `fact_order_item` | 7,910,576,081.14 |
| **Overcount from summing the transition fact** | **3.771x** |

## Incremental SCD2 MERGE test (added 2026-10-21)

`jobs/batch/test_scd2_merge.py` applies one batch of price changes with
`jobs/batch/scd2.py` to a scratch Iceberg table with the real `dim_product`
schema, then re-applies it. **15/15 checks passed** on Glue: a two-change
version chain with gap-free validity windows, exactly one current row per
product, carried title/category, recomputed price_band, unique keys, a
no-op change dropped, an unknown product ignored, and the second
application changing nothing. The first attempt failed with `MERGE INTO
TABLE is not supported temporarily` — Iceberg's Spark SQL extensions had
never been enabled on the Glue jobs (see `docs/RUNBOOK.md`).

## Backfill idempotency and blast radius (Week 5, re-done 2026-10-21)

`jobs/batch/backfill_range.py` reprocesses an event-date range: it reads raw
`ingest_date` from start to end + 1 day, keeps rows whose event date is in
range, and writes with Iceberg overwrite-by-condition on that `event_ts`
range (see `docs/RUNBOOK.md` for why the first version did not).

**The first version (2026-08-26) passed its idempotency test but was not
safe**: two runs gave identical counts (840,146) for 2024-06-01..06-07, yet
they had overwritten 2024-05-31 down to 41 rows (from 119,798), replaced the
whole quarantine table (997,170 -> 8,593 rows), and missed 44 late events
from 06-07. The test only looked inside the range. Found on 2026-10-21,
fixed, and the staging zone repaired by re-running `raw_to_staging` once.

**Fixed version, run twice over 2024-06-01..2024-06-07 (2026-10-21)**,
checking the range *and* the days on either side after each run against
`curated.fact_order_item` (built before any backfill, so an independent
reference):

| Check | Run 1 | Run 2 |
|---|---|---|
| Clean rows written for the range | 840,190 | 840,190 |
| Quarantined rows written for the range | 8,592 | 8,592 |
| `staging.order_events` total | 98,637,863 | 98,637,863 |
| `staging.order_events_quarantine` total | 997,170 | 997,170 |
| Days 2024-05-29 .. 2024-06-10 matching the reference, per day | 13 / 13 | 13 / 13 |

840,190 is exactly the full load's count for that event-date range — the
late events the old version dropped are now included — and nothing outside
the range changed.

## Cost

Real spend across both projects so far: **~$5.3-5.6 of the ~$120 planned
budget** (see `docs/COST.md` for the full itemized log, including the ~$2.40
spent on failed attempts before fixes landed — genuine Frugality-story
material, not hidden).

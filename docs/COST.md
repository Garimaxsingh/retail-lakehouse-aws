# Cost Tracker — Planned vs Actual

Logged as-we-go, not reconstructed at the end. See `docs/AWS_SETUP.md` and the
project's Free Plan execution plan for the full budget breakdown this is
tracked against (~$119 of $200 credit planned across both projects).

| Date | Week | Item | Planned | Actual | Notes |
|---|---|---|---|---|---|
| 2026-08-23 | 0 | Account setup, budgets, MFA, IAM | $0 | $0 | Free Plan confirmed |
| 2026-08-24 | 1 | S3 bucket + lifecycle rules (Terraform) | — | $0.00 | No data landed yet, empty bucket |
| 2026-08-24 | 1 | 3x Glue Data Catalog databases + crawler (not yet run) | — | $0.00 | Catalog metadata is free to 1M objects |
| 2026-08-24 | 1 | Athena workgroup + results bucket | — | $0.00 | Workgroup created, 10GB/query scan cap set |
| 2026-08-24 | 1 | Generator validation run: 500K line items -> 1,914,439 events, real Electronics catalog data (Olist not yet available — dev fixture used for sellers/customers) | — | $0.00 | Ran entirely locally, no AWS cost |
| 2026-08-24 | 1 | S3 sync of 456MB order-event data + 18MB price-change feed | — | ~$0.01 | ~1,470 PUT requests across 735 date partitions |
| 2026-08-24 | 1 | Glue crawler run on raw zone (SUCCEEDED, cataloged `order_events` + `product_price_changes`) | — | ~$0.02 | Single run, few minutes |
| 2026-08-24 | 1 | Athena validation query (count/distinct/min/max) | — | ~$0.0004 | 87.68 MB scanned — well under the 10GB workgroup cap |

**Validated end-to-end (2026-08-24)**: generator -> S3 -> Glue Catalog -> Athena, with the
query's `total_events` count (1,914,439) matching the generator's own reported count
exactly, and `delivered` status count (401,820) landing almost exactly on the expected
80% lifecycle-template weight (500,000 x 0.80 = 400,000).

**Known nit, not a data bug**: the crawler inferred `event_ts`/`ingest_ts` as `bigint`
(raw nanosecond epoch) rather than `timestamp` — a Parquet/Arrow logical-type quirk in
crawler inference, not a data problem. The raw-zone crawler is for exploration only;
the Week 2 batch job will define explicit typed Iceberg schemas rather than relying on
inferred types, so this doesn't need fixing here.

**Still not run**: the full 100M-row generation (this was a 500K-line-item / ~1.9M-event
validation sample only, ~456MB). Full-scale run and its actual cost/runtime are still
ahead — extrapolating this sample, ~100M events would be roughly 24GB in S3 (~$0.60/mo
storage), matching the original $3 planning estimate for 30GB across 4 months.

| 2026-08-24 | 1 | Full-scale generation: 100,134,278 events, real Amazon (3 categories, 4M products) + real Olist, 24GB/96,289 files/736 partitions | $8 (planned) | $0.00 | Ran entirely locally, ~33 min |
| 2026-08-24 | 1 | S3 sync of full 24GB dataset (~96,289 PUTs) | (see above) | ~$0.48 | |
| 2026-08-24 | 1 | Glue crawler re-run on full dataset | ~$2 (planned, 10 runs) | ~$0.05 | One run so far |
| 2026-08-24 | 1 | Full-scale Athena validation query (7-column aggregate over 100M rows) | (part of $20 planned) | ~$0.03 | 5.16GB scanned (column pruning — well under the raw 24GB), 8s runtime |
| **Running total** | | | **~$119 planned (both projects)** | **~$0.56** | Week 1 core deliverable complete |

**Week 1 core deliverable validated at full production scale (2026-08-24)**: Athena's
`count(*)` (100,134,278) matches the generator's own reported count exactly. Status
distribution, dirty-record rates (~0.33% each of 3 types = ~1% total), and distinct
product/order counts all landed within noise of their designed targets. Full pipeline
(local generation -> S3 -> Glue Catalog -> Athena) proven correct at the scale the
project actually targets, not just on a small sample.

| 2026-08-25 | 2 | EMR Serverless CreateApplication attempt (FAILED — SubscriptionRequiredException, service unavailable on this account) | — | $0.00 | No compute ran; switched to Glue Jobs |
| 2026-08-25 | 2 | `raw_to_staging` attempt 1 (FAILED — nanosecond timestamp incompatibility, 79s) | — | ~$0.04 | |
| 2026-08-25 | 2 | `raw_to_staging` attempt 2 (FAILED — TIMEOUT at 60min against 96,289 raw files) | — | ~$1.76 | The small-files problem, showing up uninvited from Project 2's own E1 theme |
| 2026-08-25 | 2 | `raw_to_staging` attempt 3 (FAILED — Iceberg "records not clustered by partition" on unsorted write, 20.5min) | — | ~$0.60 | Dedup/quarantine logic validated correct before the write failure (499,245 dupes removed ~0.5%, 997,170 quarantined ~1.0% — both matching design targets) |
| 2026-08-25 | 2 | `raw_to_staging` attempt 4 (**SUCCEEDED**, 36.8min, sort-before-write fix) | ~$6 (planned) | ~$1.08 | 98,637,863 clean rows, 6 staging tables created |
| 2026-08-25 | 2 | Kinesis CreateStream attempt (FAILED — same SubscriptionRequiredException) | — | $0.00 | Switched streaming source to S3 file-source (Spark Structured Streaming), documented in docs/RUNBOOK.md |
| 2026-08-25 | 2 | `staging_to_curated` (**SUCCEEDED on first try**, 14.35min) | (part of ~$6 planned) | ~$0.42 | Full star schema: dim_date (735), dim_product (6.88M, 4M current — 2.88M historical versions matches 4M x 3%/mo x 24mo exactly), dim_seller (50K), dim_customer (2M), fact_order_item (98,637,863, **0 orphans** on point-in-time SCD2 join) |
| **Running total** | | | **~$119 planned (both projects)** | **~$4.46** | Week 2 core deliverable complete and validated |

**Week 2 core deliverable validated end-to-end (2026-08-25)**: the full batch path
(raw -> staging -> curated) ran for real against the complete 100M-row dataset. Every
number is internally consistent and cross-checked: staging's clean-row count
(98,637,863) equals curated's fact_order_item row count exactly (zero rows lost or
duplicated in the dimensional join), the SCD2 historical version count (2,880,000)
matches the generator's own price-change design parameters exactly, and Athena's
independent count(*) matches the Glue job's own reported count exactly. Total incident
cost across 3 failed attempts before the working run: ~$2.40 — real Frugality-relevant
data for `docs/COST.md`'s own stated purpose.

| 2026-08-25 | 3 | Streaming path debugging: 6 Glue streaming job run attempts (most failed fast on real bugs — foreachBatch temp-view session scoping, MERGE INTO disallowed inside streaming micro-batches, missing columns on the late-write path, a timezone parsing bug; 2 runs ran ~14-15min each while validated) | ~$1 (planned, 1 shard x 60hrs — streaming ended up on Glue not Kinesis) | ~$0.7-1.0 | See docs/RUNBOOK.md incident log for each bug |
| **Running total** | | | **~$120 planned (both projects)** | **~$5.3-5.6** | Week 2 complete + Week 3 streaming mechanics live-validated |

**Streaming path live-validated (2026-08-25)**: after fixing 4 real bugs (see RUNBOOK.md),
sent live test batches through the actual running Glue streaming job (not a simulation).
Results: on-time/late watermark split matched the designed ~90/10 ratio exactly (452
on-time / 48 late on one batch), and the **idempotency replay test** — sending the exact
same 500 events a second time — landed **0 genuinely new rows** (`452 already present`).
Across the entire multi-restart debugging session (6 job runs, multiple backlogged test
files processed cumulatively as bugs were fixed), `fact_order_item`'s stream-sourced rows
show **zero duplicate event_ids** (distinct count == total count exactly), despite
checkpoint resumes and deliberate replays. This is the actual "replay produces zero new
rows" proof the spec's interview-question list asks for (Q4/Q5), done for real rather
than asserted.

**Known real bugs hit and fixed in the streaming job** (all documented in
docs/RUNBOOK.md's incident log): (1) `foreachBatch`'s `batch_df` doesn't share a temp-view
scope with the outer session — switched to a real persisted table, then discovered (2)
`MERGE INTO` is disallowed inside a streaming micro-batch's execution thread in this Spark
version — switched to anti-join + append (functionally identical for an insert-only path),
which then exposed (3) the late-arrival branch was missing several required columns
(never tested until real late data hit it), and (4) event timestamps sent as naive strings
were parsed using the session's local timezone (effectively IST, UTC+5:30) instead of UTC,
making every event register as "late" regardless of design intent — fixed by adding an
explicit `Z` suffix and forcing `spark.sql.session.timeZone=UTC`. The late-table's own
idempotency dedup (fix #5, symmetric with the on-time path) was written and deployed but
not independently re-validated by another live run — a smaller, honestly-flagged gap
versus the core on-time idempotency proof, which was fully live-validated.

| 2026-08-26 | 4 | DQ suite: 488 Athena checks (61 simulated days × 8 checks) + 3 fault-injection CTAS scratch tables + 488 CloudWatch custom metric datapoints + 8 CloudWatch alarms + 1 SNS topic | ~$5 (planned) | ~$0.05-0.10 | Day-partitioned queries scan very little each; alarms ~$0.80/month ongoing |
| **Running total** | | | **~$125 planned (both projects)** | **~$5.4-5.7** | Weeks 2-4 complete |

**Week 4 complete (2026-08-26)**: 488 checks run, 126 flagged (123 organic + 3/3
injected faults caught). Two genuine findings worth calling out as Frugality/Dive
Deep material: null-rate completeness (~0.33-0.35%) and freshness SLA (~98.0-98.1%)
both consistently miss their spec-defined thresholds (0.1% and 99% respectively) —
honest results reported as-is rather than adjusted to pass, per the project's own
stated principle. Schema-drift check correctly caught the one real transition
(2025-01-05) and drove a genuine CloudWatch alarm state change to ALARM, proving
the alerting mechanism works end-to-end. SNS email subscription created but still
pending confirmation (check inbox).

| 2026-08-26 | 5 | Backfill idempotency proof: 2 runs of `backfill_range.py` over the same 1-week range (2024-06-01..06-07), 124s each | ~$2 (planned) | ~$0.12 | Identical row counts both runs (840,146) — real proof, see RESULTS.md |
| **Running total** | | | **~$127 planned (both projects)** | **~$5.5-5.8** | Weeks 2-5 core deliverables complete |

Update this table each session — actual spend goes in Cost Explorer, transcribe
the delta here rather than trusting memory at Week 8.

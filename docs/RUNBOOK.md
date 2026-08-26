# Runbook

## Stream lag exceeds 10 minutes

1. Check the Glue streaming job's CloudWatch metrics (`--enable-metrics` is
   on) for `glue.driver.streaming.lastCompletedBatchProcessingTimeMs` vs the
   1-minute trigger interval — if batches are taking longer than the
   trigger period, they queue up and lag grows monotonically.
2. Check Kinesis `IteratorAgeMilliseconds` (CloudWatch, per-shard) — this is
   the actual "how far behind is the consumer" metric, independent of Spark
   internals.
3. **Shard scaling decision**: this project runs 1 shard by design (budget).
   If sustained throughput genuinely exceeds 1 shard's capacity (1MB/s in,
   2MB/s out), `aws kinesis update-shard-count` to reshard — but check
   whether the real cause is slow *processing* (undersized Glue workers)
   before assuming it's an *ingestion* bottleneck; resharding doesn't fix
   the former.
4. If it's a processing bottleneck: `jobs/streaming/stream_order_events.py`
   runs 2 G.1X workers — bump `number_of_workers` in `infra/glue_jobs.tf`
   before reaching for a shard split.

## Upstream schema change detected

Iceberg tables in this project evolve without a full rewrite via `ALTER
TABLE ... ADD COLUMN`, not by dropping and recreating:

```sql
ALTER TABLE glue_catalog.staging_db.order_events ADD COLUMN promotion_code STRING;
```

New columns are nullable by default, so historical rows read back as NULL
rather than failing.

**Correction (2026-08-26, found while building Week 4's schema-drift check)**:
this doc originally claimed the generator's `promotion_code` cutover
(`generator/order_events.py --schema-evolution-date`) produces genuine
physical schema evolution — files before the cutover "lacking the column."
That's not what the generator actually does: `events["promotion_code"] =
np.where(post_cutover, promo_codes, None)` runs unconditionally, so every
row gets the column, populated only after the cutover date. Checking a
pre-cutover raw file directly (`quality/checks.py`'s `schema_drift_check`)
confirmed `promotion_code` is present with all-null values well before
2025-01-05 — there is no physical column-presence difference to detect.
This is still a legitimate, common real pattern (a field ships in code
before it's ever populated), just not the specific "ALTER TABLE ADD COLUMN"
scenario originally described. The DQ check was adjusted to detect the
*population-rate* transition (0% -> >0% populated) instead of column
presence, which the actual data does show a real, detectable jump at.
The `ALTER TABLE ... ADD COLUMN` mechanism above is still the correct
answer for a *genuine* future schema change to this pipeline — it just
isn't what's organically demonstrated in this dataset. The failure mode to
avoid regardless is a *type* change (e.g. `quantity` INT -> STRING) — not a
safe in-place evolution in Iceberg either, and needs the same
expand-contract pattern DataPlan (the sibling schema-migration project) is
built to detect: add a new column, backfill, cut over readers, drop the old one.

## Backfill boundary effect: ingest_date vs event_ts (found 2026-08-26)

Running `jobs/batch/backfill_range.py` for 2024-06-01..2024-06-07 twice
produced identical counts both times (840,146 — the actual idempotency
proof, see RESULTS.md), but that number is 44 rows *less* than what
`date(event_ts) BETWEEN ...` returns from the original full-table load
(840,190). Real, understood cause: the backfill job filters the **raw**
zone by `ingest_date` (matching the S3 partition layout), but late-arriving
events can have `ingest_ts` — and therefore `ingest_date` — up to 90
minutes after `event_ts`, which crosses midnight for events near the end of
a day. A handful of 2024-06-07 `event_ts` rows land in the
`ingest_date=2024-06-08` partition and are outside the backfill's ingest-date
filter, while the original full-table run (no date filter at all) included
them. This is a genuine, common backfill subtlety — ingestion-time
partitioning never perfectly aligns with event-time boundaries once late
data exists — not a bug in the idempotency logic itself. If a future
backfill needs event-time-exact reprocessing, filter on a slightly widened
ingest_date window (e.g. end_date + 1) rather than trusting ingest_date to
equal event_ts's date.

## Backfill fails at 60%

Iceberg's snapshot isolation means a failed `createOrReplace()` job run
**does not corrupt the table** — the table still points at its last-good
snapshot until the write actually commits. Recovery:

1. `SELECT * FROM glue_catalog.curated_db.fact_order_item.snapshots` to
   confirm the last committed snapshot is still the pre-backfill one.
2. Fix whatever caused the failure (this project's own first real instance:
   the 96K-small-files timeout in Week 2 — see `docs/COST.md` and
   `generator/compact_raw_files.py` for the actual incident and fix).
3. Re-run the job. Because every batch/curated write in this project uses
   `createOrReplace()` (full rebuild) or a `MERGE ... ON event_id` (natural
   idempotency key), simply re-running is safe — there's no "partial
   write" state to clean up manually.
4. If a genuinely partial write did commit (rare, but possible with
   `.append()` on `fact_order_item_late`), roll back via
   `CALL glue_catalog.system.rollback_to_snapshot('curated_db.fact_order_item_late', <snapshot_id>)`
   rather than truncating and re-running from scratch.

## Duplicate event_id spike

1. First check: is this the *expected* 0.5% synthetic duplicate rate from
   the generator (`generator/order_events.py`), or a genuine new source of
   duplication (e.g. a Kinesis producer retry storm, or a batch job re-run
   without proper MERGE semantics)?
2. `SELECT event_id, count(*) FROM curated_db.fact_order_item GROUP BY event_id HAVING count(*) > 1`
   should return **zero rows** in the curated layer at all times — MERGE on
   `event_id` is supposed to make this structurally impossible downstream
   of staging. If it's non-zero, the bug is in whichever job's MERGE/dedup
   logic just ran, not in the source data.
3. In `staging.order_events` (post-dedup, pre-MERGE), duplicates are
   expected and by design routed to a single row via
   `raw_to_staging.py`'s "earliest ingest_ts wins" dedup — check there
   first before assuming something is broken.

## Known incident log

- **2026-08-25**: `raw_to_staging` Glue job hit `TIMEOUT` after 60 minutes
  against the raw zone's 96,289-file layout (fitting, since this is
  literally what the sibling `data-perf-teardown` project's E1 experiment
  is designed to demonstrate — it just showed up here first). Fixed via
  `generator/compact_raw_files.py`, consolidating to 736 files (one per
  date partition). Real cost from the failed run: ~$1.76 (4 DPU x 1hr).
- **2026-08-25**: Same job failed earlier with
  `Illegal Parquet type: INT64 (TIMESTAMP(NANOS,false))` — `ingest_ts` was
  written at nanosecond Parquet precision (pandas `pd.to_timedelta`
  default), which Spark's Parquet reader rejects. Fixed both the existing
  files (`generator/fix_ingest_ts_precision.py`) and the generator source
  (explicit `.astype("datetime64[us]")` at creation).
- **2026-08-25**: `terraform apply` failed creating both
  `aws_emrserverless_application` and (separately) `aws_kinesis_stream`
  with the identical `SubscriptionRequiredException` — two different
  service families on the same new account, which points at a real
  platform limitation rather than random new-account activation lag.
  Compute moved to AWS Glue ETL Jobs (pre-approved alternative in the
  project spec). Streaming moved from Kinesis to Spark Structured
  Streaming's **file source** — producers write JSON files to
  `s3://bucket/stream_landing/order_events/` instead of calling
  `kinesis:PutRecords`; `jobs/streaming/stream_order_events.py` polls that
  prefix the same way it would poll a shard. Every downstream mechanic
  (watermark, idempotent MERGE on `event_id`, late-row routing) is
  unchanged — only the ingestion transport differs. Documented here rather
  than silently substituted, since "real Kinesis streaming" was explicitly
  called out as a "don't cut" item in the original spec.
- **2026-08-25**: four real bugs surfaced getting the streaming job to actually
  run, in order:
  1. `AnalysisException: Table or view not found: on_time_batch_0` —
     `foreachBatch`'s `batch_df` carries its own `SparkSession` instance in
     this Glue runtime; a temp view registered via `createOrReplaceTempView`
     wasn't visible to the outer module-level `spark.sql(...)`, even after
     switching to `batch_df.sparkSession.sql(...)` explicitly. Fixed by
     writing the micro-batch to a real, small, overwritten-each-time Iceberg
     table instead — catalog tables resolve via the shared Glue metastore
     regardless of which session instance is asking; temp views don't.
  2. `UnsupportedOperationException: MERGE INTO TABLE is not supported
     temporarily` — a genuine Spark 3.3 restriction (this Glue version):
     `MERGE INTO` cannot run from inside a streaming micro-batch's execution
     thread at all, independent of bug #1. Since the streaming path only
     ever inserts (never updates an existing fact row), switched to an
     anti-join against existing `event_id`s + plain `.append()` —
     functionally identical to `WHEN NOT MATCHED THEN INSERT` for an
     insert-only path, and this is what actually produced the idempotency
     proof in `docs/COST.md`.
  3. `AnalysisException: Cannot find data for output column 'order_item_key'`
     (and every other derived column) — the late-arrival branch was writing
     `batch_df` almost as-is (just `+source_system`) instead of going through
     the same column-shaping the on-time branch did, so it silently worked
     right up until a real late row actually reached that code path. Fixed
     by extracting a shared `shape()` helper both branches call, specifically
     so the two paths can't drift out of sync with each other again.
  4. Every event registered as late regardless of its intended on-time/late
     split — `event_ts` was sent as a naive timestamp string (no zone
     marker), and Spark's `TimestampType` cast parsed it using the session's
     local timezone (effectively IST, UTC+5:30) instead of UTC, shifting
     every value ~5.5 hours into the past. Fixed on both sides: the producer
     now appends an explicit `Z` suffix, and the job explicitly sets
     `spark.conf.set("spark.sql.session.timeZone", "UTC")` rather than
     assuming the default. Also switched the late/on-time split's `cutoff`
     from `F.current_timestamp()` (non-deterministic, re-resolved per action)
     to one frozen Python-side value per micro-batch, with an assertion that
     `on_time + late == total` as a standing correctness check.

  After all four fixes: live-sent test batches split on-time/late at the
  designed ~90/10 ratio, and replaying the exact same 500 events a second
  time landed **0 genuinely new rows** — the real idempotency proof, not a
  simulated one. See `docs/COST.md` for the full numbers.

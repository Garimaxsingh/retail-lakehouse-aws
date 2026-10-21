# Runbook

## Stream lag exceeds 10 minutes

The stream source is an S3 landing prefix (Kinesis was unavailable on this
account — see the incident log), so "lag" means files waiting to be picked up.

1. Check the Glue streaming job's CloudWatch metrics (`--enable-metrics` is
   on) for `glue.driver.streaming.lastCompletedBatchProcessingTimeMs` vs the
   1-minute trigger interval — if batches take longer than the trigger
   period, they queue up and lag grows monotonically.
2. Count files in `s3://<bucket>/stream_landing/order_events/` newer than the
   last processed batch (the checkpoint under `checkpoints/stream_order_events/`
   records what was processed). A growing backlog with fast batches means
   `maxFilesPerTrigger` (50) is the limit — raise it.
3. If batches are slow: the idempotency anti-join reads every `event_id` in
   `fact_order_item` each micro-batch, which grows with the table. Restrict
   it to the event dates present in the batch, or bump `number_of_workers`
   in `infra/glue_jobs.tf`.

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

## Backfill overwrote data outside its range (found 2026-10-21, fixed)

**What happened**: `backfill_range.py` was run twice for 2024-06-01..06-07
on 2026-08-26 and produced identical counts both times (840,146) — so it
looked idempotent. A later review of the code raised two suspicions, and
read-only Athena queries on 2026-10-21 confirmed both in the live tables:

| Table | Expected | Actual |
|---|---|---|
| `staging.order_events`, event date 2024-05-31 | 119,798 | **41** |
| `staging.order_events`, event date 2024-06-07 | 119,924 | 119,880 |
| `staging.order_events_quarantine`, whole table | 997,170 | **8,593** |

**Causes**:
1. The job selected raw rows by `ingest_date`, but `order_events` is
   partitioned by `days(event_ts)`. Late events from 2024-05-31 ingested on
   06-01 were in the input, so `overwritePartitions()` treated 05-31 as a
   partition "present in the incoming data" and replaced it with just those
   41 rows. Conversely, 44 events from 06-07 ingested on 06-08 were left out
   (the 840,146 vs 840,190 gap, originally written up here as a harmless
   boundary effect).
2. `order_events_quarantine` was unpartitioned, so dynamic partition
   overwrite replaced the whole table.

The idempotency test only compared counts *inside* the requested range, so
it couldn't see damage outside it. `curated.fact_order_item` was built
before the backfill runs and was not affected.

**Fix**:
- Read `ingest_date` from start to end + 1 day (max lateness), keep rows by
  *event* date in range, and write with Iceberg `overwrite(condition)` on the
  `event_ts` range instead of `overwritePartitions()` — replaces exactly the
  range, including days whose new data has no rows.
- Partition `order_events_quarantine` by `days(event_ts)` (`raw_to_staging.py`).
- Guard in the job: refuse to write if any row's event date is outside the range.
- Repair: re-ran `raw_to_staging` once (full rebuild from raw), then re-ran
  the fixed backfill twice and checked the days on both sides of the range.
  Numbers in RESULTS.md.

**Lesson**: an idempotency test must check the blast radius — the
neighbouring partitions and every table the job writes — not only the rows
it meant to touch.

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
     temporarily` — originally diagnosed as a Spark 3.3 restriction on
     `MERGE INTO` inside a streaming micro-batch. **Correction
     (2026-10-21)**: the same error then appeared in a plain *batch* job
     (`test_scd2_merge.py`), and disappeared once the Glue jobs set
     `spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions`.
     Spark only supports `MERGE INTO` on Iceberg tables through those
     extensions, so the real cause was missing configuration — not
     streaming. All Iceberg jobs now set it (`infra/glue_jobs.tf`). The
     streaming job keeps its anti-join (still correct and idempotent); MERGE
     inside `foreachBatch` has not been re-tested with the extensions on. Since the streaming path only
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

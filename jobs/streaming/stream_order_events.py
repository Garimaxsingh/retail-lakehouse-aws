"""S3-landing -> curated.fact_order_item, via Spark Structured Streaming on
a Glue Streaming ETL job (gluestreaming).

**Source substitution, documented honestly**: the spec calls for Kinesis
Data Streams. Kinesis hit the same SubscriptionRequiredException as EMR
Serverless on this AWS Free Plan account (see docs/COST.md and
docs/RUNBOOK.md's incident log) — not something fixable from here. This
uses Spark Structured Streaming's **file source** instead: producers write
small JSON files to s3://bucket/stream_landing/order_events/, and this job
treats that growing S3 prefix as the stream, via directory polling +
Spark's own file-tracking checkpoint (a real, standard Structured Streaming
source — see generator/stream_producer.py for the producer side).

Every downstream mechanic this job exists to prove is unchanged by the
substitution:
  1. Idempotency: an anti-join against existing event_ids + append, keyed
     on event_id — reprocessing the same landed files (by resending the
     same event_ids in a new file, or clearing the checkpoint) must not
     change the row count. This is the file-source equivalent of "replay a
     Kinesis shard from TRIM_HORIZON." (Not a MERGE — Spark 3.3 disallows
     MERGE INTO from inside a streaming micro-batch's execution thread; an
     anti-join + append is functionally identical for an insert-only path.
     See the comment in process_batch for the actual traceback that ruled
     MERGE out.)
  2. Late data: 15-minute watermark. Events arriving later than that go to
     curated.fact_order_item_late, never silently dropped.
  3. Batch/stream reconciliation: unchanged — see jobs/batch/reconcile.py,
     which runs as a plain batch job (not inside foreachBatch) and so can
     use real MERGE INTO to fix the NULL surrogate keys these rows land with.

Runs continuously while active — bills for the whole time it's running,
unlike the batch jobs. Stop it deliberately (`aws glue stop-job-run`) or
via `make teardown`.
"""
import sys
from datetime import datetime, timedelta, timezone

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql import types as T

args = getResolvedOptions(sys.argv, ["JOB_NAME", "bucket", "curated_database"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
# Explicit, not assumed — naive timestamp strings (no zone marker) parse
# using this setting, and the default isn't guaranteed to be UTC.
spark.conf.set("spark.sql.session.timeZone", "UTC")
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

CATALOG = "glue_catalog"
CURATED_DB = args["curated_database"]
BUCKET = args["bucket"]
STREAM_LANDING_PATH = f"s3://{BUCKET}/stream_landing/order_events/"
CHECKPOINT_LOCATION = f"s3://{BUCKET}/checkpoints/stream_order_events/"
WATERMARK = "15 minutes"


def curated(table: str) -> str:
    return f"{CATALOG}.{CURATED_DB}.{table}"


EVENT_SCHEMA = T.StructType([
    T.StructField("event_id", T.StringType()),
    T.StructField("order_id", T.StringType()),
    T.StructField("order_line_id", T.IntegerType()),
    T.StructField("customer_id", T.StringType()),
    T.StructField("product_id", T.StringType()),
    T.StructField("seller_id", T.StringType()),
    T.StructField("quantity", T.IntegerType()),
    T.StructField("unit_price", T.DoubleType()),
    T.StructField("discount_amount", T.DoubleType()),
    T.StructField("order_status", T.StringType()),
    T.StructField("event_ts", T.StringType()),  # ISO string over the wire, cast below
])

raw_stream = (
    spark.readStream.format("json")
    .schema(EVENT_SCHEMA)
    .option("maxFilesPerTrigger", 50)
    .load(STREAM_LANDING_PATH)
)

parsed = (
    raw_stream
    .withColumn("event_ts", F.col("event_ts").cast(T.TimestampType()))
    .withColumn("gross_amount", F.round(F.col("quantity") * F.col("unit_price"), 2))
    .withColumn("net_amount", F.round(F.col("gross_amount") - F.col("discount_amount"), 2))
    .withColumn("ingest_ts", F.current_timestamp())
    .withWatermark("event_ts", WATERMARK)
)


def process_batch(batch_df, batch_id: int) -> None:
    batch_df.persist()
    try:
        # Late = event_ts older than the watermark relative to when we're
        # actually processing it. Route out, never drop silently.
        #
        # Frozen to a single Python-side value rather than F.current_timestamp()
        # — that's non-deterministic per Catalyst and gets re-resolved on
        # EACH separate action (on_time.count(), late.count(), the write),
        # so on_time/late could silently stop being exact complements of
        # batch_df across those calls. A fixed cutoff guarantees they are.
        cutoff_dt = datetime.now(timezone.utc) - timedelta(minutes=15)
        cutoff = F.lit(cutoff_dt)
        on_time = batch_df.where(F.col("event_ts") >= cutoff)
        late = batch_df.where(F.col("event_ts") < cutoff)
        total = batch_df.count()
        print(f"[stream batch {batch_id}] {total:,} total rows in this micro-batch, cutoff={cutoff_dt.isoformat()}")

        # Surrogate keys are NULL on the stream-inserted row deliberately —
        # the speed layer prioritizes low-latency, idempotent landing over
        # doing point-in-time dimension joins per micro-batch. The nightly
        # batch job re-processes T-2 days and MERGEs the same event_id,
        # which is where product_sk/seller_sk/customer_sk/date_key actually
        # get resolved correctly. See jobs/batch/reconcile.py.
        #
        # NOTE: deliberately NOT using MERGE INTO here — Spark 3.3 (this
        # Glue version) raises "MERGE INTO TABLE is not supported
        # temporarily" when called from inside a streaming micro-batch's
        # execution thread (a Spark-side restriction, not an Iceberg one;
        # confirmed via the actual Py4J traceback, not assumed). Since the
        # streaming path only ever INSERTs new rows (never updates an
        # existing one), an anti-join against event_id + plain append is
        # functionally identical to `WHEN NOT MATCHED THEN INSERT` and
        # avoids the restriction entirely — this IS how idempotent-insert
        # streaming upserts are commonly done when native MERGE isn't
        # available inside foreachBatch.
        def shape(df, source_system: str):
            # Both sinks (fact_order_item, fact_order_item_late) share this
            # exact column shape — a prior bug had `late` skip this and go
            # straight to .append() with only source_system added, which
            # failed with "Cannot find data for output column 'order_item_key'"
            # (and every other derived column) the first time a real late
            # row hit that path. Shared helper so the two branches can't
            # drift out of sync with each other again.
            return df.select(
                F.md5(F.concat_ws("|", F.col("order_id"), F.col("order_line_id").cast("string"), F.col("order_status"))).alias("order_item_key"),
                F.col("event_id"), F.col("order_id"), F.col("order_line_id"),
                F.lit(None).cast(T.LongType()).alias("product_sk"),
                F.lit(None).cast(T.LongType()).alias("seller_sk"),
                F.lit(None).cast(T.LongType()).alias("customer_sk"),
                F.lit(None).cast(T.IntegerType()).alias("date_key"),
                F.col("order_status"), F.col("quantity"), F.col("unit_price"),
                F.col("gross_amount"), F.col("discount_amount"), F.col("net_amount"),
                F.col("event_ts"), F.col("ingest_ts"),
                F.lit(source_system).alias("source_system"),
            )

        on_time_count = on_time.count()
        if on_time_count > 0:
            prepared = shape(on_time, "stream")
            existing_ids = spark.table(curated("fact_order_item")).select("event_id")
            new_rows = prepared.join(existing_ids, "event_id", "left_anti")
            new_count = new_rows.count()
            new_rows.writeTo(curated("fact_order_item")).append()
            print(f"[stream batch {batch_id}] {on_time_count:,} on-time rows, {new_count:,} genuinely new (idempotency check: {on_time_count - new_count:,} already present)")

        late_count = late.count()
        if late_count > 0:
            # Same idempotency treatment as the on-time path — a replay
            # (or the batch reconciliation job re-touching the same T-2
            # window) must not double-insert late rows either.
            shaped_late = shape(late, "stream_late")
            existing_late_ids = spark.table(curated("fact_order_item_late")).select("event_id")
            new_late_rows = shaped_late.join(existing_late_ids, "event_id", "left_anti")
            new_late_count = new_late_rows.count()
            new_late_rows.writeTo(curated("fact_order_item_late")).append()
            print(f"[stream batch {batch_id}] {late_count:,} late rows, {new_late_count:,} genuinely new (idempotency check: {late_count - new_late_count:,} already present)")

        assert on_time_count + late_count == total, (
            f"on_time({on_time_count}) + late({late_count}) != total({total}) — "
            "cutoff filter isn't a clean partition of this micro-batch"
        )
    finally:
        batch_df.unpersist()


query = (
    parsed.writeStream
    .foreachBatch(process_batch)
    .option("checkpointLocation", CHECKPOINT_LOCATION)
    .trigger(processingTime="1 minute")
    .start()
)

query.awaitTermination()
job.commit()

"""Parameterized, idempotent backfill: reprocesses raw -> staging for a
specific date range only, using Iceberg's `overwritePartitions()` rather
than `raw_to_staging.py`'s full-table `createOrReplace()`.

The distinction matters: `createOrReplace()` rebuilds the WHOLE table from
whatever data is in scope — correct for the initial load, wrong for a
backfill (it would silently delete every other date's data).
`overwritePartitions()` only replaces the partitions actually present in
the incoming DataFrame, leaving every other date's data untouched — the
actual "safe to re-run against a specific broken date range" primitive a
backfill needs.

Idempotency proof: run this twice with the same --start-date/--end-date and
diff the row counts — see docs/RUNBOOK.md's "Backfill fails at 60%" entry
and RESULTS.md for the executed proof.
"""
import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql import types as T

args = getResolvedOptions(sys.argv, ["JOB_NAME", "bucket", "staging_database", "start_date", "end_date"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

CATALOG = "glue_catalog"
BUCKET = args["bucket"]
STAGING_DB = args["staging_database"]
START_DATE = args["start_date"]
END_DATE = args["end_date"]


def qualified(table: str) -> str:
    return f"{CATALOG}.{STAGING_DB}.{table}"


# Only read the raw partitions in range — this is what makes the job cheap
# to re-run for a single broken day rather than reprocessing everything.
raw_events = (
    spark.read.parquet(f"s3://{BUCKET}/raw/order_events/")
    .where((F.col("ingest_date") >= START_DATE) & (F.col("ingest_date") <= END_DATE))
)

typed_events = (
    raw_events
    .withColumn("event_ts", F.col("event_ts").cast(T.TimestampType()))
    .withColumn("ingest_ts", F.col("ingest_ts").cast(T.TimestampType()))
    .withColumn("quantity", F.col("quantity").cast(T.IntegerType()))
    .withColumn("unit_price", F.col("unit_price").cast(T.DecimalType(10, 2)))
    .withColumn("discount_amount", F.col("discount_amount").cast(T.DecimalType(10, 2)))
    .withColumn("gross_amount", F.col("gross_amount").cast(T.DecimalType(12, 2)))
    .withColumn("net_amount", F.col("net_amount").cast(T.DecimalType(12, 2)))
)

window_first_arrival = typed_events.groupBy("event_id").agg(F.min("ingest_ts").alias("_first_ingest_ts"))
deduped = (
    typed_events.alias("e")
    .join(window_first_arrival.alias("w"), "event_id")
    .where(F.col("e.ingest_ts") == F.col("w._first_ingest_ts"))
    .drop("_first_ingest_ts")
    .dropDuplicates(["event_id"])
)

is_dirty = (
    F.col("product_id").isNull()
    | (F.col("quantity") < 0)
    | (F.col("seller_id") == "seller_UNKNOWN")
)


def failure_reason():
    return (
        F.when(F.col("product_id").isNull(), F.lit("null_product_id"))
        .when(F.col("quantity") < 0, F.lit("negative_quantity"))
        .when(F.col("seller_id") == "seller_UNKNOWN", F.lit("unknown_seller"))
    )


clean = deduped.where(~is_dirty)
quarantine = deduped.where(is_dirty).withColumn("failure_reason", failure_reason())

clean_count = clean.count()
quarantine_count = quarantine.count()
print(f"[backfill {START_DATE}..{END_DATE}] read {raw_events.count():,} raw rows, "
      f"clean {clean_count:,}, quarantined {quarantine_count:,}", flush=True)

# overwritePartitions() — replaces ONLY the days present in `clean`, leaves
# every other date's existing data in the table untouched. This is the
# actual backfill primitive; createOrReplace() would wipe the whole table.
clean.sort(F.to_date("event_ts")).writeTo(qualified("order_events")).overwritePartitions()
quarantine.writeTo(qualified("order_events_quarantine")).overwritePartitions()

print(f"[backfill {START_DATE}..{END_DATE}] done — {clean_count:,} clean, {quarantine_count:,} quarantined", flush=True)
job.commit()

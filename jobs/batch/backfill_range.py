"""Parameterized, idempotent backfill: reprocesses raw -> staging for an
EVENT-date range [start_date, end_date] only, leaving every other date in
the staging tables untouched.

How the range is selected (both fixes below came from a real incident —
see docs/RUNBOOK.md, "Backfill overwrote data outside its range"):

1. Read by ingest_date, keep by event date. The raw zone is partitioned by
   ingest_date, but staging is partitioned by days(event_ts). A late event
   is ingested after its event date (up to ~90 min later in this dataset,
   so possibly the next day). The job therefore reads ingest_date from
   start_date to end_date + LATE_ARRIVAL_DAYS, then keeps only rows whose
   event date is inside the requested range. The previous version filtered
   on ingest_date alone, which (a) dropped late events whose event date was
   in range but ingest date wasn't, and (b) pulled in late events from the
   day BEFORE the range — whose partition overwritePartitions() then
   replaced with just those few rows (2024-05-31 went from 119,798 rows to 41).

2. Overwrite by condition, not by "partitions present". The write uses
   Iceberg's overwrite(condition) on the event_ts range, which replaces
   exactly that range — including days where the new data happens to have
   no rows (overwritePartitions() would leave those stale), and never any
   day outside it. Both tables are partitioned by days(event_ts), so the
   condition aligns with partition boundaries and Iceberg drops whole files.

A guard asserts that every row being written has an event date inside the
range, so a future change to the selection logic fails loudly instead of
silently writing elsewhere.

Idempotency + blast-radius proof: run twice with the same range and compare
counts for the range AND for the days just outside it — see RESULTS.md.
"""
import sys
from datetime import date, timedelta

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
# Range boundaries below are UTC midnights; Iceberg's days() transform is UTC.
spark.conf.set("spark.sql.session.timeZone", "UTC")
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

CATALOG = "glue_catalog"
BUCKET = args["bucket"]
STAGING_DB = args["staging_database"]
START_DATE = date.fromisoformat(args["start_date"])
END_DATE = date.fromisoformat(args["end_date"])
# Max days an event can be ingested after its event date. The generator's
# worst case is 90 min late + 120 s duplicate delay, so 1 day covers it.
LATE_ARRIVAL_DAYS = 1

if END_DATE < START_DATE:
    raise ValueError(f"end_date {END_DATE} is before start_date {START_DATE}")


def qualified(table: str) -> str:
    return f"{CATALOG}.{STAGING_DB}.{table}"


in_range = F.to_date("event_ts").between(F.lit(START_DATE), F.lit(END_DATE))
range_start_ts = F.lit(f"{START_DATE} 00:00:00").cast(T.TimestampType())
range_end_ts = F.lit(f"{END_DATE + timedelta(days=1)} 00:00:00").cast(T.TimestampType())
overwrite_condition = (F.col("event_ts") >= range_start_ts) & (F.col("event_ts") < range_end_ts)

ingest_end = END_DATE + timedelta(days=LATE_ARRIVAL_DAYS)
raw_events = (
    spark.read.parquet(f"s3://{BUCKET}/raw/order_events/")
    .where(F.col("ingest_date").between(str(START_DATE), str(ingest_end)))
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
    .where(in_range)
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


clean = deduped.where(~is_dirty).cache()
quarantine = deduped.where(is_dirty).withColumn("failure_reason", failure_reason()).cache()

# Guard: nothing outside the requested event-date range may be written.
for name, df in (("clean", clean), ("quarantine", quarantine)):
    outside = df.where(~in_range).count()
    if outside:
        raise RuntimeError(f"{outside} {name} rows fall outside {START_DATE}..{END_DATE}; refusing to write")

clean_count = clean.count()
quarantine_count = quarantine.count()
print(f"[backfill {START_DATE}..{END_DATE}] read ingest_date {START_DATE}..{ingest_end}, "
      f"clean {clean_count:,}, quarantined {quarantine_count:,}", flush=True)



def overwrite_range(df, table: str) -> None:
    # Calls the JVM DataFrameWriterV2.overwrite(Column) directly: the PySpark
    # 3.3 wrapper in this Glue build raises "TypeError: Column is not
    # iterable" before reaching it. Same atomic overwrite-by-expression.
    df.sort(F.to_date("event_ts")).writeTo(table)._jwriter.overwrite(overwrite_condition._jc)


overwrite_range(clean, qualified("order_events"))
overwrite_range(quarantine, qualified("order_events_quarantine"))

print(f"[backfill {START_DATE}..{END_DATE}] done — {clean_count:,} clean, {quarantine_count:,} quarantined", flush=True)
job.commit()

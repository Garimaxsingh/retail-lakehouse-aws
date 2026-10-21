"""Raw -> Staging: type, dedupe, and route bad rows out. No business logic
lives here — that's the curated layer's job (see build_dimensions_and_fact.py).

Reads the generator's raw-zone output (order_events, product_price_changes,
product_catalog, sellers, customers) and writes typed Iceberg tables to the
staging Glue database, plus a quarantine table for rows that fail basic
integrity checks.

Runs as a Glue ETL job (glueetl, Glue 4.0, --datalake-formats iceberg).
"""
import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql import types as T

args = getResolvedOptions(sys.argv, ["JOB_NAME", "bucket", "staging_database"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

CATALOG = "glue_catalog"
BUCKET = args["bucket"]
STAGING_DB = args["staging_database"]
RAW_ROOT = f"s3://{BUCKET}/raw"


def qualified(table: str) -> str:
    return f"{CATALOG}.{STAGING_DB}.{table}"


# --- order_events: type, dedupe on event_id, split clean vs quarantine ----

raw_events = spark.read.parquet(f"{RAW_ROOT}/order_events/")

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

# Idempotent dedup: same event_id re-emitted keeps the EARLIEST ingest_ts
# (first arrival wins — later re-emissions are the "duplicate" by design).
window_first_arrival = typed_events.groupBy("event_id").agg(F.min("ingest_ts").alias("_first_ingest_ts"))
deduped = (
    typed_events.alias("e")
    .join(window_first_arrival.alias("w"), "event_id")
    .where(F.col("e.ingest_ts") == F.col("w._first_ingest_ts"))
    .drop("_first_ingest_ts")
    .dropDuplicates(["event_id"])  # true simultaneous ties (identical ingest_ts) — keep exactly one
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

print(f"[raw_to_staging] read {raw_events.count():,} raw rows")
print(f"[raw_to_staging] after dedup: {deduped.count():,}")
print(f"[raw_to_staging] clean: {clean.count():,}, quarantined: {quarantine.count():,}")

(
    clean.sort(F.to_date("event_ts"))  # cluster by the partition transform's value —
    # Iceberg's ClusteredWriter requires rows for the same partition to be
    # co-located per task; an unsorted write at this row count reliably
    # hits "Incoming records violate the writer assumption that records
    # are clustered by spec and by partition within each spec."
    .writeTo(qualified("order_events"))
    .using("iceberg")
    .tableProperty("write.target-file-size-bytes", "134217728")
    .partitionedBy(F.days("event_ts"))
    .createOrReplace()
)

# Partitioned the same way as order_events. An unpartitioned quarantine
# table made backfill_range.py's overwritePartitions() replace the WHOLE
# table (997,170 rows -> 8,593) — dynamic overwrite on an unpartitioned
# table has only one partition to replace. See docs/RUNBOOK.md.
(
    quarantine.sort(F.to_date("event_ts"))
    .writeTo(qualified("order_events_quarantine"))
    .using("iceberg")
    .partitionedBy(F.days("event_ts"))
    .createOrReplace()
)

# --- product_price_changes: typed passthrough — this is dim_product's SCD2 driver ---

price_changes = (
    spark.read.parquet(f"{RAW_ROOT}/product_price_changes/")
    .withColumn("change_ts", F.col("change_ts").cast(T.TimestampType()))
    .withColumn("old_price", F.col("old_price").cast(T.DecimalType(10, 2)))
    .withColumn("new_price", F.col("new_price").cast(T.DecimalType(10, 2)))
)
price_changes.writeTo(qualified("product_price_changes")).using("iceberg").createOrReplace()
print(f"[raw_to_staging] product_price_changes: {price_changes.count():,} rows")

# --- product_catalog, sellers, customers: typed passthrough (base snapshots) ---

product_catalog = (
    spark.read.parquet(f"{RAW_ROOT}/product_catalog/")
    .withColumn("price", F.col("price").cast(T.DecimalType(10, 2)))
)
product_catalog.writeTo(qualified("product_catalog")).using("iceberg").createOrReplace()
print(f"[raw_to_staging] product_catalog: {product_catalog.count():,} rows")

sellers = spark.read.parquet(f"{RAW_ROOT}/sellers/")
sellers.writeTo(qualified("sellers")).using("iceberg").createOrReplace()
print(f"[raw_to_staging] sellers: {sellers.count():,} rows")

customers = spark.read.parquet(f"{RAW_ROOT}/customers/")
customers.writeTo(qualified("customers")).using("iceberg").createOrReplace()
print(f"[raw_to_staging] customers: {customers.count():,} rows")

job.commit()

"""curated.fact_order_item -> curated.fact_order_line: an accumulating-
snapshot fact, ONE row per order line item, with a timestamp column per
lifecycle milestone and the money columns stored exactly once.

Why this table exists: fact_order_item's grain is one row per line item per
status transition, and quantity/gross_amount/net_amount are repeated on
every transition row. Summing them there counts each sale ~3.8 times (the
average number of transitions per line item). fact_order_item stays the
right table for process questions ("how long between approved and
shipped?"); fact_order_line is the right table for revenue and units.
See docs/DATA_MODEL.md.

Money columns are taken from the line's rows (they are identical across a
line's transitions). A line can be missing its 'created' row when that
event was quarantined as dirty (dirty-record injection is per event, not
per line) — such lines keep NULL created_ts, are counted below, and are
partitioned by first_event_ts instead.
"""
import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F

args = getResolvedOptions(sys.argv, ["JOB_NAME", "curated_database"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
spark.conf.set("spark.sql.session.timeZone", "UTC")
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

CURATED = f"glue_catalog.{args['curated_database']}"


def milestone(status):
    return F.min(F.when(F.col("order_status") == status, F.col("event_ts"))).alias(f"{status}_ts")


transitions = spark.table(f"{CURATED}.fact_order_item")

lines = (
    transitions
    .groupBy("order_id", "order_line_id")
    .agg(
        F.max("product_sk").alias("product_sk"),
        F.max("seller_sk").alias("seller_sk"),
        F.max("customer_sk").alias("customer_sk"),
        F.max("quantity").alias("quantity"),
        F.max("unit_price").alias("unit_price"),
        F.max("gross_amount").alias("gross_amount"),
        F.max("discount_amount").alias("discount_amount"),
        F.max("net_amount").alias("net_amount"),
        milestone("created"), milestone("approved"), milestone("shipped"),
        milestone("delivered"), milestone("canceled"),
        F.min("event_ts").alias("first_event_ts"),
        F.max_by("order_status", "event_ts").alias("current_status"),
        F.count(F.lit(1)).alias("transition_count"),
    )
    .withColumn("line_item_key", F.md5(F.concat_ws("|", "order_id", F.col("order_line_id").cast("string"))))
    .withColumn("order_date_key", F.date_format(F.coalesce("created_ts", "first_event_ts"), "yyyyMMdd").cast("int"))
    .withColumn("hours_to_deliver",
                F.round((F.col("delivered_ts").cast("long") - F.col("created_ts").cast("long")) / 3600.0, 2))
)

(
    lines.sort(F.to_date("first_event_ts"))
    .writeTo(f"{CURATED}.fact_order_line")
    .using("iceberg")
    .tableProperty("write.target-file-size-bytes", "134217728")
    .partitionedBy(F.days("first_event_ts"))
    .createOrReplace()
)

written = spark.table(f"{CURATED}.fact_order_line")
stats = written.agg(
    F.count(F.lit(1)).alias("lines"),
    F.sum(F.when(F.col("created_ts").isNull(), 1).otherwise(0)).alias("lines_without_created_row"),
    F.sum("net_amount").alias("net_amount"),
).collect()[0]
naive = transitions.agg(F.sum("net_amount")).collect()[0][0]
print(f"RESULT|lines={stats['lines']}|lines_without_created_row={stats['lines_without_created_row']}"
      f"|line_net_amount={stats['net_amount']}|naive_transition_net_amount={naive}"
      f"|overcount_ratio={float(naive) / float(stats['net_amount']):.3f}")
job.commit()

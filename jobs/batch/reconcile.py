"""Batch/stream reconciliation: the stream owns the last 48 hours and lands
fact rows with NULL surrogate keys (see jobs/streaming/stream_order_events.py
for why). This job re-processes the trailing T-2-day window from
staging.order_events with full point-in-time dimension joins, MERGEs the
corrected rows into curated.fact_order_item on event_id (fixing the NULLs,
not just inserting what's missing), and reports the row-count and
net_amount delta between what the stream landed and what batch recomputed —
the number the project's SLA (<0.01% variance) is measured against.

Run daily in the real design; run on-demand here since this project doesn't
have a live 2-year backfill of streaming data to reconcile against yet.
"""
import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F

args = getResolvedOptions(sys.argv, ["JOB_NAME", "staging_database", "curated_database", "reconcile_days"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

CATALOG = "glue_catalog"
STAGING_DB = args["staging_database"]
CURATED_DB = args["curated_database"]
RECONCILE_DAYS = int(args.get("reconcile_days", 2))


def staging(t):
    return f"{CATALOG}.{STAGING_DB}.{t}"


def curated(t):
    return f"{CATALOG}.{CURATED_DB}.{t}"


cutoff = F.date_sub(F.current_date(), RECONCILE_DAYS)

events = spark.table(staging("order_events")).where(F.to_date("event_ts") >= cutoff)
dim_product = spark.table(curated("dim_product"))
dim_seller = spark.table(curated("dim_seller"))
dim_customer = spark.table(curated("dim_customer"))
dim_date = spark.table(curated("dim_date"))

recomputed = (
    events.alias("e")
    .join(
        dim_product.alias("p"),
        (F.col("e.product_id") == F.col("p.parent_asin"))
        & (F.col("e.event_ts") >= F.col("p.valid_from"))
        & (F.col("e.event_ts") < F.col("p.valid_to")),
        "left",
    )
    .join(dim_seller.alias("s"), F.col("e.seller_id") == F.col("s.seller_id"), "left")
    .join(dim_customer.alias("c"), F.col("e.customer_id") == F.col("c.customer_id"), "left")
    .join(dim_date.alias("d"), F.to_date(F.col("e.event_ts")) == F.col("d.full_date"), "left")
    .select(
        F.md5(F.concat_ws("|", F.col("e.order_id"), F.col("e.order_line_id").cast("string"), F.col("e.order_status"))).alias("order_item_key"),
        F.col("e.event_id"), F.col("e.order_id"), F.col("e.order_line_id"),
        F.col("p.product_sk"), F.col("s.seller_sk"), F.col("c.customer_sk"), F.col("d.date_key"),
        F.col("e.order_status"), F.col("e.quantity"), F.col("e.unit_price"),
        F.col("e.gross_amount"), F.col("e.discount_amount"), F.col("e.net_amount"),
        F.col("e.event_ts"), F.col("e.ingest_ts"),
    )
)
recomputed.createOrReplaceTempView("recomputed_facts")

# --- variance report BEFORE the merge overwrites anything ---
existing_stream_rows = spark.table(curated("fact_order_item")).where(F.col("source_system") == "stream")
delta = (
    existing_stream_rows.alias("s")
    .join(recomputed.alias("b"), "event_id", "inner")
    .select(
        F.count("*").alias("matched_rows"),
        F.sum(F.abs(F.col("s.net_amount") - F.col("b.net_amount"))).alias("abs_net_amount_delta"),
        F.sum("b.net_amount").alias("total_net_amount"),
    )
    .collect()[0]
)
matched = delta["matched_rows"] or 0
abs_delta = float(delta["abs_net_amount_delta"] or 0)
total = float(delta["total_net_amount"] or 0)
variance_pct = (abs_delta / total * 100) if total else 0.0
print(f"[reconcile] matched {matched:,} stream rows against batch recompute")
print(f"[reconcile] net_amount variance: {abs_delta:.2f} / {total:.2f} = {variance_pct:.4f}% (SLA target: <0.01%)")

# --- the actual reconciliation MERGE: fixes NULL surrogate keys, inserts any batch-only rows ---
spark.sql(f"""
    MERGE INTO {curated('fact_order_item')} t
    USING recomputed_facts s
    ON t.event_id = s.event_id
    WHEN MATCHED THEN UPDATE SET
        t.product_sk = s.product_sk, t.seller_sk = s.seller_sk,
        t.customer_sk = s.customer_sk, t.date_key = s.date_key,
        t.gross_amount = s.gross_amount, t.net_amount = s.net_amount
    WHEN NOT MATCHED THEN INSERT (
        order_item_key, event_id, order_id, order_line_id, product_sk, seller_sk,
        customer_sk, date_key, order_status, quantity, unit_price, gross_amount,
        discount_amount, net_amount, event_ts, ingest_ts, source_system
    ) VALUES (
        s.order_item_key, s.event_id, s.order_id, s.order_line_id, s.product_sk, s.seller_sk,
        s.customer_sk, s.date_key, s.order_status, s.quantity, s.unit_price, s.gross_amount,
        s.discount_amount, s.net_amount, s.event_ts, s.ingest_ts, 'batch'
    )
""")

job.commit()

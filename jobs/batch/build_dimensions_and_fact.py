"""Staging -> Curated: the star schema. Business logic lives here.

Builds:
  - dim_date            generated calendar
  - dim_product         SCD Type 2 (tracked: price, price_band, category_l1;
                         overwritten: title, average_rating)
  - dim_seller          SCD2-structured, but see the note in build_dim_seller —
                         the generator doesn't model seller attribute drift, so
                         every seller currently has exactly one version. The
                         table shape supports history; the data just doesn't
                         have any yet. Documented limitation, not a bug.
  - dim_customer        SCD Type 1 (overwrite — geography corrections, not history)
  - fact_order_item     point-in-time correct join to dim_product/dim_seller
                         (i.e. joined to the dimension version that was VALID
                         AT event_ts, not whatever is current today)

This script does the INITIAL bulk historical load — the generator produced
two years of pre-existing history in one shot, so this reconstructs the full
SCD2 timeline from staging.product_price_changes in a single pass. See
jobs/batch/scd2.py for the single atomic MERGE a real *daily* incremental
run uses instead (tested on a scratch table by jobs/batch/test_scd2_merge.py).
"""
import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

args = getResolvedOptions(sys.argv, ["JOB_NAME", "bucket", "staging_database", "curated_database"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

CATALOG = "glue_catalog"
STAGING_DB = args["staging_database"]
CURATED_DB = args["curated_database"]
EPOCH = "2024-01-01 00:00:00"
END_OF_TIME = "9999-12-31 00:00:00"


def staging(table: str) -> str:
    return f"{CATALOG}.{STAGING_DB}.{table}"


def curated(table: str) -> str:
    return f"{CATALOG}.{CURATED_DB}.{table}"


def price_band(col):
    return (
        F.when(col < 25, "under_25")
        .when(col < 100, "25_to_100")
        .when(col < 500, "100_to_500")
        .otherwise("500_plus")
    )


# ============================================================ dim_date ====

order_events = spark.table(staging("order_events"))
date_bounds = order_events.select(
    F.min(F.to_date("event_ts")).alias("min_d"), F.max(F.to_date("event_ts")).alias("max_d")
).collect()[0]

dim_date = (
    spark.sql(f"SELECT explode(sequence(to_date('{date_bounds['min_d']}'), to_date('{date_bounds['max_d']}'), interval 1 day)) AS full_date")
    .withColumn("date_key", F.date_format("full_date", "yyyyMMdd").cast(T.IntegerType()))
    .withColumn("year", F.year("full_date"))
    .withColumn("month", F.month("full_date"))
    .withColumn("day_of_week", F.dayofweek("full_date"))
    .withColumn("fiscal_quarter", F.quarter("full_date"))
    .withColumn("is_holiday", F.lit(False))  # no holiday calendar source in this project — documented gap, not faked
)
dim_date.writeTo(curated("dim_date")).using("iceberg").createOrReplace()
dim_date = spark.table(curated("dim_date"))
print(f"[curated] dim_date: {dim_date.count():,} rows")


# =========================================================== dim_product ==

def build_dim_product() -> DataFrame:
    """Initial bulk SCD2 reconstruction: every price_change row for a
    product becomes a version boundary. Products with no recorded changes
    get a single version spanning the whole window."""
    catalog = spark.table(staging("product_catalog"))
    changes = spark.table(staging("product_price_changes"))

    w = Window.partitionBy("parent_asin").orderBy("change_ts")
    ranked = changes.withColumn("rn", F.row_number().over(w))

    initial_version = (
        ranked.where(F.col("rn") == 1)
        .select(
            "parent_asin",
            F.col("old_price").alias("price"),
            F.lit(EPOCH).cast(T.TimestampType()).alias("valid_from"),
            F.col("change_ts").alias("valid_to"),
        )
    )
    later_versions = (
        ranked
        .withColumn("valid_to", F.coalesce(F.lead("change_ts").over(w), F.lit(END_OF_TIME).cast(T.TimestampType())))
        .select("parent_asin", F.col("new_price").alias("price"), F.col("change_ts").alias("valid_from"), "valid_to")
    )
    changed_history = initial_version.unionByName(later_versions)

    changed_asins = changes.select("parent_asin").distinct()
    unchanged = (
        catalog.join(changed_asins, "parent_asin", "left_anti")
        .select("parent_asin", "price", F.lit(EPOCH).cast(T.TimestampType()).alias("valid_from"), F.lit(END_OF_TIME).cast(T.TimestampType()).alias("valid_to"))
    )

    history = changed_history.unionByName(unchanged)

    dim = (
        history.join(
            catalog.select("parent_asin", "title", "main_category", "category_l1", "category_l2", "category_l3", "store", "average_rating"),
            "parent_asin",
        )
        .withColumn("price_band", price_band(F.col("price")))
        .withColumn("is_current", F.col("valid_to") == F.lit(END_OF_TIME).cast(T.TimestampType()))
        .withColumn("row_hash", F.md5(F.concat_ws("|", F.col("price").cast("string"), F.col("price_band"), F.col("category_l1"))))
        .withColumn("product_sk", F.row_number().over(Window.orderBy("parent_asin", "valid_from")))
    )
    return dim


dim_product = build_dim_product()
dim_product.sort("is_current").writeTo(curated("dim_product")).using("iceberg").tableProperty("write.target-file-size-bytes", "134217728").partitionedBy("is_current").createOrReplace()
# Re-read from the persisted table rather than reusing the in-memory plan —
# the fact join below should hit the actually-written table (the normal
# pipeline-stage boundary), not force Spark to recompute the whole SCD2
# reconstruction a second time as part of the fact query's lineage.
dim_product = spark.table(curated("dim_product"))
print(f"[curated] dim_product: {dim_product.count():,} rows ({dim_product.where('is_current').count():,} current)")


# Incremental (daily) SCD2 updates do NOT use this bulk reconstruction —
# see jobs/batch/scd2.py: apply_price_changes() applies a batch of price
# changes in one atomic MERGE, and jobs/batch/test_scd2_merge.py proves it
# on a scratch table. (An earlier two-statement version lived here; it was
# not atomic and could create duplicate current rows — see DATA_MODEL.md.)


# ============================================================ dim_seller ==

sellers_raw = spark.table(staging("sellers"))
dim_seller = (
    sellers_raw
    .withColumnRenamed("source_seller_id", "olist_source_seller_id")
    .withColumn("valid_from", F.lit(EPOCH).cast(T.TimestampType()))
    .withColumn("valid_to", F.lit(END_OF_TIME).cast(T.TimestampType()))
    .withColumn("is_current", F.lit(True))
    .withColumn("seller_sk", F.row_number().over(Window.orderBy("seller_id")))
)
# NOTE: every seller has exactly one version — the generator doesn't produce
# a seller-attribute-change feed (no analogue to product_price_changes for
# seller_state/seller_tier). The table is SCD2-shaped and MERGE-ready; there
# is just nothing to merge yet. Documented as a roadmap item in DATA_MODEL.md
# rather than silently faking change history.
dim_seller.writeTo(curated("dim_seller")).using("iceberg").createOrReplace()
dim_seller = spark.table(curated("dim_seller"))
print(f"[curated] dim_seller: {dim_seller.count():,} rows (single-version — see note above)")


# ========================================================== dim_customer ==

customers_raw = spark.table(staging("customers"))
dim_customer = (
    customers_raw
    .withColumnRenamed("source_customer_id", "olist_source_customer_id")
    .withColumn("customer_sk", F.row_number().over(Window.orderBy("customer_id")))
)
dim_customer.writeTo(curated("dim_customer")).using("iceberg").createOrReplace()
dim_customer = spark.table(curated("dim_customer"))
print(f"[curated] dim_customer: {dim_customer.count():,} rows")


# ======================================================= fact_order_item ==

events = spark.table(staging("order_events"))

# Point-in-time correct join: pick the dim_product VERSION that was valid at
# event_ts, not whichever version is current today. This is the entire
# reason dim_product carries valid_from/valid_to instead of just being
# overwritten in place.
fact = (
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
        F.col("e.event_id"),
        F.col("e.order_id"),
        F.col("e.order_line_id"),
        F.col("p.product_sk"),
        F.col("s.seller_sk"),
        F.col("c.customer_sk"),
        F.col("d.date_key"),
        F.col("e.order_status"),
        F.col("e.quantity"),
        F.col("e.unit_price"),
        F.col("e.gross_amount"),
        F.col("e.discount_amount"),
        F.col("e.net_amount"),
        F.col("e.event_ts"),
        F.col("e.ingest_ts"),
        F.lit("batch").alias("source_system"),
    )
)

(
    # Explicit sort as well as write.distribution-mode=hash — belt and
    # braces after order_events hit "Incoming records violate the writer
    # assumption" on an unsorted write of comparable scale.
    fact.sort(F.to_date("event_ts"))
    .writeTo(curated("fact_order_item"))
    .using("iceberg")
    .tableProperty("write.target-file-size-bytes", "134217728")
    .tableProperty("write.distribution-mode", "hash")
    .partitionedBy(F.days("event_ts"))
    .createOrReplace()
)
print(f"[curated] fact_order_item: {fact.count():,} rows")

unmatched_product = fact.where(F.col("product_sk").isNull()).count()
print(f"[curated] fact rows with no matching dim_product version (orphan check): {unmatched_product:,}")

# fact_order_item_late: same shape, empty — the streaming job appends to
# this whenever event_ts falls outside its 15-minute watermark. Created
# here (not by the streaming job) because DataFrameWriterV2.append()
# requires the table to already exist.
(
    fact.where("1 = 0")
    .writeTo(curated("fact_order_item_late"))
    .using("iceberg")
    .partitionedBy(F.days("event_ts"))
    .createOrReplace()
)
print("[curated] fact_order_item_late: created (empty, streaming job appends here)")

job.commit()

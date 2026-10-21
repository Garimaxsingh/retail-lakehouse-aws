"""Incremental SCD Type 2 for dim_product: apply a batch of price changes
in ONE atomic MERGE.

This replaces the earlier two-statement pattern (close the current row,
then insert the new one), which had three real defects, found in review
before it ever ran in production:
  - Not atomic: a failure between the two statements left the product with
    no current version, and a re-run then found nothing to close.
  - Step 2 inserted a new version even when step 1 had closed nothing (price
    unchanged), producing two current rows.
  - The INSERT only set price/validity columns; title, categories,
    price_band, row_hash and product_sk came out NULL.

The single-MERGE "union trick": the source has two kinds of rows —
  close rows   merge_key = parent_asin -> match the current version, UPDATE it
  insert rows  merge_key = NULL        -> can never match, so INSERT
Iceberg commits the whole MERGE as one snapshot, so readers see either the
old state or the new one, never a half-applied change.

Also handles: several changes for one product in the same batch (applied in
change_ts order as a version chain), no-op changes (price equal to the
previous version — dropped), changes for unknown products (ignored and
counted), and re-runs (changes not newer than the current version's
valid_from are skipped, so applying the same batch twice is a no-op).
"""
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

END_OF_TIME = "9999-12-31 00:00:00"

# Attributes copied from the current version onto each new version (Type 1
# columns that don't change with price).
CARRIED_COLUMNS = ["title", "main_category", "category_l1", "category_l2",
                   "category_l3", "store", "average_rating"]
DIM_COLUMNS = ["parent_asin", "price", "valid_from", "valid_to", *CARRIED_COLUMNS,
               "price_band", "is_current", "row_hash", "product_sk"]


def price_band(col):
    return (
        F.when(col < 25, "under_25")
        .when(col < 100, "25_to_100")
        .when(col < 500, "100_to_500")
        .otherwise("500_plus")
    )


def build_merge_source(spark: SparkSession, dim_table: str, changes: DataFrame) -> tuple[DataFrame, dict]:
    """Returns (merge source, stats). `changes` needs parent_asin, new_price, change_ts."""
    end_of_time = F.lit(END_OF_TIME).cast(T.TimestampType())
    dim = spark.table(dim_table)
    current = dim.where("is_current").select(
        "parent_asin", F.col("price").alias("current_price"),
        F.col("valid_from").alias("current_valid_from"), *CARRIED_COLUMNS,
    )

    joined = changes.join(current, "parent_asin", "left")
    unknown = joined.where(F.col("current_price").isNull()).count()

    # Only changes newer than the current version (makes re-runs no-ops).
    candidates = joined.where(
        F.col("current_price").isNotNull() & (F.col("change_ts") > F.col("current_valid_from"))
    )
    w = Window.partitionBy("parent_asin").orderBy("change_ts")
    effective = (
        candidates
        .withColumn("prev_price", F.coalesce(F.lag("new_price").over(w), F.col("current_price")))
        .where(F.col("new_price") != F.col("prev_price"))          # drop no-op changes
        .withColumn("valid_to", F.coalesce(F.lead("change_ts").over(w), end_of_time))
    )

    max_sk = dim.agg(F.max("product_sk")).collect()[0][0] or 0
    inserts = (
        effective
        .withColumn("product_sk", F.lit(max_sk) + F.row_number().over(Window.orderBy("parent_asin", "change_ts")))
        .select(
            F.lit(None).cast(T.StringType()).alias("merge_key"),
            F.lit(None).cast(T.TimestampType()).alias("close_ts"),
            "parent_asin",
            F.col("new_price").alias("price"),
            F.col("change_ts").alias("valid_from"),
            "valid_to",
            *CARRIED_COLUMNS,
            price_band(F.col("new_price")).alias("price_band"),
            (F.col("valid_to") == end_of_time).alias("is_current"),
            F.md5(F.concat_ws("|", F.col("new_price").cast("string"), price_band(F.col("new_price")),
                              F.col("category_l1"))).alias("row_hash"),
            F.col("product_sk").cast(dim.schema["product_sk"].dataType).alias("product_sk"),
        )
    )
    closes = (
        effective.groupBy("parent_asin").agg(F.min("change_ts").alias("close_ts"))
        .select(F.col("parent_asin").alias("merge_key"), "close_ts", *[
            F.lit(None).cast(dim.schema[c].dataType).alias(c) for c in DIM_COLUMNS
        ])
    )
    source = closes.unionByName(inserts)
    stats = {"unknown_products": unknown, "new_versions": inserts.count(), "closed_versions": closes.count()}
    return source, stats


def apply_price_changes(spark: SparkSession, dim_table: str, changes: DataFrame) -> dict:
    source, stats = build_merge_source(spark, dim_table, changes)
    source.createOrReplaceTempView("scd2_merge_source")
    cols = ", ".join(DIM_COLUMNS)
    vals = ", ".join(f"s.{c}" for c in DIM_COLUMNS)
    spark.sql(f"""
        MERGE INTO {dim_table} t
        USING scd2_merge_source s
        ON t.parent_asin = s.merge_key AND t.is_current = true
        WHEN MATCHED THEN UPDATE SET t.valid_to = s.close_ts, t.is_current = false
        WHEN NOT MATCHED AND s.merge_key IS NULL THEN INSERT ({cols}) VALUES ({vals})
    """)
    return stats

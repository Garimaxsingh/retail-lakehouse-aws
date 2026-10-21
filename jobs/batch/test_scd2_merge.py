"""Proves jobs/batch/scd2.py on a scratch Iceberg table with the real
dim_product schema, then drops the table. Runs as a Glue job (scd2.py is
shipped via --extra-py-files). Fails the job if any assertion fails.

Scenario (one batch of changes):
  A  10.00 -> 12.00 on 2024-02-01, -> 15.00 on 2024-03-01   two changes, one batch
  B  20.00 -> 20.00 on 2024-02-15                            no-op, must be dropped
  C  30.00 (no change)                                       untouched
  Z  -> 99.00                                                unknown product, ignored
Then the identical batch is applied again: nothing may change.
"""
import sys
from datetime import datetime
from decimal import Decimal

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import Row

from scd2 import DIM_COLUMNS, END_OF_TIME, apply_price_changes

args = getResolvedOptions(sys.argv, ["JOB_NAME", "curated_database"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
spark.conf.set("spark.sql.session.timeZone", "UTC")
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

CURATED = f"glue_catalog.{args['curated_database']}"
SCRATCH = f"{CURATED}.scd2_test_dim_product"
EPOCH = datetime(2024, 1, 1)
EOT = datetime.fromisoformat(END_OF_TIME)

schema = spark.table(f"{CURATED}.dim_product").select(*DIM_COLUMNS).schema


def base_row(asin, price, sk):
    return Row(parent_asin=asin, price=Decimal(price), valid_from=EPOCH, valid_to=EOT,
               title=f"title {asin}", main_category="Electronics", category_l1="Electronics",
               category_l2=None, category_l3=None, store="store", average_rating=4.0,
               price_band="under_25" if Decimal(price) < 25 else "25_to_100",
               is_current=True, row_hash="seed", product_sk=sk)


seed = spark.createDataFrame([base_row("A", "10.00", 1), base_row("B", "20.00", 2), base_row("C", "30.00", 3)],
                             schema=schema)
spark.sql(f"DROP TABLE IF EXISTS {SCRATCH}")
seed.writeTo(SCRATCH).using("iceberg").create()

changes = spark.createDataFrame(
    [("A", Decimal("12.00"), datetime(2024, 2, 1)), ("A", Decimal("15.00"), datetime(2024, 3, 1)),
     ("B", Decimal("20.00"), datetime(2024, 2, 15)), ("Z", Decimal("99.00"), datetime(2024, 2, 1))],
    "parent_asin string, new_price decimal(10,2), change_ts timestamp",
)

failures = []


def check(cond, msg):
    print(f"CHECK|{'pass' if cond else 'FAIL'}|{msg}")
    if not cond:
        failures.append(msg)


stats = apply_price_changes(spark, SCRATCH, changes)
print(f"RESULT|run1|{stats}")
rows = {(r.parent_asin, r.valid_from): r for r in spark.table(SCRATCH).collect()}

a = sorted((r for r in rows.values() if r.parent_asin == "A"), key=lambda r: r.valid_from)
check([str(r.price) for r in a] == ["10.00", "12.00", "15.00"], "A has versions 10 -> 12 -> 15")
check([(r.valid_from, r.valid_to) for r in a] == [(EPOCH, datetime(2024, 2, 1)),
                                                  (datetime(2024, 2, 1), datetime(2024, 3, 1)),
                                                  (datetime(2024, 3, 1), EOT)], "A validity windows chain with no gaps")
check([r.is_current for r in a] == [False, False, True], "only A's latest version is current")
check(all(r.title == "title A" and r.category_l1 == "Electronics" for r in a), "new versions carry title/category")
check([r.price_band for r in a] == ["under_25", "under_25", "under_25"], "price_band computed on new versions")
check(len({r.product_sk for r in rows.values()}) == len(rows), "product_sk unique")
check(sum(r.parent_asin == "B" for r in rows.values()) == 1, "B no-op change dropped (still 1 version)")
check(sum(r.parent_asin == "C" for r in rows.values()) == 1, "C untouched")
check(not any(r.parent_asin == "Z" for r in rows.values()), "unknown product Z not inserted")
check(stats == {"unknown_products": 1, "new_versions": 2, "closed_versions": 1}, "stats reported correctly")
for asin in "ABC":
    check(sum(r.parent_asin == asin and r.is_current for r in rows.values()) == 1, f"exactly one current row for {asin}")

stats2 = apply_price_changes(spark, SCRATCH, changes)
rows2 = {(r.parent_asin, r.valid_from): r for r in spark.table(SCRATCH).collect()}
print(f"RESULT|run2|{stats2}")
check(rows2 == rows, "re-applying the same batch changes nothing (idempotent)")
check(stats2["new_versions"] == 0 and stats2["closed_versions"] == 0, "re-run produces no new or closed versions")

spark.sql(f"DROP TABLE IF EXISTS {SCRATCH} PURGE")
print(f"RESULT|summary|{len(failures)} failures")
if failures:
    raise AssertionError(f"SCD2 test failed: {failures}")
job.commit()

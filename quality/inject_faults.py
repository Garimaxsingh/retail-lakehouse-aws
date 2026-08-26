"""Builds a handful of deliberately-corrupted scratch tables so the
referential-integrity, duplicate, and consistency checks have a genuine
incident to catch — per the project's own definition of done ("an injected
bad batch is caught and alarms fire"), not a hypothetical.

The real pipeline is clean (0 orphans, 0 post-merge duplicates, per Week 2's
validation), which is good for the pipeline and useless for testing the
detector. These scratch tables are clearly labeled fault-injection fixtures,
built from a small real sample and then corrupted — never conflated with
organic incidents in the results log.
"""
from athena_client import run_query

STAGING_DB = "retail_lakehouse_staging"


def create_orphan_fault(sample_date: str) -> str:
    """A handful of rows get a product_sk-equivalent that can't resolve —
    simulated here on product_id, since the scratch table mirrors staging
    shape (pre-dimension-join), not curated shape.

    Deliberately excludes event_ts/ingest_ts from the SELECT — Athena's
    plain Hive/Parquet CTAS doesn't support Iceberg's `timestamp(6) with
    time zone` type ("Unsupported Hive type"), and the checks that use
    these scratch tables never need those columns anyway."""
    table = "dq_scratch_orphan_test"
    run_query(f"""
        CREATE TABLE {STAGING_DB}.{table}
        WITH (format = 'PARQUET') AS
        SELECT
            event_id, order_id, order_line_id, customer_id,
            CASE WHEN row_number() OVER (ORDER BY event_id) <= 25
                 THEN 'NONEXISTENT_PRODUCT_INJECTED'
                 ELSE product_id END AS product_id,
            seller_id, quantity, unit_price, discount_amount, order_status,
            gross_amount, net_amount
        FROM {STAGING_DB}.order_events
        WHERE date(event_ts) = DATE '{sample_date}'
        LIMIT 5000
    """, database=STAGING_DB)
    return table


def create_duplicate_fault(sample_date: str) -> str:
    """Re-insert a subset of real event_ids to create genuine post-merge
    duplicates in the scratch table. See create_orphan_fault's note on why
    event_ts/ingest_ts are excluded."""
    table = "dq_scratch_duplicate_test"
    columns = "event_id, order_id, order_line_id, customer_id, product_id, seller_id, quantity, unit_price, discount_amount, order_status, gross_amount, net_amount"
    run_query(f"""
        CREATE TABLE {STAGING_DB}.{table}
        WITH (format = 'PARQUET') AS
        SELECT {columns} FROM {STAGING_DB}.order_events
        WHERE date(event_ts) = DATE '{sample_date}'
        LIMIT 5000
    """, database=STAGING_DB)
    run_query(f"""
        INSERT INTO {STAGING_DB}.{table}
        SELECT {columns} FROM {STAGING_DB}.{table}
        LIMIT 30
    """, database=STAGING_DB)
    return table


def create_consistency_fault(sample_date: str) -> str:
    """A handful of rows get net_amount deliberately inconsistent with
    gross_amount - discount_amount. See create_orphan_fault's note on why
    event_ts/ingest_ts are excluded."""
    table = "dq_scratch_consistency_test"
    run_query(f"""
        CREATE TABLE {STAGING_DB}.{table}
        WITH (format = 'PARQUET') AS
        SELECT
            event_id, order_id, order_line_id, customer_id, product_id,
            seller_id, quantity, unit_price, discount_amount, order_status,
            gross_amount,
            CASE WHEN row_number() OVER (ORDER BY event_id) <= 15
                 THEN net_amount + 9999.99
                 ELSE net_amount END AS net_amount
        FROM {STAGING_DB}.order_events
        WHERE date(event_ts) = DATE '{sample_date}'
        LIMIT 5000
    """, database=STAGING_DB)
    return table


def drop_fault_tables() -> None:
    for table in ["dq_scratch_orphan_test", "dq_scratch_duplicate_test", "dq_scratch_consistency_test"]:
        run_query(f"DROP TABLE IF EXISTS {STAGING_DB}.{table}", database=STAGING_DB)

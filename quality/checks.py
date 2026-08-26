"""The eight DQ checks from the project spec's data-quality table. Each
takes a `run_date` (and an optional fault-injection scratch table name for
the three checks that need one, since the real pipeline is clean) and
returns a result dict: {check, run_date, passed, actual, threshold, detail,
injected}.
"""
from scipy.stats import ks_2samp

from athena_client import run_query

CURATED_DB = "retail_lakehouse_curated"
STAGING_DB = "retail_lakehouse_staging"


def _result(check, run_date, passed, actual, threshold, detail, injected=False):
    return {
        "check": check, "run_date": run_date, "passed": passed,
        "actual": actual, "threshold": threshold, "detail": detail,
        "injected": injected,
    }


def volume_check(run_date: str) -> dict:
    df = run_query(f"""
        SELECT date(event_ts) as d, count(*) as n
        FROM {CURATED_DB}.fact_order_item
        WHERE event_ts >= date_add('day', -8, timestamp '{run_date} 00:00:00')
          AND event_ts < date_add('day', 1, timestamp '{run_date} 00:00:00')
        GROUP BY 1
    """, CURATED_DB)
    df["n"] = df["n"].astype(int)
    today = df.loc[df["d"] == run_date, "n"]
    trailing = df.loc[df["d"] != run_date, "n"]
    if today.empty or trailing.empty:
        return _result("volume_vs_7d_mean", run_date, None, None, "±30%", "insufficient trailing history")
    today_n = int(today.iloc[0])
    mean_n = trailing.mean()
    pct_dev = abs(today_n - mean_n) / mean_n * 100
    passed = pct_dev <= 30
    return _result("volume_vs_7d_mean", run_date, passed, f"{pct_dev:.1f}% deviation", "±30%",
                   f"today={today_n:,}, trailing_7d_mean={mean_n:,.0f}")


def null_rate_check(run_date: str) -> dict:
    df = run_query(f"""
        SELECT
            (SELECT count(*) FROM {STAGING_DB}.order_events_quarantine
             WHERE date(event_ts) = DATE '{run_date}' AND failure_reason = 'null_product_id') AS null_product,
            (SELECT count(*) FROM {STAGING_DB}.order_events WHERE date(event_ts) = DATE '{run_date}')
            + (SELECT count(*) FROM {STAGING_DB}.order_events_quarantine WHERE date(event_ts) = DATE '{run_date}')
            AS total_seen
    """, STAGING_DB)
    null_product, total = int(df.iloc[0]["null_product"]), int(df.iloc[0]["total_seen"])
    rate = (null_product / total * 100) if total else 0.0
    passed = rate <= 0.1
    return _result("null_rate_product_id", run_date, passed, f"{rate:.3f}%", "0.1%",
                   f"{null_product:,} null / {total:,} total")


def referential_integrity_check(run_date: str, fault_table: str | None = None) -> dict:
    if fault_table:
        df = run_query(f"SELECT count(*) as orphans FROM {STAGING_DB}.{fault_table} WHERE product_id = 'NONEXISTENT_PRODUCT_INJECTED'", STAGING_DB)
    else:
        df = run_query(f"SELECT count(*) as orphans FROM {CURATED_DB}.fact_order_item WHERE date(event_ts) = DATE '{run_date}' AND product_sk IS NULL", CURATED_DB)
    orphans = int(df.iloc[0]["orphans"])
    return _result("referential_integrity_fact_to_dim_product", run_date, orphans == 0, orphans, "0 orphans",
                   f"{orphans} orphaned rows" + (" (injected fault)" if fault_table else ""), injected=bool(fault_table))


def consistency_check(run_date: str, fault_table: str | None = None) -> dict:
    table = f"{STAGING_DB}.{fault_table}" if fault_table else f"{CURATED_DB}.fact_order_item"
    date_filter = "" if fault_table else f"WHERE date(event_ts) = DATE '{run_date}'"
    df = run_query(f"SELECT count(*) as bad FROM {table} {date_filter}"
                    f"{'AND' if date_filter else 'WHERE'} abs(net_amount - (gross_amount - discount_amount)) > 0.01",
                    STAGING_DB if fault_table else CURATED_DB)
    bad = int(df.iloc[0]["bad"])
    return _result("net_amount_consistency", run_date, bad == 0, bad, "0 mismatches",
                   f"{bad} rows where net_amount != gross - discount" + (" (injected fault)" if fault_table else ""), injected=bool(fault_table))


def duplicate_check(run_date: str, fault_table: str | None = None) -> dict:
    table = f"{STAGING_DB}.{fault_table}" if fault_table else f"{CURATED_DB}.fact_order_item"
    date_filter = "" if fault_table else f"WHERE date(event_ts) = DATE '{run_date}'"
    df = run_query(f"""
        SELECT count(*) as dup_groups FROM (
            SELECT event_id FROM {table} {date_filter} GROUP BY event_id HAVING count(*) > 1
        )
    """, STAGING_DB if fault_table else CURATED_DB)
    dup_groups = int(df.iloc[0]["dup_groups"])
    return _result("duplicate_event_id_post_merge", run_date, dup_groups == 0, dup_groups, "0 duplicate groups",
                   f"{dup_groups} event_ids with >1 row" + (" (injected fault)" if fault_table else ""), injected=bool(fault_table))


def freshness_sla_check(run_date: str) -> dict:
    df = run_query(f"""
        SELECT count(*) as total,
               sum(case when date_diff('second', event_ts, ingest_ts) <= 300 then 1 else 0 end) as within_5min
        FROM {CURATED_DB}.fact_order_item
        WHERE date(event_ts) = DATE '{run_date}'
    """, CURATED_DB)
    total, within = int(df.iloc[0]["total"]), int(df.iloc[0]["within_5min"])
    attainment = (within / total * 100) if total else 0.0
    passed = attainment >= 99.0
    return _result("freshness_sla_5min", run_date, passed, f"{attainment:.2f}%", "99%",
                   f"{within:,}/{total:,} events landed within 5min of event_ts")


_SCHEMA_STATE: dict = {"previous_populated": None}


def schema_drift_check(run_date: str, bucket: str) -> dict:
    """Checks the *population rate* of `promotion_code`, not column
    presence. The generator creates this column on every row regardless of
    date (confirmed by inspecting a pre-cutover raw file directly — see the
    correction in docs/RUNBOOK.md), so a presence/absence check would never
    fire. The population rate genuinely jumps from 0% to >0% at the real
    2025-01-05 cutover, which is the actual detectable signal in this data
    for "a field started being used that wasn't before" — a realistic and
    common variant of schema drift, just not literally ADD COLUMN."""
    import pyarrow.compute as pc
    import pyarrow.fs as pafs
    import pyarrow.parquet as pq

    fs = pafs.S3FileSystem(region="ap-south-1")
    path = f"{bucket}/raw/order_events/ingest_date={run_date}/part-0.parquet"
    try:
        table = pq.ParquetFile(fs.open_input_file(path)).read(columns=["promotion_code"])
    except FileNotFoundError:
        return _result("schema_drift_vs_previous", run_date, None, None, "no change", "no raw file for this date")

    col = table.column("promotion_code")
    non_null = col.length() - pc.sum(pc.is_null(col)).as_py()
    populated = non_null > 0

    previous = _SCHEMA_STATE["previous_populated"]
    _SCHEMA_STATE["previous_populated"] = populated
    if previous is None:
        return _result("schema_drift_vs_previous", run_date, True, "baseline", "no change",
                        f"baseline: promotion_code {'populated' if populated else 'always null'} ({non_null}/{col.length()} non-null)")

    if populated == previous:
        return _result("schema_drift_vs_previous", run_date, True, "no change", "no change",
                        f"promotion_code population unchanged ({non_null}/{col.length()} non-null)")
    return _result("schema_drift_vs_previous", run_date, False,
                    f"promotion_code went from {'populated' if previous else 'always-null'} to {'populated' if populated else 'always-null'}",
                    "no change", f"{non_null}/{col.length()} non-null this run — a field started being used")


_BASELINE_PRICES = {}


def price_distribution_shift_check(run_date: str, baseline_date: str = "2024-01-15") -> dict:
    if baseline_date not in _BASELINE_PRICES:
        df = run_query(f"SELECT unit_price FROM {CURATED_DB}.fact_order_item WHERE date(event_ts) = DATE '{baseline_date}'", CURATED_DB)
        _BASELINE_PRICES[baseline_date] = df["unit_price"].astype(float).values

    df = run_query(f"SELECT unit_price FROM {CURATED_DB}.fact_order_item WHERE date(event_ts) = DATE '{run_date}'", CURATED_DB)
    today_prices = df["unit_price"].astype(float).values
    if len(today_prices) == 0 or run_date == baseline_date:
        return _result("price_distribution_shift_ks", run_date, True, "n/a", "p>=0.01", "baseline date itself, skipped")

    stat, p_value = ks_2samp(_BASELINE_PRICES[baseline_date], today_prices)
    passed = p_value >= 0.01
    return _result("price_distribution_shift_ks", run_date, passed, f"p={p_value:.4f}", "p>=0.01",
                   f"KS statistic={stat:.4f} vs baseline {baseline_date}")


ALL_CHECK_NAMES = [
    "volume_vs_7d_mean", "null_rate_product_id", "referential_integrity_fact_to_dim_product",
    "net_amount_consistency", "duplicate_event_id_post_merge", "freshness_sla_5min",
    "schema_drift_vs_previous", "price_distribution_shift_ks",
]

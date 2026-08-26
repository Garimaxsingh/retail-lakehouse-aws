"""Seeded synthetic order-event generator.

Generates `fact_order_item`-shaped events (grain: one row per order line
item per status transition) with the realism properties the project design
calls for: Zipfian product skew, seasonal/hour-of-day curve, late arrivals,
duplicates, dirty records, price drift, and a schema-evolution cutover
(promotion_code added partway through).

Runs entirely locally — no AWS calls. Output is partitioned Parquet under
`generator/data/raw/order_events/ingest_date=YYYY-MM-DD/`, meant to be
`aws s3 sync`'d up once, per the execution plan ("generator runs on your
laptop, not on AWS").
"""
import argparse
import uuid
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds

from popularity import find_exponent, popularity_weights

DATA_DIR = Path(__file__).parent / "data"
OUT_EVENTS = DATA_DIR / "raw" / "order_events"
OUT_PRICE_CHANGES = DATA_DIR / "raw" / "product_price_changes"

# --- Status lifecycle templates -------------------------------------------
# (statuses, cumulative hour-offsets from line-item creation), weight
_TEMPLATES = [
    (["created", "approved", "shipped", "delivered"], [0, 1, 30, 96], 0.80),
    (["created", "approved", "shipped", "canceled"],  [0, 1, 30, 60], 0.08),
    (["created", "canceled"],                          [0, 4],        0.07),
    (["created", "approved", "canceled"],               [0, 1, 20],    0.05),
]
_TEMPLATE_WEIGHTS = np.array([t[2] for t in _TEMPLATES])
_TEMPLATE_WEIGHTS /= _TEMPLATE_WEIGHTS.sum()
_MAX_STAGES = max(len(t[0]) for t in _TEMPLATES)
# Padded arrays for vectorized lookup: shape (n_templates, max_stages)
_STATUS_TABLE = np.full((len(_TEMPLATES), _MAX_STAGES), "", dtype=object)
_OFFSET_TABLE = np.zeros((len(_TEMPLATES), _MAX_STAGES))
_STAGE_COUNT = np.array([len(t[0]) for t in _TEMPLATES])
for i, (statuses, offsets, _) in enumerate(_TEMPLATES):
    for j, (st, off) in enumerate(zip(statuses, offsets)):
        _STATUS_TABLE[i, j] = st
        _OFFSET_TABLE[i, j] = off


def _seasonal_day_weights(start: date, end: date) -> tuple[list[date], np.ndarray]:
    days = []
    d = start
    while d <= end:
        days.append(d)
        d += timedelta(days=1)
    weights = np.array([1.5 if day.month in (10, 11, 12) else 1.0 for day in days])
    weights = weights / weights.sum()
    return days, weights


def _hour_of_day_weights() -> np.ndarray:
    hours = np.arange(24)
    # peak ~14:00, trough ~04:00 — sine wave shifted so it's strictly positive
    w = 1.0 + 0.8 * np.sin((hours - 4) / 24 * 2 * np.pi)
    return w / w.sum()


def generate_chunk(
    n_line_items: int,
    rng: np.random.Generator,
    products: pd.DataFrame,
    product_weights: np.ndarray,
    sellers: pd.DataFrame,
    customers: pd.DataFrame,
    start: date,
    end: date,
    schema_evolution_date: date,
) -> pd.DataFrame:
    """Returns the events_df for this chunk. See generate_price_changes for
    the separate (once-per-run, not once-per-chunk) price-drift feed."""

    # --- pick line-item level attributes ---
    product_idx = rng.choice(len(products), size=n_line_items, p=product_weights)
    seller_idx = rng.integers(0, len(sellers), size=n_line_items)
    customer_idx = rng.integers(0, len(customers), size=n_line_items)

    # ~65% of orders are single-line; group remaining line items under shared
    # order_ids by occasionally reusing the previous order_id. Line items
    # that reuse always immediately follow their order's first line item, so
    # order groups are contiguous runs — that lets this be done as a single
    # cumulative-group-id pass instead of a row-by-row Python loop, and means
    # only one uuid4() is needed per *order* rather than per line item.
    reuse_prev = rng.random(n_line_items) < 0.35
    reuse_prev[0] = False
    starts_new_order = ~reuse_prev
    group_id = np.cumsum(starts_new_order) - 1
    n_orders = int(starts_new_order.sum())
    unique_order_ids = np.array([str(uuid.uuid4()) for _ in range(n_orders)])
    order_ids = unique_order_ids[group_id]
    # order_line_id = running count within each contiguous order group
    order_start_pos = np.where(starts_new_order)[0]
    order_line_id = np.arange(n_line_items) - order_start_pos[group_id] + 1

    quantity = rng.integers(1, 6, size=n_line_items)
    base_price = products["price"].to_numpy()[product_idx]
    base_price = np.where(np.isnan(base_price), 19.99, base_price)  # nulls get a floor price, not fabricated realism — flagged via price_was_null
    price_was_null = np.isnan(products["price"].to_numpy()[product_idx])
    unit_price = np.round(base_price * rng.uniform(0.9, 1.1, size=n_line_items), 2)
    discount_amount = np.round(unit_price * rng.choice([0, 0, 0, 0.05, 0.1, 0.15], size=n_line_items), 2)

    # --- creation timestamp: seasonal day + sine-wave hour ---
    # Vectorized numpy datetime64 arithmetic — no per-row datetime/timedelta
    # object construction, which is the expensive part at 10M+ rows.
    days, day_weights = _seasonal_day_weights(start, end)
    days_arr = np.array(days, dtype="datetime64[s]")
    day_choice = rng.choice(len(days), size=n_line_items, p=day_weights)
    hour_weights = _hour_of_day_weights()
    hour_choice = rng.choice(24, size=n_line_items, p=hour_weights)
    minute_choice = rng.integers(0, 60, size=n_line_items)
    second_choice = rng.integers(0, 60, size=n_line_items)
    created_ts = (
        days_arr[day_choice]
        + hour_choice.astype("timedelta64[h]")
        + minute_choice.astype("timedelta64[m]")
        + second_choice.astype("timedelta64[s]")
    )

    # --- lifecycle template per line item, expanded into one row per stage ---
    template_idx = rng.choice(len(_TEMPLATES), size=n_line_items, p=_TEMPLATE_WEIGHTS)
    stage_counts = _STAGE_COUNT[template_idx]
    total_events = int(stage_counts.sum())

    row_line_item = np.repeat(np.arange(n_line_items), stage_counts.astype(int))
    # stage index within each line item's own sequence (0,1,2,...)
    stage_starts = np.repeat(np.cumsum(stage_counts.astype(int)) - stage_counts.astype(int), stage_counts.astype(int))
    stage_idx = np.arange(total_events) - stage_starts

    ev_template = template_idx[row_line_item]
    order_status = _STATUS_TABLE[ev_template, stage_idx.astype(int)]
    hour_offset = _OFFSET_TABLE[ev_template, stage_idx.astype(int)]

    event_ts = created_ts[row_line_item] + hour_offset.astype("int64").astype("timedelta64[h]")

    events = pd.DataFrame({
        "event_id": [str(uuid.uuid4()) for _ in range(total_events)],
        "order_id": order_ids[row_line_item],
        "order_line_id": order_line_id[row_line_item],
        "customer_id": customers["customer_id"].to_numpy()[customer_idx][row_line_item],
        "product_id": products["parent_asin"].to_numpy()[product_idx][row_line_item],
        "seller_id": sellers["seller_id"].to_numpy()[seller_idx][row_line_item],
        "quantity": quantity[row_line_item],
        "unit_price": unit_price[row_line_item],
        "discount_amount": discount_amount[row_line_item],
        "order_status": order_status,
        "event_ts": event_ts,
        "_price_was_null": price_was_null[row_line_item],
    })

    # --- ingest_ts: normal small lag, 2% late arrivals ---
    n = len(events)
    is_late = rng.random(n) < 0.02
    normal_lag = rng.uniform(1, 5, size=n)  # seconds
    late_lag = rng.uniform(20 * 60, 90 * 60, size=n)  # seconds, 20-90 min
    lag_seconds = np.where(is_late, late_lag, normal_lag)
    # Explicit microsecond precision — pd.to_timedelta defaults to
    # nanosecond resolution, which Spark's Parquet reader can't read
    # (Illegal Parquet type: INT64 (TIMESTAMP(NANOS,false))). Cast at
    # creation rather than relying on a post-hoc fix.
    events["ingest_ts"] = (events["event_ts"] + pd.to_timedelta(lag_seconds, unit="s")).astype("datetime64[us]")

    # --- schema evolution: promotion_code exists only from the cutover date ---
    post_cutover = events["event_ts"] >= pd.Timestamp(schema_evolution_date)
    promo_codes = rng.choice(
        ["SAVE10", "FESTIVE25", "WELCOME5", None, None, None, None],
        size=n,
    )
    events["promotion_code"] = np.where(post_cutover, promo_codes, None)

    # --- dirty records: 1%, one of three corruption types (vectorized —
    # boolean-mask assignment instead of a per-row .iat loop) ---
    dirty_mask = rng.random(n) < 0.01
    dirty_kind = rng.integers(0, 3, size=n)
    null_product_mask = dirty_mask & (dirty_kind == 0)
    neg_qty_mask = dirty_mask & (dirty_kind == 1)
    unknown_seller_mask = dirty_mask & (dirty_kind == 2)
    events.loc[null_product_mask, "product_id"] = None
    events.loc[neg_qty_mask, "quantity"] = -events.loc[neg_qty_mask, "quantity"].abs()
    events.loc[unknown_seller_mask, "seller_id"] = "seller_UNKNOWN"

    # --- duplicates: 0.5% of events re-emitted with same event_id, later ingest_ts ---
    dup_mask = rng.random(n) < 0.005
    dup_rows = events[dup_mask].copy()
    dup_rows["ingest_ts"] = (dup_rows["ingest_ts"] + pd.to_timedelta(rng.uniform(5, 120, size=len(dup_rows)), unit="s")).astype("datetime64[us]")
    events = pd.concat([events, dup_rows], ignore_index=True)

    events["gross_amount"] = np.round(events["quantity"] * events["unit_price"], 2)
    events["net_amount"] = np.round(events["gross_amount"] - events["discount_amount"], 2)
    events["source_system"] = "batch"
    events["ingest_date"] = events["ingest_ts"].dt.date.astype(str)
    events = events.drop(columns=["_price_was_null"])

    return events


def generate_price_changes(products: pd.DataFrame, rng: np.random.Generator, start: date, end: date) -> pd.DataFrame:
    """~3% of the catalog drifts price per month — this is the SCD Type 2
    driver for dim_product downstream. Independent of order-event volume,
    so this runs once for the whole date range, not once per chunk."""
    months = pd.period_range(start, end, freq="M")
    n_products = len(products)
    asins = products["parent_asin"].to_numpy()
    prices = np.where(np.isnan(products["price"].to_numpy()), 19.99, products["price"].to_numpy())

    pc_rows = []
    for m in months:
        n_changing = max(1, int(n_products * 0.03))
        changed_idx = rng.choice(n_products, size=n_changing, replace=False)
        change_day = rng.integers(1, 28, size=n_changing)
        old_price = prices[changed_idx]
        new_price = np.round(old_price * rng.uniform(0.85, 1.15, size=n_changing), 2)
        change_ts = (
            np.datetime64(f"{m.year:04d}-{m.month:02d}-01", "s")
            + (change_day - 1).astype("timedelta64[D]")
        )
        pc_rows.append(pd.DataFrame({
            "parent_asin": asins[changed_idx],
            "old_price": old_price,
            "new_price": new_price,
            "change_ts": change_ts,
        }))
    return pd.concat(pc_rows, ignore_index=True)


def write_partitioned(df: pd.DataFrame, out_dir: Path, chunk_id: int) -> None:
    table = pa.Table.from_pandas(df, preserve_index=False)
    ds.write_dataset(
        table,
        base_dir=str(out_dir),
        format="parquet",
        partitioning=["ingest_date"],
        partitioning_flavor="hive",
        basename_template=f"chunk{chunk_id}-part-{{i}}.parquet",
        existing_data_behavior="overwrite_or_ignore",
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--target-line-items", type=int, default=50_000,
                         help="Number of order line items to generate (each expands to 2-4 status-transition events)")
    parser.add_argument("--chunk-size", type=int, default=500_000)
    parser.add_argument("--start-date", type=str, default="2024-01-01")
    parser.add_argument("--end-date", type=str, default="2025-12-31")
    parser.add_argument("--schema-evolution-date", type=str, default="2025-01-05",
                         help="Date promotion_code starts appearing (default mirrors the real NYC congestion-fee cutover date used in the sibling perf-teardown project)")
    parser.add_argument("--dev-fixture", action="store_true",
                         help="Use the tiny synthetic seller/customer fixture instead of real Olist data")
    parser.add_argument("--max-products", type=int, default=4_000_000,
                         help="Cap catalog size (spec targets 3-5M ASINs; the combined raw metadata is larger). Pass 0 for no cap.")
    parser.add_argument("--amazon-metadata-dir", type=str,
                         default=str(Path(__file__).parent / "data_sources" / "raw" / "amazon_reviews_2023"))
    parser.add_argument("--olist-dir", type=str,
                         default=str(Path(__file__).parent / "data_sources" / "raw" / "olist"))
    args = parser.parse_args()

    from dimensions import load_products, load_sellers, load_customers

    start = date.fromisoformat(args.start_date)
    end = date.fromisoformat(args.end_date)
    schema_evolution_date = date.fromisoformat(args.schema_evolution_date)

    amazon_dir = Path(args.amazon_metadata_dir)
    parquet_paths = sorted(amazon_dir.glob("*.parquet"))
    if not parquet_paths:
        raise SystemExit(f"No Amazon metadata parquet found in {amazon_dir}. Run fetch_amazon_metadata.py first.")

    max_products = args.max_products or None
    print(f"[dims] loading products from {len(parquet_paths)} category file(s)...", flush=True)
    products = load_products(parquet_paths, max_products=max_products, seed=args.seed)
    print(f"[dims] {len(products):,} products loaded", flush=True)

    olist_dir = Path(args.olist_dir)
    if args.dev_fixture or not (olist_dir / "olist_sellers_dataset.csv").exists():
        from data_sources.dev_fixture import write as write_fixture, OUT_DIR as FIXTURE_DIR
        if not (FIXTURE_DIR / "olist_sellers_dataset.csv").exists():
            write_fixture()
        olist_dir = FIXTURE_DIR
        print("[dims] using dev fixture sellers/customers (Olist not available — see fetch_olist.py)", flush=True)

    sellers = load_sellers(olist_dir / "olist_sellers_dataset.csv", seed=args.seed)
    customers = load_customers(olist_dir / "olist_customers_dataset.csv", seed=args.seed)
    print(f"[dims] {len(sellers):,} sellers, {len(customers):,} customers (fanned out)", flush=True)

    print("[popularity] solving Zipf exponent for 80% mass in top 5% of catalog...", flush=True)
    s = find_exponent(len(products))
    weights = popularity_weights(len(products), s)
    print(f"[popularity] s={s:.3f}", flush=True)

    print("[price-changes] generating monthly price-drift feed...", flush=True)
    price_changes = generate_price_changes(products, np.random.default_rng(args.seed + 1), start, end)
    OUT_PRICE_CHANGES.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pandas(price_changes, preserve_index=False)
    ds.write_dataset(table, base_dir=str(OUT_PRICE_CHANGES), format="parquet",
                      basename_template="part-{i}.parquet")
    print(f"[price-changes] {len(price_changes):,} rows written to {OUT_PRICE_CHANGES}", flush=True)

    OUT_EVENTS.mkdir(parents=True, exist_ok=True)

    rng = np.random.default_rng(args.seed)
    remaining = args.target_line_items
    chunk_id = 0
    total_events_written = 0
    while remaining > 0:
        n = min(args.chunk_size, remaining)
        events = generate_chunk(
            n, rng, products, weights, sellers, customers, start, end, schema_evolution_date,
        )
        write_partitioned(events, OUT_EVENTS, chunk_id)
        total_events_written += len(events)
        print(f"[chunk {chunk_id}] {n:,} line items -> {len(events):,} events (cumulative: {total_events_written:,})", flush=True)
        remaining -= n
        chunk_id += 1

    print(f"[done] {total_events_written:,} total order-item events written to {OUT_EVENTS}", flush=True)
    print(f"[done] price-change feed written to {OUT_PRICE_CHANGES}", flush=True)


if __name__ == "__main__":
    main()

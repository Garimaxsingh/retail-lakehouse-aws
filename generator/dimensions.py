"""Builds the reference pools the event generator draws from: products (from
Amazon Reviews 2023 metadata), and sellers/customers (from Olist, fanned out
from the real ~3k sellers / ~99k customers to the larger synthetic pools the
spec calls for — same real geography/attributes, more IDs referencing them).
"""
import re
from pathlib import Path

import numpy as np
import pandas as pd

_PRICE_RE = re.compile(r"[\d,]+\.?\d*")


def _clean_price(raw) -> float | None:
    """Amazon metadata's `price` field is genuinely messy: nulls, plain
    floats, strings like "$24.99", or ranges like "$10.00 - $15.00". Take
    the first parseable number; anything unparseable becomes null rather
    than a fabricated guess — the null rate itself is a DQ signal downstream.
    """
    if raw is None:
        return None
    if isinstance(raw, (int, float)):
        return float(raw) if raw == raw else None  # NaN check
    match = _PRICE_RE.search(str(raw))
    if not match:
        return None
    try:
        return float(match.group().replace(",", ""))
    except ValueError:
        return None


def load_products(parquet_paths: list[Path], max_products: int | None = None, seed: int = 42) -> pd.DataFrame:
    frames = []
    for p in parquet_paths:
        df = pd.read_parquet(p, columns=["parent_asin", "title", "main_category", "categories", "price", "store", "average_rating", "rating_number"])
        frames.append(df)
    products = pd.concat(frames, ignore_index=True)
    products = products.drop_duplicates(subset="parent_asin")

    # Downsample BEFORE the row-wise cleaning below — with a combined catalog
    # in the tens of millions, cleaning every row first (only to throw most
    # of them away) is the dominant cost. Cheap columns first, expensive
    # per-row work only on what survives.
    if max_products is not None and len(products) > max_products:
        products = products.sample(n=max_products, random_state=seed).reset_index(drop=True)
    else:
        products = products.reset_index(drop=True)

    # Plain Python loops over .tolist() rather than df.apply(...) — apply's
    # per-row Series construction is the thing that makes this slow at
    # multi-million-row scale, not the cleaning logic itself.
    products["price"] = [_clean_price(p) for p in products["price"].tolist()]

    l1, l2, l3 = [], [], []
    for cats in products["categories"].tolist():
        if cats is None or len(cats) == 0:
            l1.append(None); l2.append(None); l3.append(None)
        else:
            l1.append(cats[0])
            l2.append(cats[1] if len(cats) > 1 else None)
            l3.append(cats[2] if len(cats) > 2 else None)
    products["category_l1"] = l1
    products["category_l2"] = l2
    products["category_l3"] = l3
    products = products.drop(columns=["categories"])

    products["product_sk"] = np.arange(1, len(products) + 1)
    return products


def _fan_out(real: pd.DataFrame, id_col: str, target_n: int, new_prefix: str, seed: int) -> pd.DataFrame:
    """Sample `target_n` rows (with replacement) from the real dimension pool
    and assign each a fresh synthetic ID, keeping every real attribute
    (geography, etc.) attached — this is what "fanned out to N synthetic"
    means: more IDs, same underlying real-world shape, no fabricated geography.
    """
    rng = np.random.default_rng(seed)
    idx = rng.integers(0, len(real), size=target_n)
    fanned = real.iloc[idx].reset_index(drop=True).copy()
    fanned["source_" + id_col] = fanned[id_col]
    fanned[id_col] = [f"{new_prefix}_{i:07d}" for i in range(target_n)]
    return fanned


def load_sellers(csv_path: Path, target_n: int = 50_000, seed: int = 42) -> pd.DataFrame:
    real = pd.read_csv(csv_path, dtype={"seller_zip_code_prefix": str})
    return _fan_out(real, "seller_id", target_n, "seller", seed)


def load_customers(csv_path: Path, target_n: int = 2_000_000, seed: int = 42) -> pd.DataFrame:
    real = pd.read_csv(csv_path, dtype={"customer_zip_code_prefix": str})
    return _fan_out(real, "customer_id", target_n, "customer", seed)

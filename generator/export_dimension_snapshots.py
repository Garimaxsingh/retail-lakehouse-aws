"""Exports the product/seller/customer reference pools the main event-
generation run used, as their own raw-zone snapshots for the batch job to
build dim_product/dim_seller/dim_customer from.

Deliberately NOT re-running the (expensive, 33-minute) event generation —
dimensions.load_products/load_sellers/load_customers are pure functions of
(source files, seed), so re-calling them with the same --seed reproduces
the exact same DataFrames the real run used, in ~90 seconds.
"""
import argparse
from pathlib import Path

from dimensions import load_products, load_sellers, load_customers

DATA_DIR = Path(__file__).parent / "data"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--max-products", type=int, default=4_000_000)
    parser.add_argument("--amazon-metadata-dir", type=str,
                         default=str(Path(__file__).parent / "data_sources" / "raw" / "amazon_reviews_2023"))
    parser.add_argument("--olist-dir", type=str,
                         default=str(Path(__file__).parent / "data_sources" / "raw" / "olist"))
    args = parser.parse_args()

    amazon_dir = Path(args.amazon_metadata_dir)
    parquet_paths = sorted(amazon_dir.glob("*.parquet"))
    print(f"[dims] loading products from {len(parquet_paths)} category file(s)...", flush=True)
    products = load_products(parquet_paths, max_products=args.max_products or None, seed=args.seed)
    print(f"[dims] {len(products):,} products", flush=True)

    olist_dir = Path(args.olist_dir)
    sellers = load_sellers(olist_dir / "olist_sellers_dataset.csv", seed=args.seed)
    customers = load_customers(olist_dir / "olist_customers_dataset.csv", seed=args.seed)
    print(f"[dims] {len(sellers):,} sellers, {len(customers):,} customers", flush=True)

    out_products = DATA_DIR / "raw" / "product_catalog"
    out_sellers = DATA_DIR / "raw" / "sellers"
    out_customers = DATA_DIR / "raw" / "customers"
    for d in (out_products, out_sellers, out_customers):
        d.mkdir(parents=True, exist_ok=True)

    products.to_parquet(out_products / "part-0.parquet", index=False)
    sellers.to_parquet(out_sellers / "part-0.parquet", index=False)
    customers.to_parquet(out_customers / "part-0.parquet", index=False)
    print(f"[done] snapshots written to {DATA_DIR / 'raw'}", flush=True)


if __name__ == "__main__":
    main()

"""Download Amazon Reviews 2023 item metadata (McAuley Lab) for a few
categories and write each out as local Parquet — this is the source of
parent_asin / title / price / category used by the order-event generator.

Does not touch AWS. Run once; output is gitignored (data/ prefix).
"""
import sys
from pathlib import Path

from datasets import load_dataset

OUT_DIR = Path(__file__).parent / "raw" / "amazon_reviews_2023"
CATEGORIES = ["Electronics", "Home_and_Kitchen", "Clothing_Shoes_and_Jewelry"]


def fetch(category: str) -> None:
    out_path = OUT_DIR / f"{category}.parquet"
    if out_path.exists():
        print(f"[skip] {category} already at {out_path}")
        return
    print(f"[fetch] raw_meta_{category} ...")
    ds = load_dataset(
        "McAuley-Lab/Amazon-Reviews-2023",
        f"raw_meta_{category}",
        split="full",
        trust_remote_code=True,
    )
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    ds.to_parquet(str(out_path))
    print(f"[done] {category}: {ds.num_rows} rows -> {out_path}")


if __name__ == "__main__":
    cats = sys.argv[1:] or CATEGORIES
    for c in cats:
        fetch(c)

"""Download the Olist Brazilian E-Commerce dataset from Kaggle.

Requires a Kaggle API token — this cannot be automated without your Kaggle
account. One-time setup:

    1. Log in at kaggle.com, go to Settings -> API -> "Create New Token".
       This downloads kaggle.json.
    2. Place it at ~/.kaggle/kaggle.json (or C:\\Users\\<you>\\.kaggle\\kaggle.json
       on Windows).
    3. pip install kaggle (already in requirements.txt)
    4. Run: python generator/data_sources/fetch_olist.py

If you'd rather not create a Kaggle token, download the dataset manually from
https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce, unzip it, and
place the CSVs directly at generator/data_sources/raw/olist/ — this script
just automates that same step.
"""
import subprocess
import sys
from pathlib import Path

OUT_DIR = Path(__file__).parent / "raw" / "olist"
DATASET = "olistbr/brazilian-ecommerce"

REQUIRED_FILES = [
    "olist_customers_dataset.csv",
    "olist_sellers_dataset.csv",
    "olist_orders_dataset.csv",
]


def main() -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    try:
        import kaggle  # noqa: F401
    except ImportError:
        print(
            "The 'kaggle' package isn't installed. Run:\n"
            "  pip install kaggle\n"
            "then re-run this script.",
            file=sys.stderr,
        )
        sys.exit(1)

    subprocess.run(
        [
            sys.executable, "-m", "kaggle", "datasets", "download",
            "-d", DATASET, "-p", str(OUT_DIR), "--unzip",
        ],
        check=True,
    )

    missing = [f for f in REQUIRED_FILES if not (OUT_DIR / f).exists()]
    if missing:
        print(f"[warn] expected files missing after download: {missing}", file=sys.stderr)
    else:
        print(f"[done] Olist CSVs in {OUT_DIR}")


if __name__ == "__main__":
    main()

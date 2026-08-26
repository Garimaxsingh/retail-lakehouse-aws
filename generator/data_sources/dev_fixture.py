"""Generate a small, clearly-fake seller/customer fixture so the event
generator can be smoke-tested before the real Olist dataset is downloaded
(Olist requires a Kaggle account token — see fetch_olist.py).

This is NOT a substitute for Olist in the actual generator run. It exists
only so `generator/order_events.py --dev-fixture` can be exercised locally
without network/Kaggle access. Row counts are tiny (hundreds), geography is
templated, not real.
"""
from pathlib import Path

import numpy as np
import pandas as pd

OUT_DIR = Path(__file__).parent / "raw" / "dev_fixture"

_STATES = ["SP", "RJ", "MG", "RS", "PR", "BA", "SC", "GO", "PE", "CE"]
_CITIES = {
    "SP": "sao paulo", "RJ": "rio de janeiro", "MG": "belo horizonte",
    "RS": "porto alegre", "PR": "curitiba", "BA": "salvador",
    "SC": "florianopolis", "GO": "goiania", "PE": "recife", "CE": "fortaleza",
}


def make_sellers(n: int, seed: int) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    states = rng.choice(_STATES, size=n)
    return pd.DataFrame({
        "seller_id": [f"devseller_{i:06d}" for i in range(n)],
        "seller_zip_code_prefix": rng.integers(1000, 99999, size=n),
        "seller_city": [_CITIES[s] for s in states],
        "seller_state": states,
    })


def make_customers(n: int, seed: int) -> pd.DataFrame:
    rng = np.random.default_rng(seed + 1)
    states = rng.choice(_STATES, size=n)
    return pd.DataFrame({
        "customer_id": [f"devcust_{i:06d}" for i in range(n)],
        "customer_zip_code_prefix": rng.integers(1000, 99999, size=n),
        "customer_city": [_CITIES[s] for s in states],
        "customer_state": states,
    })


def write(n_sellers: int = 200, n_customers: int = 2000, seed: int = 42) -> None:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    make_sellers(n_sellers, seed).to_csv(OUT_DIR / "olist_sellers_dataset.csv", index=False)
    make_customers(n_customers, seed).to_csv(OUT_DIR / "olist_customers_dataset.csv", index=False)
    print(f"[done] dev fixture sellers/customers -> {OUT_DIR}")


if __name__ == "__main__":
    write()

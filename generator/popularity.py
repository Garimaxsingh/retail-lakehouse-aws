"""Zipfian popularity weighting: 80% of order volume should land on ~5% of
the catalog. Rather than hand-tune a Zipf exponent and hope, solve for the
exponent that hits the target exactly, once, at startup.
"""
import numpy as np


def _mass_in_top_fraction(n: int, s: float, top_frac: float) -> float:
    ranks = np.arange(1, n + 1, dtype=np.float64)
    weights = ranks ** (-s)
    total = weights.sum()
    k = max(1, int(n * top_frac))
    return weights[:k].sum() / total


def find_exponent(n: int, top_frac: float = 0.05, mass_frac: float = 0.80,
                   lo: float = 0.3, hi: float = 3.0, iters: int = 40) -> float:
    """Binary search for the Zipf exponent s such that the top `top_frac` of
    `n` ranked items carry `mass_frac` of total probability mass."""
    for _ in range(iters):
        mid = (lo + hi) / 2
        mass = _mass_in_top_fraction(n, mid, top_frac)
        if mass < mass_frac:
            lo = mid
        else:
            hi = mid
    return (lo + hi) / 2


def popularity_weights(n: int, s: float) -> np.ndarray:
    ranks = np.arange(1, n + 1, dtype=np.float64)
    w = ranks ** (-s)
    return w / w.sum()

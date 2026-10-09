"""Synthetic raw daily OHLCV files (as upstream strings) for data-pipeline tests."""

from __future__ import annotations

import numpy as np
import pandas as pd

NOW = pd.Timestamp("2026-10-01", tz="UTC")


def sessions(n: int, start: str = "2024-01-02") -> pd.DatetimeIndex:
    """Session opens at 09:30 New York time (so 13:30 or 14:30 UTC depending on DST)."""
    days = pd.bdate_range(start, periods=n)
    return (
        (days + pd.Timedelta(hours=9, minutes=30)).tz_localize("America/New_York").tz_convert("UTC")
    )


def frame(
    n: int = 120, *, seed: int = 0, start: str = "2024-01-02", sigma: float = 0.01
) -> pd.DataFrame:
    """A clean random-walk daily series as raw strings, like the upstream CSVs."""
    rng = np.random.default_rng(seed)
    close = 100 * np.exp(np.cumsum(rng.normal(0, sigma, n)))
    open_ = close * (1 + rng.normal(0, 0.002, n))
    high = np.maximum(open_, close) * 1.005
    low = np.minimum(open_, close) * 0.995
    volume = rng.integers(900_000, 1_100_000, n)
    epoch = pd.Timestamp("1970-01-01", tz="UTC")
    ts = ((sessions(n, start) - epoch) // pd.Timedelta(seconds=1)).astype(str)
    return pd.DataFrame(
        {
            "timestamp": ts,
            "open": open_.round(4).astype(str),
            "high": high.round(4).astype(str),
            "low": low.round(4).astype(str),
            "close": close.round(4).astype(str),
            "volume": volume.astype(str),
        }
    )

"""Synthetic cleaned-bar frames for ML tests."""

from __future__ import annotations

import numpy as np
import pandas as pd


def cleaned_frame(
    n: int = 400, symbols: tuple[str, ...] = ("AAA", "BBB"), seed: int = 0
) -> pd.DataFrame:
    """A cleaned.parquet-shaped frame of business-day bars."""
    rng = np.random.default_rng(seed)
    dates = pd.bdate_range("2016-01-04", periods=n)
    blocks = []
    for i, symbol in enumerate(symbols):
        close = 100 * np.exp(np.cumsum(rng.normal(0, 0.01, n)))
        blocks.append(
            pd.DataFrame(
                {
                    "symbol": symbol,
                    "session_date": dates,
                    "timestamp": (dates + pd.Timedelta(hours=14, minutes=30)).tz_localize("UTC"),
                    "open": close,
                    "high": close * 1.01,
                    "low": close * 0.99,
                    "close": close,
                    "volume": rng.integers(1_000, 2_000, n) + i,
                    "missing_sessions_before": 0,
                }
            )
        )
    return pd.concat(blocks, ignore_index=True)

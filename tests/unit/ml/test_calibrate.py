from __future__ import annotations

import json
import math
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from stockml.data.calibrate import (
    DEFAULT_OUTPUT,
    TRADING_DAYS_PER_YEAR,
    calibrate_symbol,
    load_raw_bars,
)


def synthetic_gbm(n: int, mu: float, sigma: float, seed: int = 1) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    dt = 1 / TRADING_DAYS_PER_YEAR
    log_ret = (mu - 0.5 * sigma**2) * dt + sigma * math.sqrt(dt) * rng.standard_normal(n)
    close = 100 * np.exp(np.cumsum(log_ret))
    ts = pd.date_range("2015-01-02 14:30", periods=n, freq="B", tz="UTC")
    return pd.DataFrame(
        {
            "timestamp": ts,
            "open": close,
            "high": close * 1.01,
            "low": close * 0.99,
            "close": close,
            "volume": rng.lognormal(15, 0.3, n).astype(int),
        }
    )


def test_recovers_gbm_volatility() -> None:
    frame = synthetic_gbm(5_000, mu=0.08, sigma=0.3)
    result = calibrate_symbol(frame, lookback=5_000)
    assert result["sigma_annual"] == pytest.approx(0.3, rel=0.05)
    assert result["observations"] == 4_999
    assert result["log_volume_std"] == pytest.approx(0.3, rel=0.1)


def test_uses_only_the_lookback_window() -> None:
    frame = synthetic_gbm(1_000, mu=0.0, sigma=0.2)
    result = calibrate_symbol(frame, lookback=252)
    assert result["observations"] == 252
    assert result["window_end"] == frame["timestamp"].iloc[-1].date().isoformat()
    assert result["last_close"] == pytest.approx(frame["close"].iloc[-1], rel=1e-6)


def test_bad_rows_are_dropped_and_counted() -> None:
    frame = synthetic_gbm(300, mu=0.0, sigma=0.2)
    frame.loc[10, "close"] = -1.0
    frame.loc[11, "volume"] = np.nan
    assert calibrate_symbol(frame, lookback=500)["dropped_rows"] == 2


def test_too_little_data_is_rejected() -> None:
    with pytest.raises(ValueError, match="usable rows"):
        calibrate_symbol(synthetic_gbm(30, 0.0, 0.2), lookback=252)


def test_raw_reader_is_column_order_independent(tmp_path: Path) -> None:
    path = tmp_path / "X.csv"
    path.write_text(
        "volume,low,close,open,high,timestamp\n"
        "100,9.5,10.5,10,11,1262615400\n"
        "200,10,11,10.5,11.5,1262701800\n"
    )
    frame = load_raw_bars(path)
    assert list(frame.columns) == ["timestamp", "open", "high", "low", "close", "volume"]
    assert frame["timestamp"].iloc[0] == pd.Timestamp("2010-01-04 14:30", tz="UTC")
    assert frame["open"].tolist() == [10.0, 10.5]


def test_committed_calibration_file_is_well_formed() -> None:
    data = json.loads(DEFAULT_OUTPUT.read_text())
    assert data["revision"]
    assert data["symbols"]
    for params in data["symbols"].values():
        assert params["last_close"] > 0
        assert 0.05 < params["sigma_annual"] < 2.0
        assert params["median_daily_volume"] > 0

"""The shared feature computer: parity with batch formulas, causality, gaps, warm-up."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import numpy as np
import pandas as pd
import pytest

from shared.features import FEATURE_NAMES, BarInput, FeatureComputer
from shared.features.computer import WARMUP_BARS

N = 300


@pytest.fixture(scope="module")
def bars() -> pd.DataFrame:
    rng = np.random.default_rng(11)
    close = 50 * np.exp(np.cumsum(rng.normal(0.0003, 0.015, N)))
    open_ = close * np.exp(rng.normal(0, 0.004, N))
    high = np.maximum(open_, close) * (1 + rng.uniform(0, 0.01, N))
    low = np.minimum(open_, close) * (1 - rng.uniform(0, 0.01, N))
    volume = rng.integers(1_000, 100_000, N).astype(float)
    volume[37] = 0.0  # a zero-volume session must stay finite
    ts = [datetime(2024, 1, 2, 14, 30, tzinfo=UTC) + timedelta(days=i) for i in range(N)]
    return pd.DataFrame(
        {"timestamp": ts, "open": open_, "high": high, "low": low, "close": close, "volume": volume}
    )


def inputs(df: pd.DataFrame) -> list[BarInput]:
    arrays = [df[c].to_numpy(dtype=float) for c in ("open", "high", "low", "close", "volume")]
    return [
        BarInput(ts, o, h, lo, c, v)
        for ts, o, h, lo, c, v in zip(list(df["timestamp"]), *arrays, strict=True)
    ]


def run(df: pd.DataFrame) -> pd.DataFrame:
    computer = FeatureComputer()
    rows = [computer.update(bar) for bar in inputs(df)]
    return pd.DataFrame(rows, columns=list(FEATURE_NAMES), dtype="float64")


def wilder(series: pd.Series, period: int) -> pd.Series:
    """Textbook Wilder smoothing seeded with the simple mean (independent loop)."""
    values = series.to_numpy()
    out = np.full(len(values), np.nan)
    start = int(np.argmax(~np.isnan(values)))
    seed_end = start + period
    out[seed_end - 1] = values[start:seed_end].mean()
    for i in range(seed_end, len(values)):
        out[i] = (out[i - 1] * (period - 1) + values[i]) / period
    return pd.Series(out)


def reference(df: pd.DataFrame) -> pd.DataFrame:
    """Vectorised batch formulas for every feature (written independently of the computer)."""
    c, o, h, lo, v = df["close"], df["open"], df["high"], df["low"], df["volume"]
    r = pd.Series(np.log(c / c.shift(1)), index=df.index)
    out = pd.DataFrame(index=df.index)
    out["ret_1"] = r
    for k in (5, 10, 20):
        out[f"ret_{k}"] = np.log(c / c.shift(k))
    for lag in range(1, 5):
        out[f"ret_lag_{lag}"] = r.shift(lag)
    out["gap"] = np.log(o / c.shift(1))
    out["hl_range"] = (h - lo) / c
    out["close_pos"] = (c - lo) / (h - lo)
    for k in (10, 20, 50):
        out[f"close_sma{k}"] = c / c.rolling(k).mean() - 1
    out["sma10_sma50"] = c.rolling(10).mean() / c.rolling(50).mean() - 1
    ema12 = c.ewm(span=12, adjust=False).mean().where(c.index >= 11)
    ema26 = c.ewm(span=26, adjust=False).mean().where(c.index >= 25)
    macd = ema12 - ema26
    signal = macd.dropna().ewm(span=9, adjust=False).mean().reindex(c.index)
    signal[: 25 + 8] = np.nan
    out["macd_norm"] = macd / c
    out["macd_hist_norm"] = (macd - signal) / c
    out["vol_10"] = r.rolling(10).std()
    out["vol_20"] = r.rolling(20).std()
    out["vol_ratio_10_60"] = r.rolling(10).std() / r.rolling(60).std()
    tr = pd.concat([h, c.shift(1)], axis=1).max(axis=1) - pd.concat([lo, c.shift(1)], axis=1).min(
        axis=1
    )
    tr[0] = np.nan
    out["atr_14_norm"] = wilder(tr, 14) / c
    mid, sd = c.rolling(20).mean(), c.rolling(20).std(ddof=0)
    out["bb_width_20"] = 4 * sd / mid
    out["bb_pos_20"] = (c - (mid - 2 * sd)) / (4 * sd)
    delta = c.diff()
    gain, loss = wilder(delta.clip(lower=0), 14), wilder((-delta).clip(lower=0), 14)
    out["rsi_14"] = 1 - 1 / (1 + gain / loss)
    out["volume_ratio_20"] = np.log((v + 1) / (v.rolling(20).mean() + 1))
    out["volume_change"] = np.log((v + 1) / (v.shift(1) + 1))
    out["day_of_week"] = df["timestamp"].map(lambda t: t.weekday()).astype(float)
    out["month"] = df["timestamp"].map(lambda t: t.month).astype(float)
    return out[list(FEATURE_NAMES)]


def test_every_feature_matches_its_batch_definition(bars: pd.DataFrame) -> None:
    online, batch = run(bars), reference(bars)
    for name in FEATURE_NAMES:
        a, b = online[name], batch[name]
        assert a.isna().tolist() == b.isna().tolist(), f"{name}: warm-up differs"
        np.testing.assert_allclose(a.dropna(), b.dropna(), rtol=1e-8, atol=1e-12, err_msg=name)


def test_features_are_point_in_time(bars: pd.DataFrame) -> None:
    """Leakage test: features at t are identical whether or not later bars exist."""
    full = run(bars)
    for t in (WARMUP_BARS, 100, 177, N - 1):
        truncated = run(bars.iloc[: t + 1])
        pd.testing.assert_series_equal(
            truncated.iloc[t], full.iloc[t], check_names=False, rtol=0, atol=0
        )


def test_future_shock_cannot_change_past_features(bars: pd.DataFrame) -> None:
    shocked = bars.copy()
    shocked.loc[200:, ["open", "high", "low", "close"]] *= 3.0
    pd.testing.assert_frame_equal(run(bars).iloc[:200], run(shocked).iloc[:200])


def test_warmup_then_complete(bars: pd.DataFrame) -> None:
    features = run(bars)
    complete = features.notna().all(axis=1)
    assert not complete.iloc[: WARMUP_BARS - 1].any()
    assert complete.iloc[WARMUP_BARS - 1 :].all()
    assert np.isfinite(features.iloc[WARMUP_BARS - 1 :].to_numpy()).all(), (
        "zero volume stays finite"
    )


def test_gap_resets_state(bars: pd.DataFrame) -> None:
    computer = FeatureComputer()
    series = inputs(bars)
    for bar in series[:100]:
        computer.update(bar)
    ready_before = computer.ready
    nxt = series[100]
    after_gap = computer.update(
        BarInput(nxt.timestamp, nxt.open, nxt.high, nxt.low, nxt.close, nxt.volume, gap_before=3)
    )
    assert ready_before
    assert computer.bars_seen == 1
    assert after_gap["ret_1"] is None, "no return is computed across a gap"

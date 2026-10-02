"""Incremental indicators must agree with straightforward batch reference implementations."""

from __future__ import annotations

import math
from datetime import UTC, datetime, timedelta

import numpy as np
import pandas as pd
import pytest
from factories import make_bar

from shared.schemas import BarInterval, IndicatorSnapshot
from stream_processor.indicators import Ema, IndicatorEngine, RollingWindow, WilderRsi

T0 = datetime(2026, 1, 5, 14, 30, tzinfo=UTC)
N = 400


def random_walk(n: int = N, seed: int = 3) -> tuple[np.ndarray, np.ndarray]:
    rng = np.random.default_rng(seed)
    close = np.round(100 * np.exp(np.cumsum(rng.normal(0, 0.01, n))), 2)
    volume = rng.integers(1_000, 50_000, n)
    return close, volume


def run_engine(close: np.ndarray, volume: np.ndarray) -> list[IndicatorSnapshot]:
    engine = IndicatorEngine()
    out = []
    for i, (c, v) in enumerate(zip(close, volume, strict=True)):
        bar = make_bar(
            timestamp=T0 + timedelta(minutes=i),
            interval=BarInterval.M1,
            open=float(c),
            high=float(c),
            low=float(c),
            close=float(c),
            volume=int(v),
        )
        out.append(engine.update(bar))
    return out


def column(snaps: list[IndicatorSnapshot], name: str) -> pd.Series:
    return pd.Series([getattr(s, name) for s in snaps], dtype="float64")


def assert_matches(actual: pd.Series, expected: pd.Series, *, rel: float = 1e-9) -> None:
    assert actual.isna().tolist() == expected.isna().tolist(), "warm-up (None) positions differ"
    mask = expected.notna()
    np.testing.assert_allclose(actual[mask], expected[mask], rtol=rel, atol=1e-12)


@pytest.fixture(scope="module")
def data() -> tuple[pd.Series, pd.Series, list[IndicatorSnapshot]]:
    close, volume = random_walk()
    return pd.Series(close), pd.Series(volume, dtype="float64"), run_engine(close, volume)


def test_returns(data: tuple[pd.Series, pd.Series, list[IndicatorSnapshot]]) -> None:
    close, volume, snaps = data
    assert_matches(column(snaps, "return_1"), close.pct_change())
    assert_matches(column(snaps, "log_return_1"), pd.Series(np.log(close / close.shift())))
    assert_matches(column(snaps, "price_change"), close.diff())
    assert_matches(column(snaps, "volume_change_pct"), volume.pct_change())


def test_moving_averages_and_bollinger(
    data: tuple[pd.Series, pd.Series, list[IndicatorSnapshot]],
) -> None:
    close, volume, snaps = data
    sma = close.rolling(20).mean()
    std0 = close.rolling(20).std(ddof=0)
    assert_matches(column(snaps, "sma_20"), sma)
    assert_matches(column(snaps, "bb_middle"), sma)
    assert_matches(column(snaps, "bb_upper"), sma + 2 * std0, rel=1e-8)
    assert_matches(column(snaps, "bb_lower"), sma - 2 * std0, rel=1e-8)
    assert_matches(column(snaps, "bb_width"), (4 * std0) / sma, rel=1e-6)
    vol_sma = volume.rolling(20).mean()
    assert_matches(column(snaps, "volume_sma_20"), vol_sma)
    assert_matches(column(snaps, "volume_ratio"), volume / vol_sma)


def test_volatility(data: tuple[pd.Series, pd.Series, list[IndicatorSnapshot]]) -> None:
    close, _, snaps = data
    expected = pd.Series(np.log(close / close.shift())).rolling(20).std(ddof=1)
    assert_matches(column(snaps, "volatility_20"), expected, rel=1e-7)


def test_ema_and_macd(data: tuple[pd.Series, pd.Series, list[IndicatorSnapshot]]) -> None:
    close, _, snaps = data
    ema12 = close.ewm(span=12, adjust=False).mean()
    ema26 = close.ewm(span=26, adjust=False).mean()
    ema12[:11] = np.nan
    ema26[:25] = np.nan
    assert_matches(column(snaps, "ema_12"), ema12)
    assert_matches(column(snaps, "ema_26"), ema26)

    macd = ema12 - ema26
    signal = macd.dropna().ewm(span=9, adjust=False).mean().reindex(macd.index)
    signal[: 25 + 8] = np.nan
    assert_matches(column(snaps, "macd"), macd)
    assert_matches(column(snaps, "macd_signal"), signal)
    assert_matches(column(snaps, "macd_hist"), macd - signal)


def reference_wilder_rsi(close: pd.Series, period: int = 14) -> pd.Series:
    """Textbook Wilder RSI, written as a plain loop (independent of the engine)."""
    out = [math.nan] * len(close)
    deltas = close.diff().to_numpy()
    gains = np.clip(deltas, 0, None)
    losses = np.clip(-deltas, 0, None)
    avg_gain = gains[1 : period + 1].mean()
    avg_loss = losses[1 : period + 1].mean()
    for i in range(period, len(close)):
        if i > period:
            avg_gain = (avg_gain * (period - 1) + gains[i]) / period
            avg_loss = (avg_loss * (period - 1) + losses[i]) / period
        out[i] = 100.0 if avg_loss == 0 else 100 - 100 / (1 + avg_gain / avg_loss)
    return pd.Series(out)


def test_rsi(data: tuple[pd.Series, pd.Series, list[IndicatorSnapshot]]) -> None:
    close, _, snaps = data
    assert_matches(column(snaps, "rsi_14"), reference_wilder_rsi(close))
    values = column(snaps, "rsi_14").dropna()
    assert values.between(0, 100).all()


def test_rsi_flat_and_monotonic_series() -> None:
    flat = WilderRsi(3)
    assert [flat.update(10.0) for _ in range(5)][-1] == 50.0
    rising = WilderRsi(3)
    assert [rising.update(float(p)) for p in range(1, 6)][-1] == 100.0


def test_rolling_window_has_no_drift_over_long_runs() -> None:
    window = RollingWindow(20)
    rng = np.random.default_rng(0)
    values = 1e6 + rng.normal(0, 1, 200_000)  # large offset: worst case for running sums
    for v in values:
        window.push(float(v))
    tail = values[-20:]
    assert window.mean() == pytest.approx(tail.mean(), rel=1e-12)
    assert window.std() == pytest.approx(tail.std(ddof=1), rel=1e-9)


def test_rolling_std_is_accurate_for_tiny_moves_at_high_prices() -> None:
    """A 500-dollar stock moving fractions of a cent: classic cancellation trap."""
    window = RollingWindow(20)
    values = 500.0 + np.random.default_rng(1).normal(0, 1e-4, 39)  # 19 updates since resync
    for v in values:
        window.push(float(v))
    assert window.std() == pytest.approx(values[-20:].std(ddof=1), rel=1e-6)


def test_ema_warmup_and_validation() -> None:
    ema = Ema(3)
    assert [ema.update(x) for x in (1.0, 2.0)] == [None, None]
    assert ema.update(3.0) == pytest.approx(2.25)
    with pytest.raises(ValueError, match="span"):
        Ema(0)
    with pytest.raises(ValueError, match="window"):
        RollingWindow(1)


def test_zero_previous_volume_gives_undefined_change() -> None:
    engine = IndicatorEngine()
    engine.update(make_bar(volume=0))
    snap = engine.update(make_bar(timestamp=T0 + timedelta(minutes=1), volume=100))
    assert snap.volume_change_pct is None

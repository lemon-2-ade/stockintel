from __future__ import annotations

import itertools
import math
from datetime import UTC, datetime, timedelta

import numpy as np
import pytest

from market_producer.calibration import DEFAULT_PARAMS, SymbolParams
from market_producer.simulator import (
    TRADING_SECONDS_PER_YEAR,
    AnomalyConfig,
    InjectedAnomaly,
    MarketSimulator,
    SymbolSimulator,
)
from shared.schemas import BarInterval, MarketBarEvent

T0 = datetime(2026, 1, 5, 14, 30, tzinfo=UTC)
PARAMS = SymbolParams(
    start_price=250.0,
    mu_annual=0.10,
    sigma_annual=0.30,
    median_daily_volume=10_000_000,
    log_volume_std=0.3,
)


def run(sim: SymbolSimulator, n: int) -> list[MarketBarEvent]:
    step = sim.interval.duration
    return [sim.next_bar(T0 + i * step).event for i in range(n)]


def make(symbol: str = "AAPL", **kwargs: object) -> SymbolSimulator:
    options: dict[str, object] = {"interval": BarInterval.M1, "seed": 7} | kwargs
    return SymbolSimulator(symbol, PARAMS, **options)  # type: ignore[arg-type]


def test_same_seed_same_path() -> None:
    a = [(b.open, b.high, b.low, b.close, b.volume) for b in run(make(), 200)]
    b = [(b.open, b.high, b.low, b.close, b.volume) for b in run(make(), 200)]
    assert a == b


def test_different_seed_or_symbol_differs() -> None:
    base = [b.close for b in run(make(), 50)]
    assert base != [b.close for b in run(make(seed=8), 50)]
    other = SymbolSimulator("MSFT", PARAMS, interval=BarInterval.M1, seed=7)
    assert base != [b.close for b in run(other, 50)]


def test_symbol_path_independent_of_universe() -> None:
    alone = MarketSimulator([make("AAPL")])
    crowd = MarketSimulator([make("MSFT"), make("AAPL"), make("NVDA")])
    for i in range(20):
        ts = T0 + timedelta(minutes=i)
        a = alone.step(ts)[0].event
        b = next(bar.event for bar in crowd.step(ts) if bar.event.symbol == "AAPL")
        assert (a.close, a.volume) == (b.close, b.volume)


def test_bars_are_valid_and_continuous() -> None:
    bars = run(make(anomalies=AnomalyConfig(price_jump_probability=0.05)), 2_000)
    for prev, bar in itertools.pairwise(bars):
        assert bar.open == prev.close, "each bar opens at the previous close"
        assert bar.low <= min(bar.open, bar.close) <= max(bar.open, bar.close) <= bar.high
        assert bar.volume >= 0
        assert bar.timestamp - prev.timestamp == timedelta(minutes=1)
    assert bars[0].open == PARAMS.start_price


def test_prices_are_rounded_to_cents() -> None:
    for bar in run(make(), 100):
        for price in (bar.open, bar.high, bar.low, bar.close):
            assert round(price, 2) == price


def test_log_returns_match_gbm_parameters() -> None:
    """Statistical check: realised per-bar volatility matches the model within sampling error."""
    sim = make(interval=BarInterval.H1, anomalies=AnomalyConfig.disabled(), drift_annual=0.0)
    closes = np.array([sim.next_bar(T0 + timedelta(hours=i)).event.close for i in range(20_000)])
    log_returns = np.diff(np.log(closes))
    expected = PARAMS.sigma_annual * math.sqrt(3600 / TRADING_SECONDS_PER_YEAR)
    assert np.std(log_returns) == pytest.approx(expected, rel=0.03)
    assert abs(np.mean(log_returns)) < 4 * expected / math.sqrt(len(log_returns))


def test_daily_bars_carry_one_session_of_variance() -> None:
    """Regression: a 1d bar used to get 24 h of variance (sigma x1.9 vs real daily data)."""
    daily = make(interval=BarInterval.D1, anomalies=AnomalyConfig.disabled())
    assert daily.bar_sigma == pytest.approx(PARAMS.sigma_annual / math.sqrt(252))


def test_volatility_multiplier_scales_moves() -> None:
    calm = make(volatility_multiplier=1.0, anomalies=AnomalyConfig.disabled())
    wild = make(volatility_multiplier=5.0, anomalies=AnomalyConfig.disabled())
    assert wild.bar_sigma == pytest.approx(5 * calm.bar_sigma)


def test_volume_scales_with_bar_length() -> None:
    no_anoms = AnomalyConfig.disabled()
    minute = np.median([b.volume for b in run(make(anomalies=no_anoms), 3_000)])
    expected = PARAMS.median_daily_volume * 60 / (6.5 * 3600)
    # Coupling to |return| inflates the median a little above the calibrated level.
    assert expected <= minute <= 1.6 * expected


def test_no_anomalies_when_disabled() -> None:
    sim = make(anomalies=AnomalyConfig.disabled())
    assert all(not sim.next_bar(T0 + timedelta(minutes=i)).injected for i in range(5_000))


def test_injected_price_jumps_are_labelled_and_visible() -> None:
    sim = make(
        anomalies=AnomalyConfig(price_jump_probability=0.02, volume_spike_probability=0.0),
        interval=BarInterval.S1,
    )
    jumps = []
    for i in range(20_000):
        bar = sim.next_bar(T0 + timedelta(seconds=i))
        if bar.injected:
            jumps.append(bar)
    assert 250 < len(jumps) < 550  # ~400 expected at p=0.02
    for bar in jumps:
        (kind,) = bar.injected
        r = math.log(bar.event.close / bar.event.open)
        # A 2-5% jump dwarfs 1-second diffusion noise (~0.01%), so its sign is visible.
        assert (r > 0.015) if kind is InjectedAnomaly.PRICE_SPIKE else (r < -0.015)


def test_injected_volume_spikes() -> None:
    no_spikes = make(anomalies=AnomalyConfig.disabled(), seed=3)
    always = AnomalyConfig(price_jump_probability=0.0, volume_spike_probability=1.0)
    spikes = make(anomalies=always, seed=3)
    normal, spiked = [], []
    for i in range(2_000):
        ts = T0 + timedelta(minutes=i)
        normal.append(no_spikes.next_bar(ts).event.volume)
        bar = spikes.next_bar(ts)
        assert bar.injected == (InjectedAnomaly.VOLUME_SPIKE,)
        spiked.append(bar.event.volume)
    # Multiplier ~ U(5, 15): the median ratio should sit near 10.
    ratio = float(np.median(spiked) / np.median(normal))
    assert 7 < ratio < 13


@pytest.mark.parametrize(
    "kwargs",
    [
        {"price_jump_probability": 1.5},
        {"jump_min": 0.1, "jump_max": 0.05},
        {"volume_min": 0.5},
    ],
)
def test_anomaly_config_validation(kwargs: dict[str, float]) -> None:
    with pytest.raises(ValueError, match=r"require|must be"):
        AnomalyConfig(**kwargs)


def test_simulator_rejects_mixed_intervals() -> None:
    with pytest.raises(ValueError, match="interval"):
        MarketSimulator([make(interval=BarInterval.M1), make("MSFT", interval=BarInterval.S1)])


def test_default_params_are_usable() -> None:
    sim = SymbolSimulator("SIM001", DEFAULT_PARAMS, interval=BarInterval.S1, seed=1)
    assert sim.next_bar(T0).event.symbol == "SIM001"

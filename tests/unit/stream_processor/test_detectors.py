from __future__ import annotations

from datetime import UTC, datetime, timedelta

import numpy as np
import pytest
from factories import make_bar

from shared.schemas import AnomalyType, MarketBarEvent, Severity
from stream_processor.detectors import Detection, DetectorConfig, SymbolDetectors, severity_for

T0 = datetime(2026, 1, 5, 14, 30, tzinfo=UTC)


def bar(i: int, close: float, volume: int = 10_000) -> MarketBarEvent:
    return make_bar(
        timestamp=T0 + timedelta(minutes=i),
        open=close,
        high=close,
        low=close,
        close=close,
        volume=volume,
    )


def feed(
    det: SymbolDetectors, closes: list[float], volumes: list[int] | None = None
) -> list[list[Detection]]:
    volumes = volumes or [10_000] * len(closes)
    found: list[list[Detection]] = []
    prev: float | None = None
    for i, (c, v) in enumerate(zip(closes, volumes, strict=True)):
        found.append(det.evaluate(bar(i, c, v), prev))
        prev = c
    return found


def noisy_prices(n: int, seed: int = 1, sigma: float = 0.001) -> list[float]:
    rng = np.random.default_rng(seed)
    return list(np.round(100 * np.exp(np.cumsum(rng.normal(0, sigma, n))), 4))


def test_return_threshold_detects_spikes_and_drops() -> None:
    det = SymbolDetectors(DetectorConfig(return_threshold=0.01))
    results = feed(det, [100.0, 100.5, 103.0, 101.0])
    assert results[0] == []
    assert results[1] == []
    (spike,) = [d for d in results[2] if d.detector == "return_threshold"]
    assert spike.anomaly_type is AnomalyType.PRICE_SPIKE
    assert spike.observed == pytest.approx(103 / 100.5 - 1)
    (drop,) = [d for d in results[3] if d.detector == "return_threshold"]
    assert drop.anomaly_type is AnomalyType.PRICE_DROP


def test_quiet_series_raises_nothing() -> None:
    det = SymbolDetectors()
    results = feed(det, noisy_prices(2_000))
    assert sum(len(r) for r in results) == 0


def test_zscore_flags_outlier_relative_to_recent_volatility() -> None:
    # 0.3% move: far below the 1% absolute threshold, but ~30 sigma for this series.
    prices = noisy_prices(200, sigma=0.0001)
    prices.append(prices[-1] * 1.003)
    det = SymbolDetectors()
    last = feed(det, prices)[-1]
    assert [d.detector for d in last] == ["return_zscore"]
    assert last[0].severity is Severity.CRITICAL


def test_zscore_needs_minimum_history() -> None:
    det = SymbolDetectors(DetectorConfig(zscore_min_samples=50))
    prices = noisy_prices(30, sigma=0.0001)
    prices.append(prices[-1] * 1.003)
    assert all(d.detector != "return_zscore" for d in feed(det, prices)[-1])


def test_outlier_does_not_mask_the_next_one() -> None:
    """Winsorised baseline: a second jump shortly after the first is still caught."""
    prices = noisy_prices(200, sigma=0.0001)
    prices.append(prices[-1] * 1.003)
    prices += [prices[-1] * (1 + 0.00005 * (-1) ** k) for k in range(10)]
    prices.append(prices[-1] * 0.997)
    results = feed(SymbolDetectors(), prices)
    assert any(d.detector == "return_zscore" for d in results[-1])


def test_volume_spike_against_median_baseline() -> None:
    volumes = [10_000 + (i % 7) * 100 for i in range(60)] + [80_000]
    det = SymbolDetectors()
    last = feed(det, [100.0] * len(volumes), volumes)[-1]
    (spike,) = last
    assert spike.anomaly_type is AnomalyType.VOLUME_SPIKE
    assert spike.expected == pytest.approx(10_300)
    assert spike.score == pytest.approx(80_000 / 10_300)


def test_median_baseline_is_robust_to_earlier_spikes() -> None:
    volumes = [10_000] * 40 + [200_000] * 5 + [10_000] * 10 + [70_000]
    last = feed(SymbolDetectors(), [100.0] * len(volumes), volumes)[-1]
    assert [d.anomaly_type for d in last] == [AnomalyType.VOLUME_SPIKE]


@pytest.mark.parametrize(
    ("score", "severity"),
    [(1.1, Severity.LOW), (2.0, Severity.MEDIUM), (3.0, Severity.HIGH), (5.0, Severity.CRITICAL)],
)
def test_severity_bands(score: float, severity: Severity) -> None:
    assert severity_for(score, 1.0) is severity


@pytest.mark.parametrize(
    "kwargs",
    [{"return_threshold": 0}, {"volume_ratio_threshold": 1.0}, {"zscore_min_samples": 500}],
)
def test_config_validation(kwargs: dict[str, float]) -> None:
    with pytest.raises(ValueError, match=r"must|require"):
        DetectorConfig(**kwargs)  # type: ignore[arg-type]

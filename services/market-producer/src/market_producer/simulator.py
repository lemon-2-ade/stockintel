"""Market price simulator: geometric Brownian motion with injected anomalies.

Model
-----
For each symbol, the log price follows GBM, simulated on ``substeps`` points
inside every bar so that high/low are the extremes of an actual path (not
independent random numbers)::

    ln S(t + h) = ln S(t) + (mu - sigma^2 / 2) h + sigma sqrt(h) Z,   Z ~ N(0, 1)

with ``h`` measured in trading years (252 sessions x 6.5 h). Consecutive bars
are continuous: each bar opens at the previous close.

Volume is log-normal around the calibrated median, scaled to the bar length,
and rises with the size of the bar's move (the empirical volume/volatility
relationship).

Anomalies (for demonstrating and evaluating detectors) are rare, controlled
events:

* **price spike / drop**: a jump of ``U(jump_min, jump_max)`` in log price at a
  random point inside the bar. The jump *persists* (a level shift, as in
  Merton's jump-diffusion) and comes with elevated volume.
* **volume spike**: volume multiplied by ``U(volume_min, volume_max)`` with no
  price effect.

Every injected anomaly is returned alongside the bar as ground truth, so
detectors can later be scored for precision/recall. It is never written into
the market event itself: a real feed would not carry it.

Determinism: each symbol has its own RNG seeded from ``(seed, crc32(symbol))``,
so a symbol's path depends only on the seed and its own history, not on which
other symbols are simulated.
"""

from __future__ import annotations

import math
import zlib
from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum

import numpy as np

from market_producer.calibration import SymbolParams
from market_producer.providers.base import ProducedBar
from shared.schemas import BarInterval, MarketBarEvent

TRADING_SECONDS_PER_DAY = 6.5 * 3600
TRADING_SECONDS_PER_YEAR = 252 * TRADING_SECONDS_PER_DAY
MIN_PRICE = 0.01


class InjectedAnomaly(StrEnum):
    PRICE_SPIKE = "price_spike"
    PRICE_DROP = "price_drop"
    VOLUME_SPIKE = "volume_spike"


@dataclass(frozen=True, slots=True)
class AnomalyConfig:
    price_jump_probability: float = 0.001
    jump_min: float = 0.02
    jump_max: float = 0.05
    volume_spike_probability: float = 0.002
    volume_min: float = 5.0
    volume_max: float = 15.0

    def __post_init__(self) -> None:
        for name in ("price_jump_probability", "volume_spike_probability"):
            if not 0 <= getattr(self, name) <= 1:
                raise ValueError(f"{name} must be in [0, 1]")
        if not 0 < self.jump_min <= self.jump_max < 1:
            raise ValueError("require 0 < jump_min <= jump_max < 1")
        if not 1 < self.volume_min <= self.volume_max:
            raise ValueError("require 1 < volume_min <= volume_max")

    @classmethod
    def disabled(cls) -> AnomalyConfig:
        return cls(price_jump_probability=0.0, volume_spike_probability=0.0)


class SymbolSimulator:
    """Stateful GBM path for one symbol; call :meth:`next_bar` once per interval."""

    VOLUME_VOLATILITY_COUPLING = 0.3
    JUMP_VOLUME_BOOST = (2.0, 4.0)

    def __init__(
        self,
        symbol: str,
        params: SymbolParams,
        *,
        interval: BarInterval,
        seed: int,
        source: str = "simulator",
        volatility_multiplier: float = 1.0,
        drift_annual: float | None = 0.0,
        anomalies: AnomalyConfig | None = None,
        substeps: int = 10,
    ) -> None:
        if volatility_multiplier <= 0:
            raise ValueError("volatility_multiplier must be > 0")
        if substeps < 2:
            raise ValueError("substeps must be >= 2")
        self.symbol = symbol
        self.interval = interval
        self.source = source
        self.anomalies = anomalies or AnomalyConfig()
        self._rng = np.random.default_rng([seed, zlib.crc32(symbol.encode())])
        self._price = round(params.start_price, 2)
        self._substeps = substeps

        # A bar never spans more than one trading session: a daily bar carries
        # one session of variance (6.5 h), not 24 h of it.
        seconds = min(interval.duration.total_seconds(), TRADING_SECONDS_PER_DAY)
        self._bar_years = seconds / TRADING_SECONDS_PER_YEAR
        self._sigma = params.sigma_annual * volatility_multiplier
        mu = params.mu_annual if drift_annual is None else drift_annual
        sub_h = self._bar_years / substeps
        self._sub_drift = (mu - 0.5 * self._sigma**2) * sub_h
        self._sub_vol = self._sigma * math.sqrt(sub_h)
        self._bar_sigma = self._sigma * math.sqrt(self._bar_years)
        # Share volume for one bar: the daily median scaled by the bar's share of a session.
        self._median_volume = params.median_daily_volume * min(
            1.0, seconds / TRADING_SECONDS_PER_DAY
        )
        self._log_volume_std = params.log_volume_std

    @property
    def price(self) -> float:
        return self._price

    @property
    def bar_sigma(self) -> float:
        """Std-dev of one bar's log return (before jumps)."""
        return self._bar_sigma

    def next_bar(self, timestamp: datetime, *, trace_id: str | None = None) -> ProducedBar:
        rng = self._rng
        injected: list[InjectedAnomaly] = []

        increments = self._sub_drift + self._sub_vol * rng.standard_normal(self._substeps)
        if rng.random() < self.anomalies.price_jump_probability:
            size = rng.uniform(self.anomalies.jump_min, self.anomalies.jump_max)
            up = bool(rng.random() < 0.5)
            increments[rng.integers(self._substeps)] += size if up else -size
            injected.append(InjectedAnomaly.PRICE_SPIKE if up else InjectedAnomaly.PRICE_DROP)

        open_ = self._price
        path = open_ * np.exp(np.cumsum(increments))
        close = max(MIN_PRICE, round(float(path[-1]), 2))
        # Rounding is monotonic, so rounding the extremes preserves OHLC ordering.
        high = max(open_, close, round(float(path.max()), 2))
        low = max(MIN_PRICE, min(open_, close, round(float(path.min()), 2)))

        bar_return = math.log(close / open_)
        z = abs(bar_return) / self._bar_sigma if self._bar_sigma > 0 else 0.0
        volume = self._median_volume * math.exp(self._log_volume_std * rng.standard_normal())
        volume *= 1.0 + self.VOLUME_VOLATILITY_COUPLING * min(z, 10.0)
        if injected:
            volume *= rng.uniform(*self.JUMP_VOLUME_BOOST)
        if rng.random() < self.anomalies.volume_spike_probability:
            volume *= rng.uniform(self.anomalies.volume_min, self.anomalies.volume_max)
            injected.append(InjectedAnomaly.VOLUME_SPIKE)

        self._price = close
        event = MarketBarEvent(
            source=self.source,
            trace_id=trace_id,
            symbol=self.symbol,
            timestamp=timestamp,
            interval=self.interval,
            open=open_,
            high=high,
            low=low,
            close=close,
            volume=max(0, round(volume)),
        )
        return ProducedBar(event=event, injected=tuple(injected))


class MarketSimulator:
    """A universe of independent symbol simulators advanced in lock-step."""

    def __init__(self, simulators: list[SymbolSimulator]) -> None:
        if not simulators:
            raise ValueError("at least one symbol is required")
        intervals = {s.interval for s in simulators}
        if len(intervals) != 1:
            raise ValueError("all symbols must share one bar interval")
        self._simulators = simulators
        self.interval = simulators[0].interval

    @property
    def symbols(self) -> list[str]:
        return [s.symbol for s in self._simulators]

    def step(self, timestamp: datetime) -> list[ProducedBar]:
        return [s.next_bar(timestamp) for s in self._simulators]

"""Incremental technical indicators with O(1) work and bounded memory per bar.

Nothing here ever recomputes over a history DataFrame. Each primitive keeps
just the state it needs (a fixed-size window or a few running values) and is
updated once per bar. Values are ``None`` until the primitive has warmed up.

Conventions (pinned by tests against pandas reference implementations):

* SMA / rolling std: simple window; std of returns uses ``ddof=1``, Bollinger
  bands use the population std (``ddof=0``) as in Bollinger's definition.
* EMA: ``alpha = 2 / (span + 1)``, seeded with the first value (pandas
  ``ewm(adjust=False)``); reported once ``span`` values have been seen.
* RSI: Wilder smoothing, seeded with the simple mean of the first ``period``
  gains/losses.
* MACD: EMA(12) - EMA(26); signal = EMA(9) of MACD; histogram = MACD - signal.
"""

from __future__ import annotations

import math
from collections import deque
from dataclasses import dataclass

from shared.schemas import IndicatorSnapshot, MarketBarEvent

INDICATOR_VERSION = "1.0.0"


class RollingWindow:
    """Fixed-size window with O(1) mean/variance via running sums.

    Two numerical safeguards make the running sums trustworthy:

    * **Shifted data**: sums are kept for ``x - shift`` where ``shift`` is a
      recent mean. Prices sit far from zero relative to their variance (a
      300-dollar stock moving cents per second), and the naive
      ``sum(x^2) - n * mean^2`` formula then loses most of its significant
      digits to cancellation.
    * **Periodic resync**: every ``size`` updates the sums (and the shift) are
      recomputed exactly from the window, so rounding error cannot accumulate
      over millions of bars. Still O(1) amortised per update.
    """

    __slots__ = ("_shift", "_since_resync", "_sum", "_sumsq", "_values", "size")

    def __init__(self, size: int) -> None:
        if size < 2:
            raise ValueError("window size must be >= 2")
        self.size = size
        self._values: deque[float] = deque(maxlen=size)
        self._shift: float | None = None
        self._sum = 0.0
        self._sumsq = 0.0
        self._since_resync = 0

    def push(self, value: float) -> None:
        if self._shift is None:
            self._shift = value
        if len(self._values) == self.size:
            old = self._values[0] - self._shift
            self._sum -= old
            self._sumsq -= old * old
        self._values.append(value)
        dev = value - self._shift
        self._sum += dev
        self._sumsq += dev * dev
        self._since_resync += 1
        if self._since_resync >= self.size:
            self._resync()

    def _resync(self) -> None:
        self._shift = math.fsum(self._values) / len(self._values)
        devs = [v - self._shift for v in self._values]
        self._sum = math.fsum(devs)
        self._sumsq = math.fsum(d * d for d in devs)
        self._since_resync = 0

    @property
    def full(self) -> bool:
        return len(self._values) == self.size

    def __len__(self) -> int:
        return len(self._values)

    def values(self) -> tuple[float, ...]:
        return tuple(self._values)

    def mean(self) -> float | None:
        n = len(self._values)
        if not n or self._shift is None:
            return None
        return self._shift + self._sum / n

    def std(self, ddof: int = 1) -> float | None:
        n = len(self._values)
        if n - ddof <= 0:
            return None
        dev_mean = self._sum / n
        variance = (self._sumsq - n * dev_mean * dev_mean) / (n - ddof)
        return math.sqrt(max(variance, 0.0))  # clamp tiny negative rounding error


class Ema:
    __slots__ = ("_alpha", "_count", "_value", "span")

    def __init__(self, span: int) -> None:
        if span < 1:
            raise ValueError("span must be >= 1")
        self.span = span
        self._alpha = 2.0 / (span + 1)
        self._value: float | None = None
        self._count = 0

    def update(self, x: float) -> float | None:
        self._value = x if self._value is None else self._value + self._alpha * (x - self._value)
        self._count += 1
        return self.value

    @property
    def value(self) -> float | None:
        return self._value if self._count >= self.span else None


class WilderRsi:
    __slots__ = ("_avg_gain", "_avg_loss", "_count", "_prev", "_seed_gain", "_seed_loss", "period")

    def __init__(self, period: int = 14) -> None:
        self.period = period
        self._prev: float | None = None
        self._count = 0
        self._seed_gain = 0.0
        self._seed_loss = 0.0
        self._avg_gain = 0.0
        self._avg_loss = 0.0

    def update(self, price: float) -> float | None:
        if self._prev is None:
            self._prev = price
            return None
        change = price - self._prev
        self._prev = price
        gain, loss = max(change, 0.0), max(-change, 0.0)
        self._count += 1
        if self._count <= self.period:
            self._seed_gain += gain
            self._seed_loss += loss
            if self._count < self.period:
                return None
            self._avg_gain = self._seed_gain / self.period
            self._avg_loss = self._seed_loss / self.period
        else:
            n = self.period
            self._avg_gain = (self._avg_gain * (n - 1) + gain) / n
            self._avg_loss = (self._avg_loss * (n - 1) + loss) / n
        if self._avg_loss == 0.0:
            return 100.0 if self._avg_gain > 0 else 50.0
        rs = self._avg_gain / self._avg_loss
        return 100.0 - 100.0 / (1.0 + rs)


class Macd:
    __slots__ = ("_fast", "_signal", "_slow")

    def __init__(self, fast: int = 12, slow: int = 26, signal: int = 9) -> None:
        self._fast = Ema(fast)
        self._slow = Ema(slow)
        self._signal = Ema(signal)

    def update(self, price: float) -> tuple[float | None, float | None, float | None]:
        fast = self._fast.update(price)
        slow = self._slow.update(price)
        if fast is None or slow is None:
            return None, None, None
        macd = fast - slow
        signal = self._signal.update(macd)
        return macd, signal, (macd - signal) if signal is not None else None

    @property
    def fast(self) -> float | None:
        return self._fast.value

    @property
    def slow(self) -> float | None:
        return self._slow.value


@dataclass(frozen=True, slots=True)
class IndicatorConfig:
    window: int = 20
    bollinger_k: float = 2.0
    rsi_period: int = 14
    macd_fast: int = 12
    macd_slow: int = 26
    macd_signal: int = 9


class IndicatorEngine:
    """All indicators for one symbol; feed it bars in event-time order."""

    def __init__(self, config: IndicatorConfig | None = None) -> None:
        cfg = config or IndicatorConfig()
        self._cfg = cfg
        self._prices = RollingWindow(cfg.window)
        self._log_returns = RollingWindow(cfg.window)
        self._volumes = RollingWindow(cfg.window)
        self._rsi = WilderRsi(cfg.rsi_period)
        self._macd = Macd(cfg.macd_fast, cfg.macd_slow, cfg.macd_signal)
        self._prev_close: float | None = None
        self._prev_volume: int | None = None

    @property
    def previous_close(self) -> float | None:
        return self._prev_close

    def update(self, bar: MarketBarEvent) -> IndicatorSnapshot:
        close, volume = bar.close, bar.volume
        prev_close, prev_volume = self._prev_close, self._prev_volume

        ret = log_ret = change = None
        if prev_close is not None:
            change = close - prev_close
            ret = change / prev_close
            log_ret = math.log(close / prev_close)
            self._log_returns.push(log_ret)
        volume_change = (
            (volume - prev_volume) / prev_volume if prev_volume else None
        )  # None when the previous volume was 0 (undefined ratio)

        self._prices.push(close)
        self._volumes.push(float(volume))
        rsi = self._rsi.update(close)
        macd, signal, hist = self._macd.update(close)
        self._prev_close, self._prev_volume = close, volume

        sma = self._prices.mean() if self._prices.full else None
        bb_std = self._prices.std(ddof=0) if self._prices.full else None
        upper = lower = width = None
        if sma is not None and bb_std is not None:
            upper = sma + self._cfg.bollinger_k * bb_std
            lower = sma - self._cfg.bollinger_k * bb_std
            width = (upper - lower) / sma
        volume_sma = self._volumes.mean() if self._volumes.full else None

        return IndicatorSnapshot(
            return_1=ret,
            log_return_1=log_ret,
            price_change=change,
            volume_change_pct=volume_change,
            sma_20=sma,
            ema_12=self._macd.fast,
            ema_26=self._macd.slow,
            volatility_20=self._log_returns.std(ddof=1) if self._log_returns.full else None,
            volume_sma_20=volume_sma,
            volume_ratio=(volume / volume_sma) if volume_sma else None,
            rsi_14=rsi,
            macd=macd,
            macd_signal=signal,
            macd_hist=hist,
            bb_upper=upper,
            bb_middle=sma,
            bb_lower=lower,
            bb_width=width,
        )

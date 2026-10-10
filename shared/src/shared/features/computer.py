"""The feature computer: one incremental, causal implementation for training and serving.

All features are **scale-free** (returns, ratios, normalised distances,
bounded oscillators), so they mean the same thing for a $20 and a $2,000 stock
and, as far as possible, for daily and intraday bars.

Gap policy: a bar that follows missing sessions (``gap_before > 0``) resets
the state, because a "1-bar return" across a gap is not a 1-bar return. The
computer then warms up again; rows are not usable until every feature exists.
"""

from __future__ import annotations

import math
from collections import deque
from dataclasses import dataclass
from datetime import datetime
from typing import Final

from shared.features.rolling import Ema, WilderAverage, Window
from shared.schemas import MarketBarEvent

FEATURE_SET_VERSION: Final = "fs-1.0.0"

FEATURE_NAMES: Final[tuple[str, ...]] = (
    # returns and lags (log)
    "ret_1",
    "ret_5",
    "ret_10",
    "ret_20",
    "ret_lag_1",
    "ret_lag_2",
    "ret_lag_3",
    "ret_lag_4",
    # bar shape
    "gap",
    "hl_range",
    "close_pos",
    # trend
    "close_sma10",
    "close_sma20",
    "close_sma50",
    "sma10_sma50",
    "macd_norm",
    "macd_hist_norm",
    # volatility
    "vol_10",
    "vol_20",
    "vol_ratio_10_60",
    "atr_14_norm",
    "bb_width_20",
    "bb_pos_20",
    # momentum
    "rsi_14",
    # volume
    "volume_ratio_20",
    "volume_change",
    # calendar
    "day_of_week",
    "month",
)

WARMUP_BARS: Final = 61
"""Bars needed before every feature exists (60 log returns for ``vol_ratio_10_60``)."""


@dataclass(frozen=True, slots=True)
class BarInput:
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    gap_before: int = 0

    @classmethod
    def from_event(cls, event: MarketBarEvent, gap_before: int = 0) -> BarInput:
        return cls(
            timestamp=event.timestamp,
            open=event.open,
            high=event.high,
            low=event.low,
            close=event.close,
            volume=float(event.volume),
            gap_before=gap_before,
        )


Features = dict[str, float | None]


class FeatureComputer:
    """Per-symbol feature state. Feed bars in time order; read features after each."""

    def __init__(self) -> None:
        self._reset()

    def _reset(self) -> None:
        self._closes: deque[float] = deque(maxlen=21)
        self._log_returns: deque[float] = deque(maxlen=5)
        self._vol10, self._vol20, self._vol60 = Window(10), Window(20), Window(60)
        self._sma10, self._sma20, self._sma50 = Window(10), Window(20), Window(50)
        self._ema12, self._ema26, self._signal = Ema(12), Ema(26), Ema(9)
        self._atr = WilderAverage(14)
        self._gain, self._loss = WilderAverage(14), WilderAverage(14)
        self._volume20 = Window(20)
        self._prev_volume: float | None = None
        self.bars_seen = 0

    @property
    def ready(self) -> bool:
        return self.bars_seen >= WARMUP_BARS

    def update(self, bar: BarInput) -> Features:
        if bar.gap_before > 0:
            self._reset()
        self.bars_seen += 1
        prev_close = self._closes[-1] if self._closes else None
        f: Features = dict.fromkeys(FEATURE_NAMES)
        if prev_close is not None:
            self._update_returns(f, bar, prev_close)
        self._update_horizons(f, bar.close)
        bar_range = bar.high - bar.low
        f["hl_range"] = bar_range / bar.close
        f["close_pos"] = (bar.close - bar.low) / bar_range if bar_range > 0 else 0.5
        self._update_trend(f, bar.close)
        self._update_volatility(f, bar.close)
        self._update_volume(f, bar.volume)
        f["day_of_week"] = float(bar.timestamp.weekday())
        f["month"] = float(bar.timestamp.month)
        return f

    def _update_returns(self, f: Features, bar: BarInput, prev_close: float) -> None:
        log_ret = math.log(bar.close / prev_close)
        self._log_returns.appendleft(log_ret)  # [0] = newest
        for window in (self._vol10, self._vol20, self._vol60):
            window.push(log_ret)
        f["ret_1"] = log_ret
        f["gap"] = math.log(bar.open / prev_close)
        true_range = max(bar.high, prev_close) - min(bar.low, prev_close)
        atr = self._atr.update(true_range)
        f["atr_14_norm"] = atr / bar.close if atr is not None else None
        gain = self._gain.update(max(bar.close - prev_close, 0.0))
        loss = self._loss.update(max(prev_close - bar.close, 0.0))
        if gain is not None and loss is not None:
            if loss == 0:
                f["rsi_14"] = 1.0 if gain > 0 else 0.5
            else:
                f["rsi_14"] = 1 - 1 / (1 + gain / loss)

    def _update_horizons(self, f: Features, close: float) -> None:
        self._closes.append(close)
        for k, name in ((5, "ret_5"), (10, "ret_10"), (20, "ret_20")):
            if len(self._closes) > k:
                f[name] = math.log(close / self._closes[-1 - k])
        for lag in range(1, 5):
            if len(self._log_returns) > lag:
                f[f"ret_lag_{lag}"] = self._log_returns[lag]

    def _update_trend(self, f: Features, close: float) -> None:
        for window, name in (
            (self._sma10, "close_sma10"),
            (self._sma20, "close_sma20"),
            (self._sma50, "close_sma50"),
        ):
            window.push(close)
            mean = window.mean()
            f[name] = close / mean - 1 if mean else None
        sma10, sma50 = self._sma10.mean(), self._sma50.mean()
        f["sma10_sma50"] = sma10 / sma50 - 1 if sma10 and sma50 else None
        fast, slow = self._ema12.update(close), self._ema26.update(close)
        if fast is not None and slow is not None:
            macd = fast - slow
            f["macd_norm"] = macd / close
            signal = self._signal.update(macd)
            f["macd_hist_norm"] = (macd - signal) / close if signal is not None else None

    def _update_volatility(self, f: Features, close: float) -> None:
        vol10, vol60 = self._vol10.std(), self._vol60.std()
        f["vol_10"] = vol10
        f["vol_20"] = self._vol20.std()
        f["vol_ratio_10_60"] = vol10 / vol60 if vol10 is not None and vol60 else None
        mid, std0 = self._sma20.mean(), self._sma20.std(ddof=0)
        if mid and std0 is not None:
            f["bb_width_20"] = 4 * std0 / mid
            f["bb_pos_20"] = (close - (mid - 2 * std0)) / (4 * std0) if std0 > 0 else 0.5

    def _update_volume(self, f: Features, volume: float) -> None:
        # log1p-style ratios keep zero-volume sessions finite.
        self._volume20.push(volume)
        mean = self._volume20.mean()
        f["volume_ratio_20"] = math.log((volume + 1) / (mean + 1)) if mean is not None else None
        if self._prev_volume is not None:
            f["volume_change"] = math.log((volume + 1) / (self._prev_volume + 1))
        self._prev_volume = volume

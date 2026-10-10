"""Per-symbol feature state for the live stream.

Uses the same ``FeatureComputer`` that built the training set, so a feature
value served online is computed by exactly the code that produced it offline.

The input topic (``market.enriched``) is already de-duplicated and ordered per
symbol by the stream processor; this layer still refuses non-increasing
timestamps defensively (a replay, a second producer) rather than corrupting
the rolling state.
"""

from __future__ import annotations

from collections import OrderedDict
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta

from shared.features import FEATURE_NAMES, BarInput, FeatureComputer
from shared.schemas import BarInterval, EnrichedBarEvent


def weekdays_between(start: date, end: date) -> int:
    """Weekdays strictly after ``start`` up to and including ``end``."""
    if end <= start:
        return 0
    full_weeks, rest = divmod((end - start).days, 7)
    count = full_weeks * 5
    day = start + timedelta(days=full_weeks * 7)
    for _ in range(rest):
        day += timedelta(days=1)
        count += day.weekday() < 5
    return count


def missing_bars(previous: datetime, current: datetime, interval: BarInterval) -> int:
    """Bars absent between two consecutive bars of a symbol.

    Daily bars count weekdays, so a weekend is not a gap (exchange holidays
    still are: one reset per holiday is the accepted cost of not shipping an
    exchange calendar). Intraday bars use the fixed duration.
    """
    if interval is BarInterval.D1:
        return max(0, weekdays_between(previous.date(), current.date()) - 1)
    return max(0, round((current - previous) / interval.duration) - 1)


@dataclass(slots=True)
class SymbolState:
    computer: FeatureComputer = field(default_factory=FeatureComputer)
    last_timestamp: datetime | None = None
    bars: int = 0


@dataclass(frozen=True, slots=True)
class FeatureUpdate:
    features: dict[str, float] | None
    """All features when the computer is warmed up, else None."""
    gap: int = 0
    stale: bool = False
    """True when the bar was not newer than the last one (ignored)."""


class FeatureStore:
    """Bounded LRU of per-symbol feature computers."""

    def __init__(self, max_symbols: int = 10_000) -> None:
        self._states: OrderedDict[str, SymbolState] = OrderedDict()
        self.max_symbols = max_symbols

    def __len__(self) -> int:
        return len(self._states)

    def update(self, bar: EnrichedBarEvent) -> FeatureUpdate:
        state = self._states.get(bar.symbol)
        if state is None:
            state = self._states[bar.symbol] = SymbolState()
            if len(self._states) > self.max_symbols:
                self._states.popitem(last=False)
        self._states.move_to_end(bar.symbol)

        gap = 0
        if state.last_timestamp is not None:
            if bar.timestamp <= state.last_timestamp:
                return FeatureUpdate(None, stale=True)
            gap = missing_bars(state.last_timestamp, bar.timestamp, bar.interval)
        state.last_timestamp = bar.timestamp
        state.bars += 1
        values = state.computer.update(
            BarInput(
                timestamp=bar.timestamp,
                open=bar.open,
                high=bar.high,
                low=bar.low,
                close=bar.close,
                volume=float(bar.volume),
                gap_before=gap,
            )
        )
        if not state.computer.ready:
            return FeatureUpdate(None, gap=gap)
        complete = {name: values[name] for name in FEATURE_NAMES}
        if any(v is None for v in complete.values()):
            return FeatureUpdate(None, gap=gap)
        return FeatureUpdate({k: float(v) for k, v in complete.items() if v is not None}, gap=gap)

    def drop(self, symbols: set[str]) -> None:
        for symbol in symbols:
            self._states.pop(symbol, None)

    def bars_seen(self, symbol: str) -> int:
        state = self._states.get(symbol)
        return state.bars if state else 0

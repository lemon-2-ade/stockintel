"""Per-symbol state and the pure bar-processing step (no Kafka in here).

Event-time policy
-----------------
Indicators are defined over a symbol's bars *in event-time order*, and Kafka
only guarantees order per partition, i.e. as produced. So, per symbol:

* **duplicate**: an ``event_id`` seen recently, or a second bar for the
  timestamp already processed (e.g. a producer resend with a new id). Dropped:
  folding it in twice would corrupt every rolling window.
* **late / out of order**: ``timestamp`` earlier than the last processed bar.
  Not folded into indicator state (that would need retraction of already
  published values) and no enriched event is emitted. The raw bar is still
  persisted by the separate persistence consumer, so history is complete;
  only real-time analytics skip it. Counted in a metric.
* **gap**: bars missing between the previous and current timestamp. Counted;
  indicators continue without imputation (inventing prices would be worse).
* **interval change**: state for the symbol is reset.

State is bounded: fixed-size windows per symbol, a capped set of recent
event ids, and at most ``max_symbols`` symbols (least recently updated evicted).
"""

from __future__ import annotations

from collections import OrderedDict, deque
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from uuid import UUID

from shared.observability.logs import get_logger
from shared.schemas import (
    AnomalyEvent,
    BarInterval,
    EnrichedBarEvent,
    MarketBarEvent,
    derived_event_id,
    utcnow,
)
from stream_processor.detectors import DETECTOR_VERSION, DetectorConfig, SymbolDetectors
from stream_processor.indicators import INDICATOR_VERSION, IndicatorConfig, IndicatorEngine

log = get_logger(__name__)

RECENT_IDS_PER_SYMBOL = 256


class Outcome(StrEnum):
    PROCESSED = "processed"
    DUPLICATE = "duplicate"
    LATE = "late"


@dataclass(slots=True)
class ProcessResult:
    outcome: Outcome
    enriched: EnrichedBarEvent | None = None
    anomalies: list[AnomalyEvent] = field(default_factory=list)
    missing_bars: int = 0


class SymbolState:
    __slots__ = ("detectors", "engine", "interval", "last_timestamp", "recent_ids", "recent_order")

    def __init__(
        self, interval: BarInterval, indicators: IndicatorConfig, detectors: DetectorConfig
    ) -> None:
        self.interval = interval
        self.engine = IndicatorEngine(indicators)
        self.detectors = SymbolDetectors(detectors)
        self.last_timestamp: datetime | None = None
        self.recent_ids: set[UUID] = set()
        self.recent_order: deque[UUID] = deque()

    def remember(self, event_id: UUID) -> None:
        self.recent_ids.add(event_id)
        self.recent_order.append(event_id)
        if len(self.recent_order) > RECENT_IDS_PER_SYMBOL:
            self.recent_ids.discard(self.recent_order.popleft())


class BarProcessor:
    def __init__(
        self,
        *,
        indicators: IndicatorConfig | None = None,
        detectors: DetectorConfig | None = None,
        source: str = "stream-processor",
        max_symbols: int = 10_000,
        clock: Callable[[], datetime] = utcnow,
    ) -> None:
        self._indicator_cfg = indicators or IndicatorConfig()
        self._detector_cfg = detectors or DetectorConfig()
        self._source = source
        self._max_symbols = max_symbols
        self._clock = clock
        self._states: OrderedDict[str, SymbolState] = OrderedDict()

    @property
    def tracked_symbols(self) -> int:
        return len(self._states)

    def drop(self, symbols: Iterable[str]) -> None:
        """Forget state, e.g. for partitions revoked in a consumer-group rebalance."""
        for symbol in symbols:
            self._states.pop(symbol, None)

    def _state_for(self, bar: MarketBarEvent) -> SymbolState:
        state = self._states.get(bar.symbol)
        if state is not None and state.interval is not bar.interval:
            log.warning(
                "state.interval_changed",
                symbol=bar.symbol,
                old=state.interval.value,
                new=bar.interval.value,
            )
            state = None
        if state is None:
            state = SymbolState(bar.interval, self._indicator_cfg, self._detector_cfg)
            self._states[bar.symbol] = state
            if len(self._states) > self._max_symbols:
                evicted, _ = self._states.popitem(last=False)
                log.warning("state.symbol_evicted", symbol=evicted)
        self._states.move_to_end(bar.symbol)
        return state

    def process(self, bar: MarketBarEvent) -> ProcessResult:
        state = self._state_for(bar)
        last = state.last_timestamp
        if bar.event_id in state.recent_ids or (last is not None and bar.timestamp == last):
            return ProcessResult(Outcome.DUPLICATE)
        if last is not None and bar.timestamp < last:
            return ProcessResult(Outcome.LATE)

        missing = 0
        if last is not None:
            steps = (bar.timestamp - last) / bar.interval.duration
            missing = max(0, round(steps) - 1)

        detections = state.detectors.evaluate(bar, state.engine.previous_close)
        snapshot = state.engine.update(bar)
        state.last_timestamp = bar.timestamp
        state.remember(bar.event_id)

        latency_ms = max(0.0, (self._clock() - bar.produced_at).total_seconds() * 1000)
        enriched = EnrichedBarEvent.from_bar(
            bar,
            indicators=snapshot,
            indicator_version=INDICATOR_VERSION,
            source=self._source,
            processing_latency_ms=latency_ms,
        )
        anomalies = [
            AnomalyEvent(
                event_id=derived_event_id(bar.event_id, "anomaly", d.detector, d.anomaly_type),
                anomaly_id=derived_event_id(bar.event_id, d.detector, d.anomaly_type),
                source=self._source,
                trace_id=bar.trace_id,
                symbol=bar.symbol,
                timestamp=bar.timestamp,
                interval=bar.interval,
                anomaly_type=d.anomaly_type,
                severity=d.severity,
                observed_value=d.observed,
                expected_value=d.expected,
                score=d.score,
                threshold=d.threshold,
                detector=d.detector,
                detector_version=DETECTOR_VERSION,
                source_event_id=bar.event_id,
                details=d.details,
            )
            for d in detections
        ]
        return ProcessResult(Outcome.PROCESSED, enriched, anomalies, missing)

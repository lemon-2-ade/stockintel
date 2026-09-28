"""The producer main loop: pacing, publishing and graceful shutdown."""

from __future__ import annotations

import dataclasses
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass

from market_producer.metrics import ProducerMetrics
from market_producer.providers.base import MarketDataProvider
from market_producer.publisher import KafkaMarketPublisher
from shared.observability.logs import get_logger

log = get_logger(__name__)


@dataclass(frozen=True, slots=True)
class RunSummary:
    batches: int
    bars: int
    delivered: int
    failed: int
    undelivered_at_exit: int
    stopped_by_signal: bool


class ProducerRuntime:
    """Drive a provider into Kafka until it is exhausted or ``stop`` is set.

    Waiting uses ``stop.wait(timeout)`` rather than ``sleep`` so a SIGTERM is
    honoured immediately, even between widely spaced bars. On exit the
    producer is flushed so acknowledged-or-failed is known for every event.
    """

    def __init__(
        self,
        provider: MarketDataProvider,
        publisher: KafkaMarketPublisher,
        metrics: ProducerMetrics,
        *,
        stop: threading.Event,
        flush_timeout_s: float = 10.0,
        wall_clock: Callable[[], float] = time.time,
    ) -> None:
        self._provider = provider
        self._publisher = publisher
        self._metrics = metrics
        self._stop = stop
        self._flush_timeout_s = flush_timeout_s
        self._now = wall_clock

    def run(self) -> RunSummary:
        batches = bars = 0
        log.info("producer.started", provider=self._provider.name)
        try:
            for batch in self._provider.batches():
                delay = batch.due_at - self._now()
                if delay > 0 and self._stop.wait(delay):
                    break
                if self._stop.is_set():
                    break
                self._metrics.publish_lag.observe(max(0.0, self._now() - batch.due_at))
                for bar in batch.bars:
                    self._publisher.publish(bar)
                bars += len(batch.bars)
                batches += 1
                self._publisher.serve_delivery_reports()
        finally:
            remaining = self._publisher.flush(self._flush_timeout_s)
        summary = RunSummary(
            batches=batches,
            bars=bars,
            delivered=self._publisher.delivered,
            failed=self._publisher.failed,
            undelivered_at_exit=remaining,
            stopped_by_signal=self._stop.is_set(),
        )
        log.info("producer.stopped", **dataclasses.asdict(summary))
        return summary

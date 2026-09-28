"""Publish bars to Kafka with delivery tracking and bounded backpressure.

``produce()`` in librdkafka is asynchronous: it enqueues locally and the
delivery report arrives later via ``poll()``. Success is therefore only
counted in the delivery callback, after the broker acknowledged the write
(``acks=all``), never at enqueue time.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from typing import Any, Protocol

from market_producer.metrics import ProducerMetrics
from market_producer.providers.base import ProducedBar
from shared.kafka.serde import EventSerde
from shared.observability.logs import get_logger

log = get_logger(__name__)

HEADER_INJECTED_ANOMALY = "x-sim-injected-anomaly"


Headers = list[tuple[str, str | bytes | None]]


class KafkaProducerLike(Protocol):
    """The subset of ``confluent_kafka.Producer`` we use (eases testing)."""

    def produce(
        self,
        topic: str,
        *,
        key: bytes,
        value: bytes,
        headers: Headers,
        on_delivery: Callable[[Any, Any], None],
    ) -> None: ...

    def poll(self, timeout: float = ...) -> int: ...

    def flush(self, timeout: float = ...) -> int: ...

    def __len__(self) -> int: ...


class QueueFullError(RuntimeError):
    """The local queue stayed full for longer than the backpressure budget."""


class KafkaMarketPublisher:
    def __init__(
        self,
        producer: KafkaProducerLike,
        *,
        topic: str,
        serde: EventSerde,
        metrics: ProducerMetrics,
        backpressure_timeout_s: float = 30.0,
        clock: Any = time.monotonic,
    ) -> None:
        self._producer = producer
        self._topic = topic
        self._serde = serde
        self._metrics = metrics
        self._backpressure_timeout_s = backpressure_timeout_s
        self._clock = clock
        self.delivered = 0
        self.failed = 0

    def publish(self, bar: ProducedBar) -> None:
        event = bar.event
        headers: Headers = list(self._serde.headers(event))
        if bar.injected:
            headers.append((HEADER_INJECTED_ANOMALY, ",".join(bar.injected).encode()))
            for kind in bar.injected:
                self._metrics.injected_anomalies.labels(type=kind).inc()

        value = self._serde.serialize(event)
        sent_at = self._clock()
        event_id = str(event.event_id)

        def on_delivery(err: Any, _msg: Any) -> None:
            if err is not None:
                self.failed += 1
                self._metrics.delivery_failures.labels(
                    topic=self._topic, reason=str(err.name())
                ).inc()
                log.error(
                    "producer.delivery_failed",
                    event_id=event_id,
                    symbol=event.symbol,
                    error=str(err),
                )
                return
            self.delivered += 1
            self._metrics.delivered.labels(topic=self._topic).inc()
            self._metrics.delivery_latency.observe(self._clock() - sent_at)
            self._metrics.last_delivery.set_to_current_time()

        deadline = sent_at + self._backpressure_timeout_s
        while True:
            try:
                self._producer.produce(
                    self._topic,
                    key=event.partition_key().encode(),
                    value=value,
                    headers=headers,
                    on_delivery=on_delivery,
                )
                break
            except BufferError:
                # Local queue full: the broker is slower than us. Serve delivery
                # reports to drain the queue, then retry, within a bounded budget.
                self._metrics.buffer_full.inc()
                if self._clock() >= deadline:
                    raise QueueFullError(
                        f"producer queue full for {self._backpressure_timeout_s}s"
                    ) from None
                self._producer.poll(0.1)

        self._metrics.bars_generated.labels(symbol=event.symbol).inc()

    def serve_delivery_reports(self) -> None:
        self._producer.poll(0)
        self._metrics.queue_length.set(len(self._producer))

    def flush(self, timeout_s: float) -> int:
        """Block until queued messages are delivered; return how many are still pending."""
        remaining = self._producer.flush(timeout_s)
        self._metrics.queue_length.set(remaining)
        if remaining:
            log.error("producer.flush_incomplete", undelivered=remaining)
        return remaining

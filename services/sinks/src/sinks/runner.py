"""Generic at-least-once batch runner for sinks.

Per ``consume()`` batch:

1. decode; undecodable messages are dead-lettered (and acknowledged first);
2. ``handler.write(events)`` with bounded, jittered retries on transient
   storage errors (database restart, Redis failover);
3. events the handler *rejects* (e.g. a row the database refuses) are
   dead-lettered too;
4. only then are input offsets stored for background commit.

If storage stays unavailable past the retry budget the runner raises
without committing: the process exits, the orchestrator restarts it, and the
batch is re-delivered. Sinks are idempotent, so re-delivery is harmless.
"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from typing import Any, Protocol

from shared.kafka.serde import EventDeserializationError, JsonEventSerde
from shared.kafka.topics import Topic
from shared.observability.logs import get_logger
from shared.schemas import BaseEvent, DeadLetterEvent, FailureReason
from shared.utils.retry import BackoffPolicy, retry_call
from sinks.metrics import SinkMetrics

log = get_logger(__name__)

Headers = list[tuple[str, str | bytes | None]]


class TransientSinkError(RuntimeError):
    """Storage temporarily unavailable; the whole batch may be retried."""


@dataclass(slots=True)
class WriteResult:
    written: dict[str, int] = field(default_factory=dict)
    """Rows/keys actually changed, per target (table or key family)."""
    skipped: dict[str, int] = field(default_factory=dict)
    """Already present (duplicates / stale), per target."""
    rejected: list[tuple[BaseEvent, str]] = field(default_factory=list)
    """Events the storage refused permanently, with the reason."""


class SinkHandler(Protocol):
    name: str
    topics: Sequence[str]

    def write(self, events: Sequence[BaseEvent]) -> WriteResult: ...


class ProducerLike(Protocol):
    def produce(
        self,
        topic: str,
        *,
        key: bytes,
        value: bytes,
        headers: Headers,
        on_delivery: Callable[[Any, Any], None],
    ) -> None: ...
    def flush(self, timeout: float = ...) -> int: ...


@dataclass(frozen=True, slots=True)
class RunnerConfig:
    group_id: str
    batch_size: int = 1_000
    poll_timeout_s: float = 0.5
    flush_timeout_s: float = 10.0
    retry: BackoffPolicy = field(
        default_factory=lambda: BackoffPolicy(max_attempts=6, base_delay_s=0.5, max_delay_s=15.0)
    )


class DeadLetterDeliveryError(RuntimeError):
    """DLQ records were not acknowledged; the batch must not be committed."""


class BatchSinkRunner:
    def __init__(
        self,
        consumer: Any,
        dlq_producer: ProducerLike,
        handler: SinkHandler,
        metrics: SinkMetrics,
        config: RunnerConfig,
        *,
        stop: threading.Event,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self._consumer = consumer
        self._producer = dlq_producer
        self._handler = handler
        self._metrics = metrics
        self._cfg = config
        self._stop = stop
        self._sleep = sleep
        self._serde = JsonEventSerde()
        self._dlq_errors = 0

    def run(self) -> None:
        self._consumer.subscribe(list(self._handler.topics))
        log.info("sink.started", sink=self._handler.name, topics=list(self._handler.topics))
        try:
            while not self._stop.is_set():
                messages = self._consumer.consume(
                    num_messages=self._cfg.batch_size, timeout=self._cfg.poll_timeout_s
                )
                if messages:
                    self.handle_batch(messages)
        finally:
            self._producer.flush(self._cfg.flush_timeout_s)
            self._consumer.close()
            log.info("sink.stopped", sink=self._handler.name)

    def handle_batch(self, messages: Sequence[Any]) -> None:
        events: list[BaseEvent] = []
        dead_letters = 0
        last: dict[tuple[str, int], Any] = {}
        for msg in messages:
            if msg.error():
                log.warning("consumer.message_error", error=str(msg.error()))
                continue
            last[(msg.topic(), msg.partition())] = msg
            try:
                events.append(self._serde.deserialize_any(msg.value()))
            except EventDeserializationError as exc:
                dead_letters += self._dead_letter_raw(msg, exc.reason, exc)

        if events:
            started = time.perf_counter()
            result = retry_call(
                lambda: self._handler.write(events),
                policy=self._cfg.retry,
                retry_on=(TransientSinkError,),
                operation=f"sink.{self._handler.name}.write",
                sleep=self._sleep,
            )
            self._metrics.write_seconds.observe(time.perf_counter() - started)
            for target, count in result.written.items():
                self._metrics.written.labels(target=target).inc(count)
            for target, count in result.skipped.items():
                self._metrics.skipped.labels(target=target).inc(count)
            for event, reason in result.rejected:
                dead_letters += self._dead_letter_event(event, reason)

        if dead_letters:
            remaining = self._producer.flush(self._cfg.flush_timeout_s)
            if remaining or self._dlq_errors:
                raise DeadLetterDeliveryError(
                    "dead-letter records not acknowledged; not committing"
                )
        for msg in last.values():
            self._consumer.store_offsets(message=msg)
        self._metrics.messages.inc(len(messages))

    # ------------------------------------------------------------------ DLQ
    def _on_delivery(self, err: Any, _msg: Any) -> None:
        if err is not None:
            self._dlq_errors += 1
            log.error("dlq.delivery_failed", error=str(err))

    def _publish(self, record: DeadLetterEvent) -> int:
        self._producer.produce(
            Topic.DEAD_LETTER,
            key=record.partition_key().encode(),
            value=self._serde.serialize(record),
            headers=list(self._serde.headers(record)),
            on_delivery=self._on_delivery,
        )
        self._metrics.dead_lettered.labels(reason=record.reason.value).inc()
        log.warning(
            "message.dead_lettered",
            sink=self._handler.name,
            reason=record.reason.value,
            topic=record.original_topic,
            offset=record.original_offset,
            error=record.error_message[:200],
        )
        return 1

    def _dead_letter_raw(self, msg: Any, reason: FailureReason, error: BaseException) -> int:
        return self._publish(
            DeadLetterEvent.from_failure(
                topic=msg.topic(),
                partition=msg.partition(),
                offset=msg.offset(),
                key=msg.key(),
                value=msg.value(),
                reason=reason,
                error=error,
                consumer_group=self._cfg.group_id,
                source=f"sink-{self._handler.name}",
            )
        )

    def _dead_letter_event(self, event: BaseEvent, reason: str) -> int:
        # Offsets of individual rejected events are not tracked through the
        # batch write; the record carries the full event instead (offset 0).
        return self._publish(
            DeadLetterEvent.from_failure(
                topic=f"sink:{self._handler.name}",
                partition=0,
                offset=0,
                key=event.partition_key().encode(),
                value=self._serde.serialize(event),
                reason=FailureReason.PROCESSING,
                error=reason,
                consumer_group=self._cfg.group_id,
                source=f"sink-{self._handler.name}",
            )
        )

"""Kafka consume -> process -> produce loop with at-least-once guarantees.

Batch protocol (per ``consume()`` call):

1. Deserialize each message; invalid ones go to the dead-letter topic.
2. Run the pure :class:`BarProcessor`; produce enriched bars and anomalies.
3. ``flush()`` the producer: wait until *every* output (incl. DLQ records)
   is acknowledged by Kafka.
4. Only then store the input offsets (committed in the background).

A crash anywhere before step 4 re-delivers the batch; outputs carry
deterministic event ids and sinks are idempotent, so that is safe. If an
output cannot be delivered the batch is **not** committed and the process
exits non-zero, so nothing is lost silently.

State recovery: indicator state lives in memory. When partitions are
assigned, the consumer seeks back ``warmup_messages`` before the committed
offset and replays them in *warm-up mode* (state is rebuilt, nothing is
published or committed) until it reaches the committed offset. After a
restart or rebalance, indicators therefore continue as if nothing happened,
up to the warm-up depth, without an external state store.
"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any, Protocol

from shared.kafka.serde import EventDeserializationError, JsonEventSerde
from shared.kafka.topics import Topic
from shared.observability.logs import get_logger
from shared.schemas import BaseEvent, DeadLetterEvent, FailureReason, MarketBarEvent
from stream_processor.evaluation import INJECTED_HEADER, parse_injected, score_bar
from stream_processor.metrics import StreamMetrics
from stream_processor.processor import BarProcessor, Outcome, ProcessResult

log = get_logger(__name__)

OFFSET_INVALID = -1001
PartitionKey = tuple[str, int]
Headers = list[tuple[str, str | bytes | None]]


class MessageLike(Protocol):
    def error(self) -> Any: ...
    def topic(self) -> str | None: ...
    def partition(self) -> int | None: ...
    def offset(self) -> int | None: ...
    def key(self) -> bytes | None: ...
    def value(self) -> bytes | None: ...
    def headers(self) -> list[tuple[str, bytes]] | None: ...


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
    def poll(self, timeout: float = ...) -> int: ...
    def flush(self, timeout: float = ...) -> int: ...


class DeliveryError(RuntimeError):
    """Outputs of a batch were not acknowledged; the batch must not be committed."""


@dataclass(frozen=True, slots=True)
class AppConfig:
    group_id: str = "stream-processor"
    input_topic: str = Topic.MARKET_RAW
    enriched_topic: str = Topic.MARKET_ENRICHED
    anomalies_topic: str = Topic.MARKET_ANOMALIES
    dead_letter_topic: str = Topic.DEAD_LETTER
    source: str = "stream-processor"
    batch_size: int = 500
    poll_timeout_s: float = 0.5
    flush_timeout_s: float = 10.0
    warmup_messages: int = 2_000
    lag_interval_s: float = 5.0


def warmup_start(committed: int, low: int, warmup: int) -> int | None:
    """Offset to seek to on assignment, or None to keep the default position."""
    if committed < 0:  # nothing committed yet: auto.offset.reset applies, no history to rebuild
        return None
    return max(low, committed - warmup)


class StreamProcessorApp:
    def __init__(
        self,
        consumer: Any,
        producer: ProducerLike,
        processor: BarProcessor,
        metrics: StreamMetrics,
        config: AppConfig,
        *,
        stop: threading.Event,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._consumer = consumer
        self._producer = producer
        self._processor = processor
        self._metrics = metrics
        self._cfg = config
        self._stop = stop
        self._clock = clock
        self._serde = JsonEventSerde()
        self._resume_at: dict[PartitionKey, int] = {}
        self._symbols_by_partition: dict[PartitionKey, set[str]] = {}
        self._delivery_errors = 0
        self._last_lag_check = 0.0
        self._lag_labels: set[tuple[str, str]] = set()

    # ------------------------------------------------------------------ rebalance
    def on_assign(self, consumer: Any, partitions: list[Any]) -> None:
        committed = consumer.committed(partitions, timeout=10)
        for tp in committed:
            low, _high = consumer.get_watermark_offsets(tp, timeout=10)
            start = warmup_start(tp.offset, low, self._cfg.warmup_messages)
            if start is not None and start < tp.offset:
                self._resume_at[(tp.topic, tp.partition)] = tp.offset
                tp.offset = start
            log.info(
                "partition.assigned",
                topic=tp.topic,
                partition=tp.partition,
                committed=self._resume_at.get((tp.topic, tp.partition)),
                warmup_from=start,
            )
        consumer.incremental_assign(committed)

    def on_revoke(self, consumer: Any, partitions: list[Any]) -> None:
        try:
            consumer.commit(asynchronous=False)
        except Exception as exc:  # e.g. _NO_OFFSET when nothing was stored yet
            log.debug("revoke.commit_skipped", error=str(exc))
        self._forget(partitions)

    def on_lost(self, consumer: Any, partitions: list[Any]) -> None:
        log.warning("partitions.lost", partitions=[(p.topic, p.partition) for p in partitions])
        self._forget(partitions)

    def _forget(self, partitions: list[Any]) -> None:
        for tp in partitions:
            key = (tp.topic, tp.partition)
            self._resume_at.pop(key, None)
            self._processor.drop(self._symbols_by_partition.pop(key, set()))
            labels = (tp.topic, str(tp.partition))
            if labels in self._lag_labels:
                self._metrics.consumer_lag.remove(*labels)
                self._lag_labels.discard(labels)
        self._metrics.tracked_symbols.set(self._processor.tracked_symbols)

    # ------------------------------------------------------------------ main loop
    def run(self) -> None:
        self._consumer.subscribe(
            [self._cfg.input_topic],
            on_assign=self.on_assign,
            on_revoke=self.on_revoke,
            on_lost=self.on_lost,
        )
        log.info("stream_processor.started", group=self._cfg.group_id)
        try:
            while not self._stop.is_set():
                messages = self._consumer.consume(
                    num_messages=self._cfg.batch_size, timeout=self._cfg.poll_timeout_s
                )
                if messages:
                    self.handle_batch(messages)
                self._update_lag()
        finally:
            remaining = self._producer.flush(self._cfg.flush_timeout_s)
            self._consumer.close()  # commits stored offsets, leaves the group cleanly
            log.info("stream_processor.stopped", undelivered=remaining)

    def handle_batch(self, messages: Sequence[MessageLike]) -> None:
        self._metrics.batch_size.observe(len(messages))
        to_store: dict[PartitionKey, MessageLike] = {}
        produced = 0
        for msg in messages:
            if msg.error():
                log.warning("consumer.message_error", error=str(msg.error()))
                continue
            key = (msg.topic() or "", msg.partition() or 0)
            count, store = self._handle_message(msg, warming=self._is_warming(key, msg))
            produced += count
            if store:
                to_store[key] = msg
        self._commit(to_store, produced)

    def _is_warming(self, key: PartitionKey, msg: MessageLike) -> bool:
        resume = self._resume_at.get(key)
        if resume is None:
            return False
        if (msg.offset() or 0) < resume:
            return True
        del self._resume_at[key]
        log.info("partition.warmup_complete", topic=key[0], partition=key[1])
        return False

    def _handle_message(self, msg: MessageLike, *, warming: bool) -> tuple[int, bool]:
        """Process one message; return (outputs produced, whether to store its offset)."""
        try:
            bar = self._serde.deserialize(msg.value(), MarketBarEvent)
        except EventDeserializationError as exc:
            self._metrics.messages.labels(outcome="invalid").inc()
            return (0, False) if warming else (self._dead_letter(msg, exc.reason, exc), True)

        key = (msg.topic() or "", msg.partition() or 0)
        self._symbols_by_partition.setdefault(key, set()).add(bar.symbol)
        started = time.perf_counter()
        try:
            result = self._processor.process(bar)
        except Exception as exc:  # a bug must not wedge the partition
            log.exception("process.failed", event_id=str(bar.event_id), symbol=bar.symbol)
            self._metrics.messages.labels(outcome="error").inc()
            if warming:
                return 0, False
            return self._dead_letter(msg, FailureReason.PROCESSING, exc), True
        self._metrics.compute_seconds.observe(time.perf_counter() - started)

        if warming:
            self._metrics.messages.labels(outcome="warmup").inc()
            return 0, False
        self._metrics.messages.labels(outcome=result.outcome.value).inc()
        if result.outcome is not Outcome.PROCESSED or result.enriched is None:
            return 0, True
        return self._publish(bar, msg, result), True

    def _publish(self, bar: MarketBarEvent, msg: MessageLike, result: ProcessResult) -> int:
        enriched = result.enriched
        if enriched is None:
            return 0
        if result.missing_bars:
            self._metrics.missing_bars.inc(result.missing_bars)
        produced = self._produce(self._cfg.enriched_topic, enriched)
        if enriched.processing_latency_ms is not None:
            self._metrics.end_to_end.observe(enriched.processing_latency_ms / 1000)
        for anomaly in result.anomalies:
            produced += self._produce(self._cfg.anomalies_topic, anomaly)
            self._metrics.anomalies.labels(
                anomaly_type=anomaly.anomaly_type.value, severity=anomaly.severity.value
            ).inc()
        if bar.source == "simulator":
            injected = parse_injected(dict(msg.headers() or []).get(INJECTED_HEADER))
            for detector, outcome in score_bar(injected, result.anomalies):
                self._metrics.detector_outcomes.labels(
                    detector=detector, outcome=outcome.value
                ).inc()
        return produced

    def _commit(self, to_store: dict[PartitionKey, MessageLike], produced: int) -> None:
        """Wait for every output to be acknowledged, then store input offsets."""
        if produced:
            started = time.perf_counter()
            remaining = self._producer.flush(self._cfg.flush_timeout_s)
            self._metrics.flush_seconds.observe(time.perf_counter() - started)
            if remaining or self._delivery_errors:
                raise DeliveryError(
                    f"{remaining} outputs undelivered, {self._delivery_errors} delivery errors; "
                    "batch not committed"
                )
        for msg in to_store.values():
            self._consumer.store_offsets(message=msg)
        self._metrics.tracked_symbols.set(self._processor.tracked_symbols)

    # ------------------------------------------------------------------ outputs
    def _on_delivery(self, err: Any, _msg: Any) -> None:
        if err is not None:
            self._delivery_errors += 1
            log.error("producer.delivery_failed", error=str(err))

    def _produce(self, topic: str, event: BaseEvent) -> int:
        while True:
            try:
                self._producer.produce(
                    topic,
                    key=event.partition_key().encode(),
                    value=self._serde.serialize(event),
                    headers=list(self._serde.headers(event)),
                    on_delivery=self._on_delivery,
                )
                return 1
            except BufferError:
                self._producer.poll(0.1)  # bounded by flush timeout of the batch

    def _dead_letter(self, msg: MessageLike, reason: FailureReason, error: BaseException) -> int:
        record = DeadLetterEvent.from_failure(
            topic=msg.topic() or "",
            partition=msg.partition() or 0,
            offset=msg.offset() or 0,
            key=msg.key(),
            value=msg.value(),
            reason=reason,
            error=error,
            consumer_group=self._cfg.group_id,
            source=self._cfg.source,
        )
        self._metrics.dead_lettered.labels(reason=reason.value).inc()
        log.warning(
            "message.dead_lettered",
            reason=reason.value,
            topic=record.original_topic,
            partition=record.original_partition,
            offset=record.original_offset,
            error=record.error_message[:200],
        )
        return self._produce(self._cfg.dead_letter_topic, record)

    # ------------------------------------------------------------------ lag
    def _update_lag(self) -> None:
        now = self._clock()
        if now - self._last_lag_check < self._cfg.lag_interval_s:
            return
        self._last_lag_check = now
        try:
            assignment = self._consumer.assignment()
            for tp in self._consumer.position(assignment):
                if tp.offset < 0:
                    continue
                _low, high = self._consumer.get_watermark_offsets(tp, timeout=1, cached=False)
                labels = (tp.topic, str(tp.partition))
                self._metrics.consumer_lag.labels(*labels).set(max(0, high - tp.offset))
                self._lag_labels.add(labels)
        except Exception as exc:  # metrics must never take the processor down
            log.debug("lag.update_failed", error=str(exc))

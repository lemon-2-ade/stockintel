"""Kafka consume -> features -> /predict -> produce loop.

Per ``consume()`` batch:

1. Each enriched bar updates its symbol's ``FeatureComputer``. Bars whose
   symbol is warmed up (61 bars without a gap) become prediction requests.
2. Requests go to the inference service in batches of up to 256.
3. Returned ``PredictionEvent`` records get the feature snapshot attached (for
   drift monitoring) and are produced to ``market.predictions``.
4. ``flush()``; only then are input offsets stored (at-least-once). Prediction
   ids are deterministic, so a redelivered batch re-publishes identical
   records that the sinks de-duplicate.

**Degradation policy.** Market data must never wait for the model. If the
inference service is unavailable after a couple of quick retries, or rejects
the input, the bars of that batch get **no prediction** (counted in
``sip_pipeline_predictions_skipped_total``) and the offsets are still
committed. A prediction for a bar that is minutes old is worth little; a
stalled consumer would also delay recovery once the service is back.

**State recovery** mirrors the stream processor: on assignment the consumer
seeks back ``warmup_messages`` before the committed offset and replays them
without predicting, so the rolling features are rebuilt after a restart.
"""

from __future__ import annotations

import threading
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any, Protocol

from prediction_pipeline.client import (
    InferenceRejectedError,
    InferenceUnavailableError,
    Predictor,
)
from prediction_pipeline.features import FeatureStore
from prediction_pipeline.metrics import PipelineMetrics
from shared.features import FEATURE_SET_VERSION
from shared.kafka.serde import EventDeserializationError, JsonEventSerde
from shared.kafka.topics import Topic
from shared.observability.logs import get_logger
from shared.schemas import BaseEvent, DeadLetterEvent, EnrichedBarEvent, FailureReason
from shared.schemas.inference import MAX_BATCH, PredictInstance

log = get_logger(__name__)

PartitionKey = tuple[str, int]
Headers = list[tuple[str, str | bytes | None]]


class MessageLike(Protocol):
    def error(self) -> Any: ...
    def topic(self) -> str | None: ...
    def partition(self) -> int | None: ...
    def offset(self) -> int | None: ...
    def key(self) -> bytes | None: ...
    def value(self) -> bytes | None: ...


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
class PipelineConfig:
    group_id: str = "prediction-pipeline"
    input_topic: str = Topic.MARKET_ENRICHED
    output_topic: str = Topic.MARKET_PREDICTIONS
    dead_letter_topic: str = Topic.DEAD_LETTER
    source: str = "prediction-pipeline"
    batch_size: int = 500
    poll_timeout_s: float = 0.5
    flush_timeout_s: float = 10.0
    warmup_messages: int = 2_000
    predict_every_n_bars: int = 1
    lag_interval_s: float = 5.0
    feature_set_version: str = FEATURE_SET_VERSION


@dataclass(frozen=True, slots=True)
class Pending:
    instance: PredictInstance
    features: dict[str, float]


class PredictionPipelineApp:
    def __init__(
        self,
        consumer: Any,
        producer: ProducerLike,
        predictor: Predictor,
        metrics: PipelineMetrics,
        config: PipelineConfig,
        *,
        stop: threading.Event,
        features: FeatureStore | None = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._consumer = consumer
        self._producer = producer
        self._predictor = predictor
        self._metrics = metrics
        self._cfg = config
        self._stop = stop
        self._features = features or FeatureStore()
        self._clock = clock
        self._serde = JsonEventSerde()
        self._resume_at: dict[PartitionKey, int] = {}
        self._symbols_by_partition: dict[PartitionKey, set[str]] = {}
        self._delivery_errors = 0
        self._last_lag_check = 0.0

    # ------------------------------------------------------------------ rebalance
    def on_assign(self, consumer: Any, partitions: list[Any]) -> None:
        committed = consumer.committed(partitions, timeout=10)
        for tp in committed:
            if tp.offset >= 0:
                low, _high = consumer.get_watermark_offsets(tp, timeout=10)
                start = max(low, tp.offset - self._cfg.warmup_messages)
                if start < tp.offset:
                    self._resume_at[(tp.topic, tp.partition)] = tp.offset
                    tp.offset = start
            log.info("partition.assigned", topic=tp.topic, partition=tp.partition)
        consumer.incremental_assign(committed)

    def on_revoke(self, consumer: Any, partitions: list[Any]) -> None:
        try:
            consumer.commit(asynchronous=False)
        except Exception as exc:  # nothing stored yet
            log.debug("revoke.commit_skipped", error=str(exc))
        self._forget(partitions)

    def on_lost(self, consumer: Any, partitions: list[Any]) -> None:
        self._forget(partitions)

    def _forget(self, partitions: list[Any]) -> None:
        for tp in partitions:
            key = (tp.topic, tp.partition)
            self._resume_at.pop(key, None)
            self._features.drop(self._symbols_by_partition.pop(key, set()))
        self._metrics.tracked_symbols.set(len(self._features))

    # ------------------------------------------------------------------ main loop
    def run(self) -> None:
        self._consumer.subscribe(
            [self._cfg.input_topic],
            on_assign=self.on_assign,
            on_revoke=self.on_revoke,
            on_lost=self.on_lost,
        )
        log.info("prediction_pipeline.started", group=self._cfg.group_id)
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
            self._consumer.close()
            log.info("prediction_pipeline.stopped", undelivered=remaining)

    def handle_batch(self, messages: Sequence[MessageLike]) -> None:
        to_store: dict[PartitionKey, MessageLike] = {}
        pending: list[Pending] = []
        produced = 0
        for msg in messages:
            if msg.error():
                log.warning("consumer.message_error", error=str(msg.error()))
                continue
            key = (msg.topic() or "", msg.partition() or 0)
            warming = self._is_warming(key, msg)
            try:
                bar = self._serde.deserialize(msg.value(), EnrichedBarEvent)
            except EventDeserializationError as exc:
                self._metrics.messages.labels("invalid").inc()
                if not warming:
                    produced += self._dead_letter(msg, exc.reason, exc)
                    to_store[key] = msg
                continue
            self._symbols_by_partition.setdefault(key, set()).add(bar.symbol)
            item = self._update(bar, warming=warming)
            if item is not None:
                pending.append(item)
            if not warming:
                to_store[key] = msg
        produced += self._predict_and_publish(pending)
        self._commit(to_store, produced)
        self._metrics.tracked_symbols.set(len(self._features))

    def _is_warming(self, key: PartitionKey, msg: MessageLike) -> bool:
        resume = self._resume_at.get(key)
        if resume is None:
            return False
        if (msg.offset() or 0) < resume:
            return True
        del self._resume_at[key]
        log.info("partition.warmup_complete", topic=key[0], partition=key[1])
        return False

    def _update(self, bar: EnrichedBarEvent, *, warming: bool) -> Pending | None:
        update = self._features.update(bar)
        if update.gap:
            self._metrics.feature_gaps.inc()
        if warming:
            self._metrics.messages.labels("warmup").inc()
            return None
        if update.stale:
            self._metrics.messages.labels("stale").inc()
            return None
        if update.features is None:
            self._metrics.messages.labels("not_ready").inc()
            return None
        if self._features.bars_seen(bar.symbol) % self._cfg.predict_every_n_bars:
            self._metrics.messages.labels("sampled_out").inc()
            return None
        self._metrics.messages.labels("scored").inc()
        instance = PredictInstance(
            symbol=bar.symbol,
            timestamp=bar.timestamp,
            interval=bar.interval,
            source_event_id=bar.source_event_id,
            feature_set_version=self._cfg.feature_set_version,
            features=update.features,
        )
        return Pending(instance, update.features)

    def _predict_and_publish(self, pending: list[Pending]) -> int:
        produced = 0
        for start in range(0, len(pending), MAX_BATCH):
            chunk = pending[start : start + MAX_BATCH]
            started = time.perf_counter()
            try:
                response = self._predictor.predict([p.instance for p in chunk])
            except InferenceUnavailableError:
                self._metrics.skipped.labels("unavailable").inc(len(chunk))
                continue
            except InferenceRejectedError as exc:
                log.error("inference.rejected", detail=str(exc)[:300], instances=len(chunk))
                self._metrics.skipped.labels("rejected").inc(len(chunk))
                continue
            finally:
                self._metrics.inference_seconds.observe(time.perf_counter() - started)
            for item, event in zip(chunk, response.predictions, strict=True):
                produced += self._produce(
                    self._cfg.output_topic, event.model_copy(update={"features": item.features})
                )
                self._metrics.predictions.labels(event.model_version).inc()
        return produced

    def _commit(self, to_store: dict[PartitionKey, MessageLike], produced: int) -> None:
        if produced:
            remaining = self._producer.flush(self._cfg.flush_timeout_s)
            if remaining or self._delivery_errors:
                raise DeliveryError(
                    f"{remaining} outputs undelivered, {self._delivery_errors} delivery errors; "
                    "batch not committed"
                )
        for msg in to_store.values():
            self._consumer.store_offsets(message=msg)

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
                self._producer.poll(0.1)

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
        return self._produce(self._cfg.dead_letter_topic, record)

    def _update_lag(self) -> None:
        now = self._clock()
        if now - self._last_lag_check < self._cfg.lag_interval_s:
            return
        self._last_lag_check = now
        try:
            for tp in self._consumer.position(self._consumer.assignment()):
                if tp.offset < 0:
                    continue
                _low, high = self._consumer.get_watermark_offsets(tp, timeout=1, cached=False)
                self._metrics.consumer_lag.labels(tp.topic, str(tp.partition)).set(
                    max(0, high - tp.offset)
                )
        except Exception as exc:  # metrics must never take the pipeline down
            log.debug("lag.update_failed", error=str(exc))

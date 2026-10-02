from __future__ import annotations

import json
import threading
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from factories import make_bar
from kafka_fakes import FakeProducer
from prometheus_client import CollectorRegistry

from shared.kafka.serde import JsonEventSerde
from shared.schemas import EnrichedBarEvent
from stream_processor.app import AppConfig, DeliveryError, StreamProcessorApp, warmup_start
from stream_processor.metrics import StreamMetrics
from stream_processor.processor import BarProcessor

T0 = datetime(2026, 1, 5, 14, 30, tzinfo=UTC)
serde = JsonEventSerde()


@dataclass
class Msg:
    _value: bytes | None
    _offset: int
    _partition: int = 0
    _topic: str = "market.raw"
    _key: bytes | None = b"AAPL"
    _headers: list[tuple[str, bytes]] = field(default_factory=list)
    _error: Any = None

    def error(self) -> Any:
        return self._error

    def topic(self) -> str:
        return self._topic

    def partition(self) -> int:
        return self._partition

    def offset(self) -> int:
        return self._offset

    def key(self) -> bytes | None:
        return self._key

    def value(self) -> bytes | None:
        return self._value

    def headers(self) -> list[tuple[str, bytes]]:
        return self._headers


@dataclass
class TP:
    topic: str
    partition: int
    offset: int = -1001


class FakeConsumer:
    def __init__(self, committed: dict[int, int] | None = None, low: int = 0) -> None:
        self.stored: list[tuple[int, int]] = []
        self.assigned: list[TP] = []
        self._committed = committed or {}
        self._low = low

    def store_offsets(self, message: Msg) -> None:
        self.stored.append((message.partition(), message.offset()))

    def committed(self, partitions: list[TP], timeout: float) -> list[TP]:
        return [
            TP(p.topic, p.partition, self._committed.get(p.partition, -1001)) for p in partitions
        ]

    def get_watermark_offsets(
        self, tp: TP, timeout: float = 0, cached: bool = False
    ) -> tuple[int, int]:
        return self._low, 10_000

    def incremental_assign(self, partitions: list[TP]) -> None:
        self.assigned = partitions

    def commit(self, asynchronous: bool = False) -> None:
        pass


def bar_msg(i: int, offset: int, close: float = 100.0, **kw: Any) -> Msg:
    event = make_bar(
        timestamp=T0 + timedelta(minutes=i), open=close, high=close, low=close, close=close, **kw
    )
    return Msg(serde.serialize(event), offset, _key=event.symbol.encode())


def build(
    consumer: FakeConsumer | None = None, producer: FakeProducer | None = None
) -> tuple[StreamProcessorApp, FakeConsumer, FakeProducer, StreamMetrics]:
    # `is None`, not `or`: an empty FakeProducer is falsy because it defines __len__.
    consumer = FakeConsumer() if consumer is None else consumer
    producer = FakeProducer() if producer is None else producer
    metrics = StreamMetrics(CollectorRegistry())
    app = StreamProcessorApp(
        consumer,
        producer,
        BarProcessor(),
        metrics,
        AppConfig(warmup_messages=100),
        stop=threading.Event(),
    )
    return app, consumer, producer, metrics


def sample(metrics: StreamMetrics, name: str, **labels: str) -> float:
    return metrics.registry.get_sample_value(name, labels or None) or 0.0


def topics(producer: FakeProducer) -> list[str]:
    return [s.topic for s in producer.sent]


def test_valid_bars_are_enriched_then_offsets_stored() -> None:
    app, consumer, producer, metrics = build()
    app.handle_batch([bar_msg(0, 10), bar_msg(1, 11, close=103.0)])  # 2nd bar is a +3% spike
    assert topics(producer) == ["market.enriched", "market.enriched", "market.anomalies"]
    assert consumer.stored == [(0, 11)], "one store per partition, highest offset"
    assert sample(metrics, "sip_stream_messages_total", outcome="processed") == 2
    enriched = EnrichedBarEvent.model_validate(json.loads(producer.sent[0].value))
    assert enriched.symbol == "AAPL"


def test_poison_message_goes_to_dlq_and_partition_moves_on() -> None:
    app, consumer, producer, metrics = build()
    app.handle_batch([Msg(b"\xff not json", 5), bar_msg(0, 6)])
    assert topics(producer) == ["market.dead-letter", "market.enriched"]
    dlq = json.loads(producer.sent[0].value)
    assert dlq["reason"] == "deserialization"
    assert dlq["original_offset"] == 5
    assert consumer.stored == [(0, 6)]
    assert sample(metrics, "sip_stream_dead_lettered_total", reason="deserialization") == 1


def test_schema_violation_is_dead_lettered_with_reason() -> None:
    app, _, producer, _ = build()
    payload = json.loads(serde.serialize(make_bar()))
    payload["high"] = 1.0
    app.handle_batch([Msg(json.dumps(payload).encode(), 3)])
    assert json.loads(producer.sent[0].value)["reason"] == "validation"


def test_undelivered_outputs_block_the_commit() -> None:
    producer = FakeProducer(fail_keys={b"AAPL"})
    app, consumer, _, _ = build(producer=producer)
    with pytest.raises(DeliveryError):
        app.handle_batch([bar_msg(0, 1)])
    assert consumer.stored == [], "nothing committed when outputs were not acknowledged"


def test_redelivered_batch_is_harmless() -> None:
    app, consumer, producer, metrics = build()
    batch = [bar_msg(0, 1), bar_msg(1, 2)]
    app.handle_batch(batch)
    app.handle_batch(batch)  # e.g. after a crash before commit
    assert topics(producer).count("market.enriched") == 2
    assert sample(metrics, "sip_stream_messages_total", outcome="duplicate") == 2
    assert consumer.stored[-1] == (0, 2)


def test_warmup_rebuilds_state_without_publishing() -> None:
    consumer = FakeConsumer(committed={0: 50}, low=0)
    app, _, producer, metrics = build(consumer=consumer)
    app.on_assign(consumer, [TP("market.raw", 0)])
    assert consumer.assigned[0].offset == 0, "seek back warmup_messages (bounded by low watermark)"

    app.handle_batch([bar_msg(i, i) for i in range(50)])  # replayed history
    assert producer.sent == []
    assert consumer.stored == []
    assert sample(metrics, "sip_stream_messages_total", outcome="warmup") == 50

    app.handle_batch([bar_msg(50, 50)])  # first new message
    (out,) = producer.sent
    enriched = json.loads(out.value)
    assert enriched["indicators"]["sma_20"] is not None, "indicators survived the restart"
    assert consumer.stored == [(0, 50)]


@pytest.mark.parametrize(
    ("committed", "low", "warmup", "expected"),
    [(-1001, 0, 100, None), (500, 0, 100, 400), (50, 0, 100, 0), (500, 450, 100, 450)],
)
def test_warmup_start(committed: int, low: int, warmup: int, expected: int | None) -> None:
    assert warmup_start(committed, low, warmup) == expected


def test_revoke_forgets_state_of_moved_partitions() -> None:
    app, consumer, _, metrics = build()
    app.handle_batch([bar_msg(0, 1)])
    assert sample(metrics, "sip_stream_tracked_symbols") == 1
    app.on_revoke(consumer, [TP("market.raw", 0)])
    assert sample(metrics, "sip_stream_tracked_symbols") == 0


def test_detector_scoring_uses_simulator_ground_truth() -> None:
    app, _, _, metrics = build()
    msgs = [bar_msg(0, 1, source="simulator")]
    spike = bar_msg(1, 2, close=103.0, source="simulator")
    spike._headers = [("x-sim-injected-anomaly", b"price_spike")]
    app.handle_batch([*msgs, spike])
    labels = {"detector": "return_threshold", "outcome": "tp"}
    assert sample(metrics, "sip_stream_detector_outcomes_total", **labels) == 1

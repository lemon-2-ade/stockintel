from __future__ import annotations

import json
import threading
from collections.abc import Sequence

import pytest
from factories import make_bar
from kafka_fakes import FakeConsumer, FakeProducer, Msg
from prometheus_client import CollectorRegistry

from shared.kafka.serde import JsonEventSerde
from shared.schemas import BaseEvent
from shared.utils.retry import BackoffPolicy
from sinks.metrics import SinkMetrics
from sinks.runner import (
    BatchSinkRunner,
    DeadLetterDeliveryError,
    RunnerConfig,
    TransientSinkError,
    WriteResult,
)

serde = JsonEventSerde()
FAST = BackoffPolicy(max_attempts=3, base_delay_s=0.0, max_delay_s=0.0)


class FakeSink:
    name = "fake"
    topics: Sequence[str] = ("market.raw",)

    def __init__(self, failures: int = 0, reject: bool = False) -> None:
        self.failures = failures
        self.reject = reject
        self.calls = 0
        self.received: list[BaseEvent] = []

    def write(self, events: Sequence[BaseEvent]) -> WriteResult:
        self.calls += 1
        if self.failures:
            self.failures -= 1
            raise TransientSinkError("db restarting")
        self.received.extend(events)
        result = WriteResult(written={"t": len(events)})
        if self.reject:
            result.rejected.append((events[0], "constraint violated"))
        return result


def build(
    sink: FakeSink, producer: FakeProducer | None = None
) -> tuple[BatchSinkRunner, FakeConsumer, FakeProducer, SinkMetrics]:
    consumer = FakeConsumer()
    producer = FakeProducer() if producer is None else producer
    metrics = SinkMetrics(CollectorRegistry())
    runner = BatchSinkRunner(
        consumer,
        producer,
        sink,
        metrics,
        RunnerConfig(group_id="g", retry=FAST),
        stop=threading.Event(),
        sleep=lambda _: None,
    )
    return runner, consumer, producer, metrics


def msg(offset: int, partition: int = 0) -> Msg:
    return Msg(serde.serialize(make_bar()), offset, _partition=partition)


def test_writes_then_stores_last_offset_per_partition() -> None:
    sink = FakeSink()
    runner, consumer, _, metrics = build(sink)
    runner.handle_batch([msg(1), msg(2), msg(7, partition=1)])
    assert len(sink.received) == 3
    assert sorted(consumer.stored) == [(0, 2), (1, 7)]
    assert metrics.registry.get_sample_value("sip_sink_written_total", {"target": "t"}) == 3


def test_transient_failures_are_retried() -> None:
    sink = FakeSink(failures=2)
    runner, consumer, _, _ = build(sink)
    runner.handle_batch([msg(1)])
    assert sink.calls == 3
    assert consumer.stored == [(0, 1)]


def test_persistent_outage_raises_without_committing() -> None:
    sink = FakeSink(failures=10)
    runner, consumer, _, _ = build(sink)
    with pytest.raises(TransientSinkError):
        runner.handle_batch([msg(1)])
    assert consumer.stored == []


def test_undecodable_and_rejected_events_are_dead_lettered() -> None:
    sink = FakeSink(reject=True)
    runner, consumer, producer, _ = build(sink)
    runner.handle_batch([Msg(b"{broken", 1), msg(2)])
    reasons = [json.loads(s.value)["reason"] for s in producer.sent]
    assert reasons == ["deserialization", "processing"]
    assert consumer.stored == [(0, 2)]


def test_dlq_delivery_failure_blocks_commit() -> None:
    producer = FakeProducer(fail_keys={b"market.raw"})
    runner, consumer, _, _ = build(FakeSink(), producer)
    with pytest.raises(DeadLetterDeliveryError):
        runner.handle_batch([Msg(b"{broken", 1, _key=None)])
    assert consumer.stored == []

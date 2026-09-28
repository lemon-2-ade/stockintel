from __future__ import annotations

import threading
from collections.abc import Iterator

from factories import make_bar
from kafka_fakes import FakeProducer
from prometheus_client import CollectorRegistry

from market_producer.metrics import ProducerMetrics
from market_producer.providers.base import Batch, ProducedBar
from market_producer.publisher import KafkaMarketPublisher
from market_producer.runtime import ProducerRuntime
from shared.kafka.serde import JsonEventSerde


class ListProvider:
    name = "list"

    def __init__(self, batches: list[Batch], on_yield: object = None) -> None:
        self._batches = batches
        self.yielded = 0

    def batches(self) -> Iterator[Batch]:
        for batch in self._batches:
            self.yielded += 1
            yield batch


class FakeClock:
    """Wall clock that only advances when the runtime waits on the stop event."""

    def __init__(self, start: float) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now


class ClockedEvent(threading.Event):
    def __init__(self, clock: FakeClock, stop_at: float | None = None) -> None:
        super().__init__()
        self.clock = clock
        self.stop_at = stop_at
        self.waits: list[float] = []

    def wait(self, timeout: float | None = None) -> bool:
        assert timeout is not None
        self.waits.append(timeout)
        self.clock.now += timeout
        if self.stop_at is not None and self.clock.now >= self.stop_at:
            self.set()
        return self.is_set()


def build(
    batches: list[Batch], stop: ClockedEvent, clock: FakeClock
) -> tuple[ProducerRuntime, FakeProducer]:
    producer = FakeProducer()
    metrics = ProducerMetrics(CollectorRegistry())
    publisher = KafkaMarketPublisher(
        producer, topic="market.raw", serde=JsonEventSerde(), metrics=metrics
    )
    runtime = ProducerRuntime(
        ListProvider(batches), publisher, metrics, stop=stop, wall_clock=clock
    )
    return runtime, producer


def batch(due: float, *symbols: str) -> Batch:
    return Batch(due_at=due, bars=[ProducedBar(event=make_bar(symbol=s)) for s in symbols])


def test_publishes_everything_and_paces_by_due_time() -> None:
    clock = FakeClock(100.0)
    stop = ClockedEvent(clock)
    runtime, producer = build(
        [batch(90, "AAA"), batch(100, "BBB"), batch(102.5, "AAA", "BBB")], stop, clock
    )
    summary = runtime.run()

    assert [s.key for s in producer.sent] == [b"AAA", b"BBB", b"AAA", b"BBB"]
    assert stop.waits == [2.5], "past-due batches are sent immediately, future ones awaited"
    assert (summary.batches, summary.bars, summary.delivered, summary.failed) == (3, 4, 4, 0)
    assert summary.undelivered_at_exit == 0
    assert not summary.stopped_by_signal


def test_stop_interrupts_waiting_and_flushes() -> None:
    clock = FakeClock(0.0)
    stop = ClockedEvent(clock, stop_at=5.0)
    runtime, producer = build([batch(0, "AAA"), batch(10, "BBB"), batch(20, "CCC")], stop, clock)
    summary = runtime.run()

    assert [s.key for s in producer.sent] == [b"AAA"]
    assert summary.stopped_by_signal
    assert summary.delivered == 1, "already-sent events are flushed on shutdown"

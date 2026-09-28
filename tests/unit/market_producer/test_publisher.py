from __future__ import annotations

import json
from datetime import UTC, datetime

import pytest
from factories import make_bar
from kafka_fakes import FakeProducer
from prometheus_client import CollectorRegistry

from market_producer.metrics import ProducerMetrics
from market_producer.providers.base import ProducedBar
from market_producer.publisher import (
    HEADER_INJECTED_ANOMALY,
    KafkaMarketPublisher,
    QueueFullError,
)
from shared.kafka.serde import HEADER_EVENT_TYPE, JsonEventSerde


def metric(metrics: ProducerMetrics, name: str, **labels: str) -> float:
    value = metrics.registry.get_sample_value(name, labels or None)
    return 0.0 if value is None else value


def make_publisher(
    producer: FakeProducer, **kwargs: object
) -> tuple[KafkaMarketPublisher, ProducerMetrics]:
    metrics = ProducerMetrics(CollectorRegistry())
    publisher = KafkaMarketPublisher(
        producer,
        topic="market.raw",
        serde=JsonEventSerde(),
        metrics=metrics,
        **kwargs,  # type: ignore[arg-type]
    )
    return publisher, metrics


def test_publishes_keyed_serialized_event_with_headers() -> None:
    producer = FakeProducer()
    publisher, metrics = make_publisher(producer)
    bar = make_bar(symbol="NVDA")
    publisher.publish(ProducedBar(event=bar))

    (sent,) = producer.sent
    assert sent.topic == "market.raw"
    assert sent.key == b"NVDA"
    assert json.loads(sent.value)["event_id"] == str(bar.event_id)
    headers = dict(sent.headers)
    assert headers[HEADER_EVENT_TYPE] == b"market.bar"
    assert HEADER_INJECTED_ANOMALY not in headers
    assert metric(metrics, "sip_producer_bars_generated_total", symbol="NVDA") == 1


def test_success_is_counted_only_after_delivery_report() -> None:
    producer = FakeProducer()
    publisher, metrics = make_publisher(producer)
    publisher.publish(ProducedBar(event=make_bar()))
    assert publisher.delivered == 0
    assert metric(metrics, "sip_producer_events_delivered_total", topic="market.raw") == 0
    publisher.serve_delivery_reports()
    assert publisher.delivered == 1
    assert metric(metrics, "sip_producer_events_delivered_total", topic="market.raw") == 1
    assert metric(metrics, "sip_producer_last_delivery_timestamp_seconds") > 0


def test_delivery_failures_are_counted_not_raised() -> None:
    producer = FakeProducer(fail_keys={b"TSLA"})
    publisher, metrics = make_publisher(producer)
    publisher.publish(ProducedBar(event=make_bar(symbol="TSLA")))
    publisher.publish(ProducedBar(event=make_bar(symbol="AAPL")))
    publisher.serve_delivery_reports()
    assert (publisher.delivered, publisher.failed) == (1, 1)
    assert (
        metric(
            metrics,
            "sip_producer_delivery_failures_total",
            topic="market.raw",
            reason="_MSG_TIMED_OUT",
        )
        == 1
    )


def test_injected_anomalies_travel_as_header_and_metric() -> None:
    producer = FakeProducer()
    publisher, metrics = make_publisher(producer)
    publisher.publish(ProducedBar(event=make_bar(), injected=("price_spike", "volume_spike")))
    headers = dict(producer.sent[0].headers)
    assert headers[HEADER_INJECTED_ANOMALY] == b"price_spike,volume_spike"
    assert metric(metrics, "sip_producer_injected_anomalies_total", type="price_spike") == 1
    # ...and never inside the event payload itself.
    assert "injected" not in json.loads(producer.sent[0].value)


def test_backpressure_retries_until_queue_drains() -> None:
    producer = FakeProducer()
    producer.buffer_errors_to_raise = 3
    publisher, metrics = make_publisher(producer)
    publisher.publish(ProducedBar(event=make_bar()))
    assert len(producer.sent) == 1
    assert metric(metrics, "sip_producer_buffer_full_total") == 3


def test_backpressure_is_bounded() -> None:
    producer = FakeProducer(capacity=0)
    ticks = iter(float(i) for i in range(1_000))
    publisher, _ = make_publisher(producer, backpressure_timeout_s=5.0, clock=lambda: next(ticks))
    with pytest.raises(QueueFullError):
        publisher.publish(ProducedBar(event=make_bar(timestamp=datetime(2026, 1, 1, tzinfo=UTC))))

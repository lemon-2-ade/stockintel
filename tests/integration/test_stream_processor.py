"""market.raw -> stream processor -> market.enriched / market.anomalies / DLQ (needs `make dev`)."""

from __future__ import annotations

import json
import threading
import uuid
from datetime import UTC, datetime, timedelta

import pytest
from factories import make_bar
from prometheus_client import CollectorRegistry

from shared.config import KafkaSettings
from shared.kafka.client_config import consumer_config, producer_config
from shared.kafka.serde import JsonEventSerde
from shared.kafka.topics import Topic
from stream_processor.app import AppConfig, StreamProcessorApp
from stream_processor.metrics import StreamMetrics
from stream_processor.processor import BarProcessor

pytestmark = pytest.mark.integration
confluent_kafka = pytest.importorskip("confluent_kafka")
serde = JsonEventSerde()


def collect(kafka: KafkaSettings, topic: str, run: str, want: int) -> list[dict[str, object]]:
    consumer = confluent_kafka.Consumer(
        consumer_config(kafka, group_id=f"it-read-{run}-{topic}", client_id=f"it-{run}")
    )
    consumer.subscribe([topic])
    found: list[dict[str, object]] = []
    deadline = datetime.now(UTC) + timedelta(seconds=60)
    try:
        while len(found) < want and datetime.now(UTC) < deadline:
            msg = consumer.poll(1.0)
            if msg is None or msg.error():
                continue
            payload = json.loads(msg.value())
            if run in json.dumps(payload):
                found.append(payload)
    finally:
        consumer.close()
    return found


def test_end_to_end_enrichment_anomalies_and_dlq(kafka_settings: KafkaSettings) -> None:
    run = uuid.uuid4().hex[:6].upper()
    symbol = f"S{run}"
    producer = confluent_kafka.Producer(producer_config(kafka_settings, client_id=f"it-{run}"))
    start = datetime(2026, 1, 5, 14, 30, tzinfo=UTC)
    closes = [100.0 + 0.01 * i for i in range(40)] + [104.0]  # last bar: +4% spike
    for i, close in enumerate(closes):
        bar = make_bar(
            symbol=symbol,
            timestamp=start + timedelta(minutes=i),
            open=close,
            high=close,
            low=close,
            close=close,
            trace_id=run,
        )
        producer.produce(
            Topic.MARKET_RAW,
            key=symbol.encode(),
            value=serde.serialize(bar),
            headers=serde.headers(bar),
        )
    producer.produce(Topic.MARKET_RAW, key=symbol.encode(), value=f"garbage {run}".encode())
    assert producer.flush(15) == 0

    stop = threading.Event()
    app = StreamProcessorApp(
        confluent_kafka.Consumer(
            consumer_config(kafka_settings, group_id=f"it-sp-{run}", client_id=f"it-sp-{run}")
        ),
        confluent_kafka.Producer(producer_config(kafka_settings, client_id=f"it-sp-{run}")),
        BarProcessor(),
        StreamMetrics(CollectorRegistry()),
        AppConfig(group_id=f"it-sp-{run}"),
        stop=stop,
    )
    worker = threading.Thread(target=app.run, daemon=True)
    worker.start()
    try:
        enriched = collect(kafka_settings, Topic.MARKET_ENRICHED, run, len(closes))
        anomalies = collect(kafka_settings, Topic.MARKET_ANOMALIES, run, 1)
        dead = collect(kafka_settings, Topic.DEAD_LETTER, run, 1)
    finally:
        stop.set()
        worker.join(30)

    assert len(enriched) == len(closes)
    assert enriched[-1]["indicators"]["sma_20"] is not None  # type: ignore[index]
    assert any(a["anomaly_type"] == "price_spike" for a in anomalies)
    assert dead[0]["reason"] == "deserialization"

"""Producer -> Kafka -> consumer, against the running stack (`make dev`)."""

from __future__ import annotations

import itertools
import threading
import uuid
from datetime import UTC, datetime, timedelta

import pytest
from prometheus_client import CollectorRegistry

from market_producer.calibration import DEFAULT_PARAMS
from market_producer.metrics import ProducerMetrics
from market_producer.providers.simulated import SimulatorProvider
from market_producer.publisher import HEADER_INJECTED_ANOMALY, KafkaMarketPublisher
from market_producer.runtime import ProducerRuntime
from market_producer.simulator import AnomalyConfig, MarketSimulator, SymbolSimulator
from shared.config import KafkaSettings
from shared.kafka.client_config import consumer_config, producer_config
from shared.kafka.serde import JsonEventSerde
from shared.kafka.topics import Topic
from shared.schemas import BarInterval, MarketBarEvent

pytestmark = pytest.mark.integration
confluent_kafka = pytest.importorskip("confluent_kafka")

BARS_PER_SYMBOL = 50


def test_simulated_bars_reach_kafka_intact(kafka_settings: KafkaSettings) -> None:
    run = uuid.uuid4().hex[:6].upper()
    symbols = [f"T{run}{i}" for i in range(3)]
    simulator = MarketSimulator(
        [
            SymbolSimulator(
                s,
                DEFAULT_PARAMS,
                interval=BarInterval.S1,
                seed=1,
                anomalies=AnomalyConfig(price_jump_probability=0.2, volume_spike_probability=0.0),
            )
            for s in symbols
        ]
    )
    # Start in the past so every bar is already due: the test runs at full speed.
    start = datetime.now(UTC) - timedelta(hours=1)
    provider = SimulatorProvider(simulator, start=start, max_bars=BARS_PER_SYMBOL)

    metrics = ProducerMetrics(CollectorRegistry())
    producer = confluent_kafka.Producer(producer_config(kafka_settings, client_id=f"it-{run}"))
    publisher = KafkaMarketPublisher(
        producer, topic=Topic.MARKET_RAW, serde=JsonEventSerde(), metrics=metrics
    )
    summary = ProducerRuntime(provider, publisher, metrics, stop=threading.Event()).run()
    expected = BARS_PER_SYMBOL * len(symbols)
    assert (summary.delivered, summary.failed, summary.undelivered_at_exit) == (expected, 0, 0)

    consumer = confluent_kafka.Consumer(
        consumer_config(kafka_settings, group_id=f"it-{run}", client_id=f"it-{run}")
    )
    consumer.subscribe([Topic.MARKET_RAW])
    serde = JsonEventSerde()
    received: dict[str, list[MarketBarEvent]] = {s: [] for s in symbols}
    injected_headers = 0
    deadline = datetime.now(UTC) + timedelta(seconds=60)
    try:
        while sum(map(len, received.values())) < expected and datetime.now(UTC) < deadline:
            msg = consumer.poll(1.0)
            if msg is None or msg.error():
                continue
            event = serde.deserialize(msg.value(), MarketBarEvent)
            if event.symbol in received:
                assert msg.key() == event.symbol.encode()
                received[event.symbol].append(event)
                injected_headers += HEADER_INJECTED_ANOMALY in dict(msg.headers() or [])
    finally:
        consumer.close()

    for symbol, events in received.items():
        assert len(events) == BARS_PER_SYMBOL, symbol
        # Per-symbol order and continuity survive the trip through Kafka.
        stamps = [e.timestamp for e in events]
        assert stamps == sorted(stamps)
        assert all(b.open == a.close for a, b in itertools.pairwise(events))
    assert injected_headers > 0

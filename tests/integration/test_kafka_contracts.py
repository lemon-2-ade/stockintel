"""Provision topics and push real events through a live broker.

Verifies the properties later phases rely on: topics exist with the declared
layout, a symbol always maps to one partition (ordering), and the wire format
survives a real produce/consume round trip including headers.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta

import pytest
from factories import make_bar

from shared.config import KafkaSettings
from shared.kafka.client_config import admin_config, consumer_config, producer_config
from shared.kafka.provision import TopicProvisioner, plan
from shared.kafka.serde import HEADER_EVENT_TYPE, JsonEventSerde
from shared.kafka.topics import TOPICS, Topic
from shared.schemas import MarketBarEvent

pytestmark = pytest.mark.integration

confluent_kafka = pytest.importorskip("confluent_kafka")
admin_module = pytest.importorskip("confluent_kafka.admin")

serde = JsonEventSerde()


@pytest.fixture(scope="module")
def provisioned(kafka_settings: KafkaSettings) -> TopicProvisioner:
    admin = admin_module.AdminClient(admin_config(kafka_settings, client_id="it-admin"))
    provisioner = TopicProvisioner(admin, kafka_settings, timeout_s=20)
    try:
        existing = provisioner.existing_partitions()
    except Exception as exc:
        pytest.fail(
            f"Kafka unreachable at {kafka_settings.bootstrap_servers}: {exc}", pytrace=False
        )
    provisioner.apply(plan(existing, TOPICS.values()))
    return provisioner


def test_topics_match_declared_layout(provisioned: TopicProvisioner) -> None:
    existing = provisioned.existing_partitions()
    for spec in TOPICS.values():
        assert existing.get(spec.name) == spec.partitions, spec.name


def test_provisioning_is_idempotent(provisioned: TopicProvisioner) -> None:
    provisioned.apply(plan(provisioned.existing_partitions(), TOPICS.values()))


def test_round_trip_preserves_events_and_per_symbol_partitioning(
    provisioned: TopicProvisioner, kafka_settings: KafkaSettings
) -> None:
    run = uuid.uuid4().hex[:8]
    producer = confluent_kafka.Producer(producer_config(kafka_settings, client_id=f"it-{run}"))
    base = datetime(2026, 1, 5, 14, 30, tzinfo=UTC)
    sent: dict[str, MarketBarEvent] = {}
    for i in range(30):
        symbol = ("AAA", "BBB", "CCC")[i % 3]
        bar = make_bar(symbol=symbol, timestamp=base + timedelta(minutes=i), trace_id=f"{run}-{i}")
        sent[str(bar.event_id)] = bar
        producer.produce(
            Topic.MARKET_RAW,
            key=bar.partition_key(),
            value=serde.serialize(bar),
            headers=serde.headers(bar),
        )
    assert producer.flush(15) == 0

    consumer = confluent_kafka.Consumer(
        consumer_config(kafka_settings, group_id=f"it-{run}", client_id=f"it-{run}")
    )
    consumer.subscribe([Topic.MARKET_RAW])
    received: dict[str, tuple[MarketBarEvent, int]] = {}
    deadline = datetime.now(UTC) + timedelta(seconds=30)
    try:
        while len(received) < len(sent) and datetime.now(UTC) < deadline:
            msg = consumer.poll(1.0)
            if msg is None or msg.error():
                continue
            event = serde.deserialize(msg.value(), MarketBarEvent)
            if event.trace_id and event.trace_id.startswith(run):
                assert dict(msg.headers())[HEADER_EVENT_TYPE] == b"market.bar"
                received[str(event.event_id)] = (event, msg.partition())
                consumer.store_offsets(msg)
    finally:
        consumer.close()

    assert received.keys() == sent.keys()
    for event_id, (event, _) in received.items():
        assert event == sent[event_id]

    partitions_by_symbol: dict[str, set[int]] = {}
    for event, partition in received.values():
        partitions_by_symbol.setdefault(event.symbol, set()).add(partition)
    assert all(len(p) == 1 for p in partitions_by_symbol.values()), partitions_by_symbol

    # Within a partition, per-symbol order is the produce order.
    for symbol in partitions_by_symbol:
        ts = [e.timestamp for e, _ in received.values() if e.symbol == symbol]
        assert ts == sorted(ts)

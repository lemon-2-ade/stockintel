from __future__ import annotations

import pytest

from shared.kafka.topics import TOPICS, Topic, TopicSpec, spec_for
from shared.schemas import EVENT_REGISTRY


def test_every_topic_is_declared_exactly_once() -> None:
    assert set(TOPICS) == set(Topic)
    for topic, spec in TOPICS.items():
        assert spec.name is topic


def test_topics_only_carry_registered_event_types() -> None:
    registered = {event_type for event_type, _ in EVENT_REGISTRY}
    for spec in TOPICS.values():
        assert set(spec.event_types) <= registered, spec.name


def test_market_topics_are_keyed_by_symbol() -> None:
    for topic in (
        Topic.MARKET_RAW,
        Topic.MARKET_ENRICHED,
        Topic.MARKET_ANOMALIES,
        Topic.MARKET_PREDICTIONS,
    ):
        assert spec_for(topic).key == "symbol"


def test_raw_retention_covers_derived_topics() -> None:
    """Derived topics must be rebuildable by replaying market.raw."""
    raw = spec_for(Topic.MARKET_RAW)
    assert raw.retention_ms >= spec_for(Topic.MARKET_ENRICHED).retention_ms
    assert raw.partitions >= spec_for(Topic.MARKET_ENRICHED).partitions


def test_configs_include_managed_keys() -> None:
    configs = spec_for("market.raw").configs(min_insync_replicas=2)
    assert configs["cleanup.policy"] == "delete"
    assert configs["min.insync.replicas"] == "2"
    assert configs["retention.ms"] == str(7 * 24 * 3600 * 1000)
    assert configs["message.timestamp.type"] == "CreateTime"


@pytest.mark.parametrize(("partitions", "retention_ms"), [(0, 7 * 24 * 3600 * 1000), (3, 1000)])
def test_spec_guards(partitions: int, retention_ms: int) -> None:
    with pytest.raises(ValueError, match=r"partitions|retention"):
        TopicSpec(
            name=Topic.MARKET_RAW,
            partitions=partitions,
            retention_ms=retention_ms,
            key="symbol",
            event_types=(),
            description="",
        )

from __future__ import annotations

from collections.abc import Iterable
from concurrent.futures import Future
from types import SimpleNamespace
from typing import Any

import pytest

from shared.config import KafkaSettings
from shared.kafka.provision import (
    ApplyConfigs,
    CreateTopic,
    IncreasePartitions,
    PartitionSurplus,
    TopicProvisioner,
    plan,
)
from shared.kafka.topics import TOPICS, Topic


def test_plan_creates_everything_on_an_empty_cluster() -> None:
    actions = plan({}, TOPICS.values())
    assert all(isinstance(a, CreateTopic) for a in actions)
    assert {a.spec.name for a in actions} == set(Topic)


def test_plan_is_a_noop_config_refresh_when_up_to_date() -> None:
    existing: dict[str, int] = {spec.name: spec.partitions for spec in TOPICS.values()}
    actions = plan(existing, TOPICS.values())
    assert all(isinstance(a, ApplyConfigs) for a in actions)
    assert len(actions) == len(TOPICS)


def test_plan_grows_but_never_shrinks() -> None:
    raw, enriched = TOPICS[Topic.MARKET_RAW], TOPICS[Topic.MARKET_ENRICHED]
    actions = plan({raw.name: 2, enriched.name: 99}, [raw, enriched])
    assert IncreasePartitions(raw, 2) in actions
    assert PartitionSurplus(enriched, 99) in actions
    assert ApplyConfigs(raw) in actions
    assert ApplyConfigs(enriched) in actions


def _done(value: Any = None) -> Future[Any]:
    future: Future[Any] = Future()
    future.set_result(value)
    return future


class FakeAdmin:
    """Records AdminClient calls; mimics confluent_kafka's future-returning API."""

    def __init__(self, topics: dict[str, int]) -> None:
        self.topics = topics
        self.created: list[Any] = []
        self.grown: list[Any] = []
        self.altered: list[Any] = []

    def list_topics(self, timeout: float) -> Any:
        return SimpleNamespace(
            topics={
                name: SimpleNamespace(partitions=dict.fromkeys(range(n)))
                for name, n in self.topics.items()
            }
        )

    def create_topics(self, new_topics: Iterable[Any], operation_timeout: float) -> Any:
        self.created.extend(new_topics)
        return {t.topic: _done() for t in self.created}

    def create_partitions(self, parts: Iterable[Any], operation_timeout: float) -> Any:
        self.grown.extend(parts)
        return {p.topic: _done() for p in self.grown}

    def incremental_alter_configs(self, resources: Iterable[Any]) -> Any:
        self.altered.extend(resources)
        return {r: _done() for r in self.altered}


def test_existing_partitions_hides_internal_topics() -> None:
    admin = FakeAdmin({"market.raw": 6, "__consumer_offsets": 50})
    provisioner = TopicProvisioner(admin, KafkaSettings())
    assert provisioner.existing_partitions() == {"market.raw": 6}


def test_apply_issues_the_expected_admin_calls() -> None:
    pytest.importorskip("confluent_kafka")
    admin = FakeAdmin({"market.raw": 2})
    settings = KafkaSettings(topic_replication_factor=3, topic_min_insync_replicas=2)
    provisioner = TopicProvisioner(admin, settings)
    provisioner.apply(plan(provisioner.existing_partitions(), TOPICS.values()))

    created = {t.topic: t for t in admin.created}
    assert set(created) == {t.value for t in Topic} - {"market.raw"}
    enriched = created["market.enriched"]
    assert enriched.num_partitions == 6
    assert enriched.replication_factor == 3
    assert enriched.config["min.insync.replicas"] == "2"
    assert [(p.topic, p.new_total_count) for p in admin.grown] == [("market.raw", 6)]
    assert [r.name for r in admin.altered] == ["market.raw"]

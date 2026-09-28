"""Declarative Kafka topology: the single source of truth for every topic.

Topic provisioning (``shared.kafka.provision``), producers and consumers all
read from here, so a topic's name, key and retention cannot drift between
services. See ``docs/KAFKA_DESIGN.md`` for the reasoning behind each value.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from types import MappingProxyType

_HOUR_MS = 60 * 60 * 1000
_DAY_MS = 24 * _HOUR_MS


class Topic(StrEnum):
    MARKET_RAW = "market.raw"
    MARKET_ENRICHED = "market.enriched"
    MARKET_ANOMALIES = "market.anomalies"
    MARKET_PREDICTIONS = "market.predictions"
    DEAD_LETTER = "market.dead-letter"


@dataclass(frozen=True, slots=True)
class TopicSpec:
    name: Topic
    partitions: int
    retention_ms: int
    key: str
    event_types: tuple[str, ...]
    description: str
    cleanup_policy: str = "delete"
    extra_configs: Mapping[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.partitions < 1:
            raise ValueError(f"{self.name}: partitions must be >= 1")
        if self.retention_ms < _HOUR_MS:
            raise ValueError(f"{self.name}: retention below one hour is almost certainly a bug")

    def configs(self, *, min_insync_replicas: int) -> dict[str, str]:
        """Broker-side topic configs managed by the provisioner."""
        return {
            "cleanup.policy": self.cleanup_policy,
            "retention.ms": str(self.retention_ms),
            "min.insync.replicas": str(min_insync_replicas),
            # Keep the producer's CreateTime: it is our *event-time* anchor.
            "message.timestamp.type": "CreateTime",
            **self.extra_configs,
        }


TOPICS: Mapping[Topic, TopicSpec] = MappingProxyType(
    {
        spec.name: spec
        for spec in (
            TopicSpec(
                name=Topic.MARKET_RAW,
                partitions=6,
                retention_ms=7 * _DAY_MS,
                key="symbol",
                event_types=("market.bar",),
                description=(
                    "Immutable raw OHLCV bars from the simulator or a market-data provider. "
                    "Longest retention of the hot topics so any consumer can be rebuilt by replay."
                ),
            ),
            TopicSpec(
                name=Topic.MARKET_ENRICHED,
                partitions=6,
                retention_ms=3 * _DAY_MS,
                key="symbol",
                event_types=("market.enriched",),
                description=(
                    "Bars plus rolling technical indicators. Derived data: can be regenerated "
                    "from market.raw, so it is retained for less time."
                ),
            ),
            TopicSpec(
                name=Topic.MARKET_ANOMALIES,
                partitions=3,
                retention_ms=30 * _DAY_MS,
                key="symbol",
                event_types=("market.anomaly",),
                description="Price / volume anomaly detections. Low volume, high value.",
            ),
            TopicSpec(
                name=Topic.MARKET_PREDICTIONS,
                partitions=3,
                retention_ms=30 * _DAY_MS,
                key="symbol",
                event_types=("market.prediction",),
                description=(
                    "Model predictions with model/feature versions; consumed by persistence, "
                    "the API cache and the model monitor."
                ),
            ),
            TopicSpec(
                name=Topic.DEAD_LETTER,
                partitions=1,
                retention_ms=14 * _DAY_MS,
                key="original key (symbol) or original topic",
                event_types=("dead_letter",),
                description=(
                    "Poison messages and events that exhausted their retries, wrapped with "
                    "their origin coordinates. Single partition: low volume, global order "
                    "helps triage."
                ),
            ),
        )
    }
)


def spec_for(topic: Topic | str) -> TopicSpec:
    return TOPICS[Topic(topic)]

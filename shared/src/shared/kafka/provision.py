"""Idempotent Kafka topic provisioning from :mod:`shared.kafka.topics`.

Broker auto-topic-creation is disabled in every environment: a typo in a
topic name must fail loudly, not create a 1-partition topic with default
retention. This tool is the only thing that creates topics.

Behaviour
---------
* missing topic                -> create with the declared partitions/configs
* fewer partitions than spec   -> increase (NOTE: remaps keys to partitions,
                                  so per-symbol ordering is only guaranteed
                                  for messages produced after the change)
* more partitions than spec    -> warn; Kafka cannot shrink a topic
* managed configs              -> always (re)applied with incremental alter,
                                  which is idempotent

Run: ``python -m shared.kafka.provision [--dry-run]`` or ``make topics``.
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any

from shared.config import KafkaSettings, LogSettings
from shared.kafka.topics import TOPICS, TopicSpec
from shared.observability.logs import configure_logging, get_logger
from shared.utils.retry import BackoffPolicy, retry_call

log = get_logger(__name__)


@dataclass(frozen=True, slots=True)
class CreateTopic:
    spec: TopicSpec


@dataclass(frozen=True, slots=True)
class IncreasePartitions:
    spec: TopicSpec
    current: int


@dataclass(frozen=True, slots=True)
class PartitionSurplus:
    spec: TopicSpec
    current: int


@dataclass(frozen=True, slots=True)
class ApplyConfigs:
    spec: TopicSpec


Action = CreateTopic | IncreasePartitions | PartitionSurplus | ApplyConfigs


def plan(existing_partitions: Mapping[str, int], specs: Iterable[TopicSpec]) -> list[Action]:
    """Pure diff between the declared topology and what the cluster has."""
    actions: list[Action] = []
    for spec in specs:
        current = existing_partitions.get(spec.name)
        if current is None:
            actions.append(CreateTopic(spec))
            continue
        if current < spec.partitions:
            actions.append(IncreasePartitions(spec, current))
        elif current > spec.partitions:
            actions.append(PartitionSurplus(spec, current))
        actions.append(ApplyConfigs(spec))
    return actions


class TopicProvisioner:
    def __init__(self, admin: Any, settings: KafkaSettings, *, timeout_s: float = 30.0) -> None:
        self._admin = admin
        self._settings = settings
        self._timeout_s = timeout_s

    def existing_partitions(self) -> dict[str, int]:
        metadata = self._admin.list_topics(timeout=self._timeout_s)
        return {
            name: len(topic.partitions)
            for name, topic in metadata.topics.items()
            if not name.startswith("__")
        }

    def apply(self, actions: list[Action]) -> None:
        from confluent_kafka.admin import (  # noqa: PLC0415 - optional dependency
            AlterConfigOpType,
            ConfigEntry,
            ConfigResource,
            NewPartitions,
            NewTopic,
        )

        mis = self._settings.topic_min_insync_replicas
        creates = [a.spec for a in actions if isinstance(a, CreateTopic)]
        if creates:
            futures = self._admin.create_topics(
                [
                    NewTopic(
                        spec.name,
                        num_partitions=spec.partitions,
                        replication_factor=self._settings.topic_replication_factor,
                        config=spec.configs(min_insync_replicas=mis),
                    )
                    for spec in creates
                ],
                operation_timeout=self._timeout_s,
            )
            self._wait(futures, "create")

        grows = [a for a in actions if isinstance(a, IncreasePartitions)]
        if grows:
            futures = self._admin.create_partitions(
                [NewPartitions(a.spec.name, a.spec.partitions) for a in grows],
                operation_timeout=self._timeout_s,
            )
            self._wait(futures, "increase_partitions")

        for action in actions:
            if isinstance(action, PartitionSurplus):
                log.warning(
                    "topic.partition_surplus",
                    topic=action.spec.name,
                    current=action.current,
                    declared=action.spec.partitions,
                    hint="Kafka cannot shrink topics; update topics.py or recreate the topic",
                )

        alters = [a.spec for a in actions if isinstance(a, ApplyConfigs)]
        if alters:
            resources = [
                ConfigResource(
                    ConfigResource.Type.TOPIC,
                    spec.name,
                    incremental_configs=[
                        ConfigEntry(k, v, incremental_operation=AlterConfigOpType.SET)
                        for k, v in spec.configs(min_insync_replicas=mis).items()
                    ],
                )
                for spec in alters
            ]
            futures = self._admin.incremental_alter_configs(resources)
            self._wait({str(r): f for r, f in futures.items()}, "alter_configs")

    def _wait(self, futures: Mapping[str, Any], operation: str) -> None:
        for name, future in futures.items():
            future.result(timeout=self._timeout_s)
            log.info("topic.provisioned", topic=name, operation=operation)


def _describe(action: Action) -> str:
    match action:
        case CreateTopic(spec):
            return f"CREATE   {spec.name} partitions={spec.partitions}"
        case IncreasePartitions(spec, current):
            return f"GROW     {spec.name} partitions {current} -> {spec.partitions}"
        case PartitionSurplus(spec, current):
            return f"WARN     {spec.name} has {current} partitions > declared {spec.partitions}"
        case ApplyConfigs(spec):
            return f"CONFIG   {spec.name} retention.ms={spec.retention_ms}"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Provision Kafka topics declared in topics.py")
    parser.add_argument("--dry-run", action="store_true", help="print the plan only")
    parser.add_argument("--timeout", type=float, default=30.0)
    parser.add_argument("--connect-attempts", type=int, default=10)
    args = parser.parse_args(argv)

    log_settings = LogSettings()
    configure_logging("topic-provisioner", level=log_settings.level, fmt=log_settings.format)
    from confluent_kafka.admin import AdminClient  # noqa: PLC0415 - optional dependency

    from shared.kafka.client_config import admin_config  # noqa: PLC0415

    settings = KafkaSettings()
    provisioner = TopicProvisioner(
        AdminClient(admin_config(settings)), settings, timeout_s=args.timeout
    )
    # The broker may still be starting (compose/k8s ordering is best effort).
    try:
        existing = retry_call(
            provisioner.existing_partitions,
            policy=BackoffPolicy(
                max_attempts=args.connect_attempts, base_delay_s=1.0, max_delay_s=8.0
            ),
            operation="kafka.list_topics",
        )
    except Exception as exc:  # CLI boundary: report and exit non-zero
        log.error(
            "topic.provisioning_failed",
            bootstrap_servers=settings.bootstrap_servers,
            error=str(exc),
        )
        return 1
    actions = plan(existing, TOPICS.values())
    for action in actions:
        log.info("topic.plan", action=_describe(action))
    if args.dry_run:
        return 0
    provisioner.apply(actions)
    log.info("topic.provisioning_complete", topics=len(TOPICS))
    return 0


if __name__ == "__main__":
    sys.exit(main())

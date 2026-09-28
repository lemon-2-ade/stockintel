"""librdkafka configuration with production-minded defaults.

Plain dicts, no ``confluent_kafka`` import, so the choices are unit-testable
and reviewable in one place.

Delivery semantics (see docs/KAFKA_DESIGN.md):

* Producers: idempotent (``enable.idempotence``) + ``acks=all`` -> no
  duplicates or reordering *caused by producer retries* within a partition.
* Consumers: at-least-once. The offset of a message is *stored* only after it
  has been fully handled (``enable.auto.offset.store=false`` +
  ``store_offsets``); the background auto-commit then commits only stored
  offsets. This is librdkafka's recommended at-least-once pattern: no
  per-message synchronous commit round-trip, yet nothing unprocessed is ever
  committed. Duplicates remain possible after a crash or rebalance, so every
  sink must be idempotent.
"""

from __future__ import annotations

from typing import Any

from shared.config import KafkaSettings


def _common(settings: KafkaSettings, client_id: str) -> dict[str, Any]:
    conf: dict[str, Any] = {
        "bootstrap.servers": settings.bootstrap_servers,
        "client.id": client_id,
        "security.protocol": settings.security_protocol,
        # Emit librdkafka stats every 15s; services forward them to Prometheus.
        "statistics.interval.ms": 15_000,
    }
    if settings.sasl_mechanism:
        conf["sasl.mechanism"] = settings.sasl_mechanism
        conf["sasl.username"] = settings.sasl_username
        conf["sasl.password"] = (
            settings.sasl_password.get_secret_value() if settings.sasl_password else None
        )
    return conf


def producer_config(settings: KafkaSettings, *, client_id: str, **overrides: Any) -> dict[str, Any]:
    conf = _common(settings, client_id) | {
        "enable.idempotence": True,
        "acks": "all",
        # <= 5 in-flight keeps ordering guarantees with idempotence enabled.
        "max.in.flight.requests.per.connection": 5,
        "compression.type": "lz4",
        # Small linger trades ~5 ms latency for far better batching throughput.
        "linger.ms": 5,
        "batch.size": 131_072,
        # Upper bound on a record's total time in the producer incl. retries.
        "delivery.timeout.ms": 120_000,
        "retry.backoff.ms": 100,
        "retry.backoff.max.ms": 1_000,
    }
    return conf | overrides


def consumer_config(
    settings: KafkaSettings, *, group_id: str, client_id: str, **overrides: Any
) -> dict[str, Any]:
    conf = _common(settings, client_id) | {
        "group.id": group_id,
        "enable.auto.commit": True,
        # Offsets are *stored* manually after successful processing; the
        # background auto-commit then only commits what we explicitly stored.
        "enable.auto.offset.store": False,
        "auto.commit.interval.ms": 1_000,
        "auto.offset.reset": "earliest",
        # Incremental rebalancing: scaling a group does not stop every member.
        "partition.assignment.strategy": "cooperative-sticky",
        "session.timeout.ms": 45_000,
        "max.poll.interval.ms": 300_000,
        "isolation.level": "read_committed",
    }
    return conf | overrides


def admin_config(
    settings: KafkaSettings, *, client_id: str = "topic-provisioner"
) -> dict[str, Any]:
    return _common(settings, client_id)

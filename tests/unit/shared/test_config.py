from __future__ import annotations

import pytest
from pydantic import SecretStr, ValidationError

from shared.config import KafkaSettings, PostgresSettings
from shared.kafka.client_config import admin_config, consumer_config, producer_config


def test_kafka_settings_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    monkeypatch.setenv("KAFKA_TOPIC_REPLICATION_FACTOR", "3")
    monkeypatch.setenv("KAFKA_TOPIC_MIN_INSYNC_REPLICAS", "2")
    settings = KafkaSettings()
    assert settings.bootstrap_servers == "kafka:9092"
    assert settings.topic_replication_factor == 3


def test_min_isr_cannot_exceed_replication() -> None:
    with pytest.raises(ValidationError, match="min_insync"):
        KafkaSettings(topic_replication_factor=1, topic_min_insync_replicas=2)


def test_sasl_requires_credentials() -> None:
    with pytest.raises(ValidationError, match="SASL"):
        KafkaSettings(security_protocol="SASL_SSL")


def test_postgres_url_escapes_and_hides_password() -> None:
    settings = PostgresSettings(password=SecretStr("p@ss:w/rd%"), host="db")
    assert settings.sqlalchemy_url() == (
        "postgresql+psycopg://stockintel:p%40ss%3Aw%2Frd%25@db:5432/stockintel"
    )
    assert "p@ss" not in repr(settings)


def test_producer_is_idempotent_and_durable() -> None:
    conf = producer_config(KafkaSettings(), client_id="p")
    assert conf["enable.idempotence"] is True
    assert conf["acks"] == "all"
    assert conf["max.in.flight.requests.per.connection"] <= 5


def test_consumer_is_at_least_once() -> None:
    conf = consumer_config(KafkaSettings(), group_id="g", client_id="c")
    assert conf["enable.auto.offset.store"] is False
    assert conf["group.id"] == "g"
    assert conf["partition.assignment.strategy"] == "cooperative-sticky"


def test_overrides_win() -> None:
    assert producer_config(KafkaSettings(), client_id="p", **{"linger.ms": 50})["linger.ms"] == 50


def test_sasl_settings_are_forwarded() -> None:
    settings = KafkaSettings(
        security_protocol="SASL_SSL",
        sasl_mechanism="SCRAM-SHA-512",
        sasl_username="svc",
        sasl_password=SecretStr("s3cret"),
    )
    conf = admin_config(settings)
    assert conf["sasl.username"] == "svc"
    assert conf["sasl.password"] == "s3cret"
    assert conf["security.protocol"] == "SASL_SSL"

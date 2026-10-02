"""Fixtures for tests against live infrastructure (``make test-integration``).

These fixtures *fail* rather than skip when the infrastructure is missing:
an integration run that silently skips everything is worse than no run.
Start the stack first with ``make infra-up``.
"""

from __future__ import annotations

from collections.abc import Iterator

import pytest
import redis
from sqlalchemy import Engine, text

from shared.config import KafkaSettings, PostgresSettings, RedisSettings
from shared.db.session import create_db_engine


@pytest.fixture(scope="session")
def postgres_settings() -> PostgresSettings:
    return PostgresSettings()


@pytest.fixture(scope="session")
def kafka_settings() -> KafkaSettings:
    return KafkaSettings()


@pytest.fixture(scope="session")
def pg_engine(postgres_settings: PostgresSettings) -> Iterator[Engine]:
    engine = create_db_engine(postgres_settings, application_name="integration-tests")
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
    except Exception as exc:
        pytest.fail(f"PostgreSQL unreachable at {postgres_settings.host}: {exc}", pytrace=False)
    yield engine
    engine.dispose()


@pytest.fixture(scope="session")
def redis_settings() -> RedisSettings:
    return RedisSettings()


@pytest.fixture
def redis_client(redis_settings: RedisSettings) -> Iterator[redis.Redis]:
    client = redis.Redis(**redis_settings.client_kwargs())  # type: ignore[arg-type]
    try:
        client.ping()
    except redis.RedisError as exc:
        pytest.fail(f"Redis unreachable at {redis_settings.host}: {exc}", pytrace=False)
    yield client
    client.close()


@pytest.fixture
def migrated_engine(pg_engine: Engine, postgres_settings: PostgresSettings) -> Engine:
    """An engine on a database migrated to head (idempotent)."""
    from alembic import command  # noqa: PLC0415

    from shared.db.migrate import alembic_config  # noqa: PLC0415

    command.upgrade(alembic_config(postgres_settings), "head")
    return pg_engine

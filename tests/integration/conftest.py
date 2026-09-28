"""Fixtures for tests against live infrastructure (``make test-integration``).

These fixtures *fail* rather than skip when the infrastructure is missing:
an integration run that silently skips everything is worse than no run.
Start the stack first with ``make infra-up``.
"""

from __future__ import annotations

from collections.abc import Iterator

import pytest
from sqlalchemy import Engine, text

from shared.config import KafkaSettings, PostgresSettings
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

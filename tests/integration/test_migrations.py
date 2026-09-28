"""The Alembic migration chain is the schema; the ORM models must agree with it."""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from datetime import UTC, datetime

import pytest
from alembic import command
from alembic.autogenerate import compare_metadata
from alembic.migration import MigrationContext
from sqlalchemy import Engine, inspect, text
from sqlalchemy.exc import IntegrityError

from shared.config import PostgresSettings
from shared.db.migrate import alembic_config
from shared.db.models import Base

pytestmark = pytest.mark.integration

EXPECTED_TABLES = {
    "symbols",
    "market_bars",
    "bar_indicators",
    "anomalies",
    "predictions",
    "prediction_outcomes",
    "model_deployments",
    "monitoring_reports",
    "watchlist_items",
}


@pytest.fixture(scope="module")
def migrated(pg_engine: Engine, postgres_settings: PostgresSettings) -> Iterator[Engine]:
    config = alembic_config(postgres_settings)
    command.downgrade(config, "base")
    command.upgrade(config, "head")
    yield pg_engine
    with pg_engine.begin() as conn:
        for table in ("market_bars", "anomalies", "watchlist_items", "symbols"):
            conn.execute(text(f"TRUNCATE {table} CASCADE"))


def test_all_tables_exist(migrated: Engine) -> None:
    assert set(inspect(migrated).get_table_names()) >= EXPECTED_TABLES


def test_models_match_migrations(migrated: Engine) -> None:
    """Fails if someone edits models.py without generating a migration (or vice versa)."""
    with migrated.connect() as conn:
        diff = compare_metadata(
            MigrationContext.configure(conn, opts={"compare_type": True}), Base.metadata
        )
    assert diff == []


def _insert_bar(conn: object, **overrides: object) -> int:
    row = {
        "symbol": "AAPL",
        "bar_interval": "1m",
        "ts": datetime(2026, 1, 5, 14, 30, tzinfo=UTC),
        "open": 100.0,
        "high": 101.0,
        "low": 99.0,
        "close": 100.5,
        "volume": 10,
        "source": "test",
        "event_id": uuid.uuid4(),
    } | overrides
    result = conn.execute(  # type: ignore[attr-defined]
        text(
            "INSERT INTO market_bars (symbol, bar_interval, ts, open, high, low, close, volume,"
            " source, event_id) VALUES (:symbol, :bar_interval, :ts, :open, :high, :low, :close,"
            " :volume, :source, :event_id) ON CONFLICT DO NOTHING"
        ),
        row,
    )
    return int(result.rowcount)


def test_redelivered_bar_is_idempotent(migrated: Engine) -> None:
    event_id = uuid.uuid4()
    with migrated.begin() as conn:
        assert _insert_bar(conn, symbol="IDEM", event_id=event_id) == 1
        assert _insert_bar(conn, symbol="IDEM", event_id=event_id) == 0
        count = conn.execute(text("SELECT count(*) FROM market_bars WHERE symbol='IDEM'"))
        assert count.scalar_one() == 1


@pytest.mark.parametrize(
    "overrides",
    [
        {"high": 98.0},  # high below low/open/close
        {"low": 100.8},  # low above open
        {"volume": -1},
        {"open": 0.0},
        {"bar_interval": "7m"},
    ],
)
def test_database_rejects_impossible_bars(migrated: Engine, overrides: dict[str, object]) -> None:
    """Defence in depth: even a buggy writer cannot persist an impossible bar."""
    with pytest.raises(IntegrityError), migrated.begin() as conn:
        _insert_bar(conn, symbol="BAD", **overrides)


def test_watchlist_requires_known_symbol(migrated: Engine) -> None:
    with pytest.raises(IntegrityError), migrated.begin() as conn:
        conn.execute(text("INSERT INTO watchlist_items (user_id, symbol) VALUES ('u1', 'ZZZZ')"))
    with migrated.begin() as conn:
        conn.execute(text("INSERT INTO symbols (symbol, name) VALUES ('WLST', 'Watch Co')"))
        conn.execute(text("INSERT INTO watchlist_items (user_id, symbol) VALUES ('u1', 'WLST')"))


def test_downgrade_is_clean(pg_engine: Engine, postgres_settings: PostgresSettings) -> None:
    config = alembic_config(postgres_settings)
    command.downgrade(config, "base")
    remaining = set(inspect(pg_engine).get_table_names()) - {"alembic_version"}
    assert remaining.isdisjoint(EXPECTED_TABLES)
    command.upgrade(config, "head")

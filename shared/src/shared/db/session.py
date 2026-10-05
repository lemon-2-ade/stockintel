"""Engine factory with pooling and fail-fast timeouts."""

from __future__ import annotations

from sqlalchemy import Engine, create_engine
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from shared.config import PostgresSettings


def create_db_engine(settings: PostgresSettings, *, application_name: str) -> Engine:
    """Synchronous engine; async services build theirs the same way via create_async_engine.

    * ``pool_pre_ping`` transparently replaces connections killed by a DB restart.
    * ``statement_timeout`` bounds any single query so one bad request cannot
      pin a pooled connection forever.
    * ``application_name`` makes each service identifiable in pg_stat_activity.
    """
    return create_engine(
        settings.sqlalchemy_url(),
        pool_size=settings.pool_size,
        max_overflow=settings.max_overflow,
        pool_pre_ping=True,
        pool_recycle=1_800,
        connect_args={
            "application_name": application_name,
            "connect_timeout": 5,
            "options": f"-c statement_timeout={settings.statement_timeout_ms}",
        },
    )


def create_async_db_engine(settings: PostgresSettings, *, application_name: str) -> AsyncEngine:
    """Async engine (psycopg 3 async driver) for asyncio services such as the API."""
    return create_async_engine(
        settings.sqlalchemy_url(),
        pool_size=settings.pool_size,
        max_overflow=settings.max_overflow,
        pool_pre_ping=True,
        pool_recycle=1_800,
        pool_timeout=5,
        connect_args={
            "application_name": application_name,
            "connect_timeout": 5,
            "options": f"-c statement_timeout={settings.statement_timeout_ms}",
        },
    )

"""The promotion audit log in PostgreSQL commits together with the registry change."""

from __future__ import annotations

import uuid

import pytest
from sqlalchemy import select
from sqlalchemy.engine import Engine

from shared.db.models import ModelDeployment
from stockml.registry.audit import AuditEntry, PostgresAuditSink

pytestmark = pytest.mark.integration


def rows(engine: Engine, model_name: str) -> list[str]:
    query = select(ModelDeployment.to_stage).where(ModelDeployment.model_name == model_name)
    with engine.connect() as conn:
        return list(conn.scalars(query))


def test_rows_commit_with_the_change_and_roll_back_without_it(migrated_engine: Engine) -> None:
    name = f"audit-test-{uuid.uuid4().hex[:8]}"
    sink = PostgresAuditSink(migrated_engine)
    entry = AuditEntry(name, "1", "candidate", "champion", "test", "pytest", {"gates": {}})
    applied: list[bool] = []

    sink.record([entry], lambda: applied.append(True))
    assert applied == [True]
    assert len(rows(migrated_engine, name)) == 1

    def failing() -> None:
        raise RuntimeError("registry unavailable")

    with pytest.raises(RuntimeError):
        sink.record([entry], failing)
    assert len(rows(migrated_engine, name)) == 1, "the failed change left no audit row"

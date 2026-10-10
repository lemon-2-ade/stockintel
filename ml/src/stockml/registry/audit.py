"""Audit log of model stage transitions.

The system of record is the ``model_deployments`` table in PostgreSQL (created
by the Phase 1 migrations). A JSON-lines file sink exists for working without
the stack (``--audit-file``); it records exactly the same entries.

Ordering: the audit rows are inserted inside a transaction, the registry
change runs, and only then is the transaction committed. If the registry
change fails, the rows roll back; if the database is down, nothing changes in
the registry. The one remaining window (registry updated, then the commit
fails) is logged loudly by the caller.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

from sqlalchemy import insert
from sqlalchemy.engine import Engine

from shared.db.models import ModelDeployment


@dataclass(frozen=True, slots=True)
class AuditEntry:
    model_name: str
    model_version: str
    from_stage: str | None
    to_stage: str
    reason: str
    actor: str
    metrics: dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))


class AuditSink(Protocol):
    def record(self, entries: list[AuditEntry], apply: Callable[[], None]) -> None:
        """Run ``apply`` (the registry change) and persist ``entries`` together."""
        ...


class PostgresAuditSink:
    def __init__(self, engine: Engine) -> None:
        self.engine = engine

    def record(self, entries: list[AuditEntry], apply: Callable[[], None]) -> None:
        rows = [asdict(e) for e in entries]
        with self.engine.begin() as conn:  # one transaction for all entries
            conn.execute(insert(ModelDeployment), rows)
            apply()


class JsonlAuditSink:
    def __init__(self, path: Path) -> None:
        self.path = path

    def record(self, entries: list[AuditEntry], apply: Callable[[], None]) -> None:
        apply()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        lines = "".join(json.dumps(asdict(e), default=str) + "\n" for e in entries)
        with self.path.open("a", encoding="utf-8") as handle:
            handle.write(lines)


class MemoryAuditSink:
    """For tests."""

    def __init__(self) -> None:
        self.entries: list[AuditEntry] = []

    def record(self, entries: list[AuditEntry], apply: Callable[[], None]) -> None:
        apply()
        self.entries.extend(entries)

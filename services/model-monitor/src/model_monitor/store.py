"""Reading served predictions and writing monitoring reports (PostgreSQL),
and fetching reference profiles (MLflow)."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Protocol

from sqlalchemy import insert, text
from sqlalchemy.engine import Engine

from model_monitor.checks import Report, ServedRow
from shared.db.models import MonitoringReport
from shared.monitoring import ReferenceProfile
from shared.observability.logs import get_logger

log = get_logger(__name__)

WINDOW_SQL = text(
    """
    SELECT p.model_name, p.model_version, p.features,
           (p.class_probabilities ->> 'up')::double precision AS p_up,
           o.actual_direction
    FROM predictions p
    LEFT JOIN prediction_outcomes o ON o.prediction_id = p.prediction_id
    WHERE p.ts > :start AND p.ts <= :end AND p.task = 'direction'
    ORDER BY p.ts DESC
    LIMIT :limit
    """
)


def load_window(engine: Engine, start: datetime, end: datetime, limit: int) -> list[ServedRow]:
    with engine.connect() as conn:
        rows = conn.execute(WINDOW_SQL, {"start": start, "end": end, "limit": limit}).all()
    return [
        ServedRow(
            model_name=r.model_name,
            model_version=r.model_version,
            features=r.features,
            p_up=r.p_up,
            actual_direction=r.actual_direction,
        )
        for r in rows
    ]


def write_reports(
    engine: Engine, reports: list[Report], window_start: datetime, window_end: datetime
) -> None:
    if not reports:
        return
    rows = [
        {
            "report_type": r.report_type,
            "model_name": r.model_name,
            "model_version": r.model_version,
            "window_start": window_start,
            "window_end": window_end,
            "metric_name": r.metric_name,
            "feature_name": r.feature_name,
            "value": r.value,
            "threshold": r.threshold,
            "status": r.status,
            "details": r.details,
        }
        for r in reports
    ]
    with engine.begin() as conn:
        conn.execute(insert(MonitoringReport), rows)


class ProfileSource(Protocol):
    def get(self, model_name: str, model_version: str) -> ReferenceProfile | None: ...


class MlflowProfileSource:
    """Downloads ``monitoring/reference_profile.json`` from the version's run, cached."""

    ARTIFACT = "monitoring/reference_profile.json"

    def __init__(self) -> None:
        self._cache: dict[tuple[str, str], ReferenceProfile | None] = {}

    def get(self, model_name: str, model_version: str) -> ReferenceProfile | None:
        key = (model_name, model_version)
        if key not in self._cache:
            self._cache[key] = self._fetch(model_name, model_version)
        return self._cache[key]

    def _fetch(self, model_name: str, model_version: str) -> ReferenceProfile | None:
        import mlflow  # noqa: PLC0415 - heavy import, only when monitoring
        from mlflow import MlflowClient  # noqa: PLC0415

        try:
            run_id = MlflowClient().get_model_version(model_name, model_version).run_id
            path = mlflow.artifacts.download_artifacts(run_id=run_id, artifact_path=self.ARTIFACT)
            return ReferenceProfile.from_dict(json.loads(Path(path).read_text()))
        except Exception as exc:  # absent for versions registered before Phase 9
            log.warning(
                "monitor.reference_unavailable",
                model=model_name,
                version=model_version,
                error=str(exc)[:200],
            )
            return None

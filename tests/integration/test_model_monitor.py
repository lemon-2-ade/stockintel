"""Outcome resolution and a full monitor cycle against PostgreSQL."""

from __future__ import annotations

import math
import uuid
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta

import numpy as np
import pytest
from sqlalchemy import delete, insert, select
from sqlalchemy.engine import Engine

from model_monitor.main import MonitorMetrics, MonitorSettings, run_cycle
from model_monitor.outcomes import resolve_outcomes
from shared.db.models import MarketBar, MonitoringReport, Prediction, PredictionOutcome
from shared.features import FEATURE_NAMES, FEATURE_SET_VERSION
from shared.monitoring import ReferenceProfile, profile

pytestmark = pytest.mark.integration

T0 = datetime(2026, 3, 2, 14, 30, tzinfo=UTC)


@pytest.fixture(name="symbol")
def symbol_fixture(migrated_engine: Engine) -> Iterator[str]:
    symbol = f"T{uuid.uuid4().hex[:6].upper()}"
    yield symbol
    with migrated_engine.begin() as conn:
        ids = select(Prediction.prediction_id).where(Prediction.symbol == symbol)
        conn.execute(delete(PredictionOutcome).where(PredictionOutcome.prediction_id.in_(ids)))
        conn.execute(delete(Prediction).where(Prediction.symbol == symbol))
        conn.execute(delete(MarketBar).where(MarketBar.symbol == symbol))
        conn.execute(delete(MonitoringReport).where(MonitoringReport.model_name == symbol))


def bars(engine: Engine, symbol: str, closes: list[float]) -> None:
    rows = [
        {
            "symbol": symbol,
            "bar_interval": "1m",
            "ts": T0 + timedelta(minutes=i),
            "open": c,
            "high": c,
            "low": c,
            "close": c,
            "volume": 100,
            "source": "test",
            "event_id": uuid.uuid4(),
        }
        for i, c in enumerate(closes)
    ]
    with engine.begin() as conn:
        conn.execute(insert(MarketBar), rows)


def prediction(
    symbol: str, i: int, p_up: float, *, model: str, horizon: int = 5
) -> dict[str, object]:
    rng = np.random.default_rng(i)
    return {
        "prediction_id": uuid.uuid4(),
        "symbol": symbol,
        "bar_interval": "1m",
        "ts": T0 + timedelta(minutes=i),
        "target_ts": T0 + timedelta(minutes=i + horizon),
        "horizon_bars": horizon,
        "task": "direction",
        "predicted_direction": "up" if p_up >= 0.5 else "down",
        "class_probabilities": {"up": p_up, "down": 1 - p_up},
        "confidence": max(p_up, 1 - p_up),
        "model_name": model,
        "model_version": "1",
        "feature_set_version": FEATURE_SET_VERSION,
        "features": {f: float(rng.normal()) for f in FEATURE_NAMES},
        "source_event_id": uuid.uuid4(),
    }


def outcomes(engine: Engine, symbol: str) -> dict[datetime, tuple[str, bool | None, float]]:
    query = (
        select(Prediction.ts, PredictionOutcome)
        .join(PredictionOutcome, PredictionOutcome.prediction_id == Prediction.prediction_id)
        .where(Prediction.symbol == symbol)
    )
    with engine.connect() as conn:
        return {
            row.ts: (row.actual_direction, row.is_correct, row.actual_return)
            for row in conn.execute(query)
        }


def test_outcomes_use_the_target_bar_and_are_idempotent(
    migrated_engine: Engine, symbol: str
) -> None:
    # Prices rise for 10 minutes, then the stream stops.
    bars(migrated_engine, symbol, [100 + i for i in range(10)])
    rows = [
        prediction(symbol, 0, 0.7, model=symbol),  # target minute 5: resolvable, correct
        prediction(symbol, 1, 0.3, model=symbol),  # target minute 6: resolvable, wrong
        prediction(symbol, 8, 0.6, model=symbol),  # target minute 13: no bar yet
    ]
    with migrated_engine.begin() as conn:
        conn.execute(insert(Prediction), rows)
    now = T0 + timedelta(hours=1)
    assert resolve_outcomes(migrated_engine, now=now, lookback=timedelta(days=1)) == 2
    assert resolve_outcomes(migrated_engine, now=now, lookback=timedelta(days=1)) == 0
    resolved = outcomes(migrated_engine, symbol)
    direction, correct, actual = resolved[T0]
    assert (direction, correct) == ("up", True)
    assert actual == pytest.approx(math.log(105 / 100))
    assert resolved[T0 + timedelta(minutes=1)][1] is False
    assert T0 + timedelta(minutes=8) not in resolved, "never scored against a later price"


class StaticProfiles:
    def __init__(self, ref: ReferenceProfile) -> None:
        self.ref = ref

    def get(self, model_name: str, model_version: str) -> ReferenceProfile | None:
        return self.ref


def test_monitor_cycle_writes_reports(migrated_engine: Engine, symbol: str) -> None:
    rng = np.random.default_rng(0)
    ref = ReferenceProfile(
        feature_set_version=FEATURE_SET_VERSION,
        # Training inputs centred at +3: the served N(0, 1) inputs have drifted.
        features={f: profile(rng.normal(3.0, 1.0, 5_000)) for f in FEATURE_NAMES},
        prediction=profile(rng.normal(0.55, 0.02, 5_000)),
        base_rate=0.55,
    )
    closes = list(100 + np.cumsum(rng.normal(0, 0.5, 400)))
    bars(migrated_engine, symbol, [max(1.0, c) for c in closes])
    with migrated_engine.begin() as conn:
        conn.execute(
            insert(Prediction),
            [prediction(symbol, i, float(rng.uniform(0.4, 0.7)), model=symbol) for i in range(390)],
        )
    settings = MonitorSettings(window_hours=48, min_samples=200, min_outcomes=100)
    reports = run_cycle(
        migrated_engine,
        StaticProfiles(ref),
        settings,
        MonitorMetrics(),
        now=T0 + timedelta(hours=12),
    )
    by_metric = {(r.report_type, r.metric_name, r.feature_name): r for r in reports}
    assert by_metric[("data_drift", "psi", "ret_5")].status == "alert"
    assert ("performance", "log_loss_minus_null", None) in by_metric
    assert ("operational", "retrain_recommended", None) in by_metric
    with migrated_engine.connect() as conn:
        stored = conn.execute(
            select(MonitoringReport.report_type).where(MonitoringReport.model_name == symbol)
        ).all()
    assert len(stored) == len(reports)

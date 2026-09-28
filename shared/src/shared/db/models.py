"""PostgreSQL schema (SQLAlchemy 2.0 typed ORM declarations).

Postgres holds the **system of record** for everything that must be queried
historically or survive a restart: bars, derived indicators, anomalies,
predictions + their realised outcomes, model promotions, monitoring results
and watchlists. Hot "latest value" reads are served from Redis instead.

Conventions
-----------
* Every timestamp is ``timestamptz`` and stored in UTC.
* Idempotent writes: each table has a natural or deterministic key so that a
  redelivered Kafka event becomes ``INSERT ... ON CONFLICT DO NOTHING``.
* Enumerations are ``text`` + ``CHECK`` rather than native PG enums: adding a
  value to a PG enum cannot be rolled back inside a transaction, a CHECK can.
* No FK from ``market_bars`` to ``symbols``: the ingest hot path must not fail
  because reference data lags. User-facing tables (watchlist) do use FKs.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    Double,
    ForeignKey,
    Index,
    Integer,
    MetaData,
    String,
    Text,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB, TIMESTAMP, UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from shared.schemas.anomaly import AnomalyType, Severity
from shared.schemas.market import BarInterval
from shared.schemas.prediction import Direction, PredictionTask

# Deterministic constraint names -> reproducible, reviewable migrations.
NAMING_CONVENTION = {
    "ix": "ix_%(table_name)s_%(column_0_N_name)s",
    "uq": "uq_%(table_name)s_%(column_0_N_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}

TZ = TIMESTAMP(timezone=True)

MODEL_STAGES = ("candidate", "challenger", "champion", "archived")
REPORT_TYPES = ("data_quality", "data_drift", "prediction_drift", "performance", "operational")
REPORT_STATUSES = ("ok", "warning", "alert")


def _in(column: str, values: tuple[str, ...] | list[str]) -> str:
    quoted = ", ".join(f"'{v}'" for v in values)
    return f"{column} IN ({quoted})"


def _values(enum: Any) -> list[str]:
    return [member.value for member in enum]


class Base(DeclarativeBase):
    metadata = MetaData(naming_convention=NAMING_CONVENTION)


class Symbol(Base):
    """Reference data for tradable instruments."""

    __tablename__ = "symbols"

    symbol: Mapped[str] = mapped_column(String(15), primary_key=True)
    name: Mapped[str | None] = mapped_column(Text)
    exchange: Mapped[str | None] = mapped_column(String(32))
    currency: Mapped[str] = mapped_column(String(3), server_default=text("'USD'"))
    sector: Mapped[str | None] = mapped_column(String(64))
    is_active: Mapped[bool] = mapped_column(Boolean, server_default=text("true"))
    created_at: Mapped[datetime] = mapped_column(TZ, server_default=func.now())


class MarketBar(Base):
    """Raw OHLCV observations. Append-only; the source of truth for history."""

    __tablename__ = "market_bars"

    symbol: Mapped[str] = mapped_column(String(15), primary_key=True)
    bar_interval: Mapped[str] = mapped_column(String(4), primary_key=True)
    ts: Mapped[datetime] = mapped_column(TZ, primary_key=True, comment="Bar open time (UTC)")
    open: Mapped[float] = mapped_column(Double)
    high: Mapped[float] = mapped_column(Double)
    low: Mapped[float] = mapped_column(Double)
    close: Mapped[float] = mapped_column(Double)
    volume: Mapped[int] = mapped_column(BigInteger)
    source: Mapped[str] = mapped_column(String(64))
    event_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), unique=True)
    ingested_at: Mapped[datetime] = mapped_column(TZ, server_default=func.now())

    __table_args__ = (
        CheckConstraint("open > 0 AND high > 0 AND low > 0 AND close > 0", name="positive_prices"),
        CheckConstraint(
            "high >= GREATEST(open, close, low) AND low <= LEAST(open, close)", name="ohlc"
        ),
        CheckConstraint("volume >= 0", name="non_negative_volume"),
        CheckConstraint(_in("bar_interval", _values(BarInterval)), name="bar_interval"),
        # PK (symbol, bar_interval, ts) already serves "history for one symbol".
        # BRIN on ts is tiny and serves cross-symbol time-range scans
        # (retention jobs, "everything since X") on this append-only table.
        Index("ix_market_bars_ts_brin", "ts", postgresql_using="brin"),
    )


class BarIndicators(Base):
    """Indicator snapshot per bar, for historical charts of RSI/MACD/etc.

    JSONB because the indicator set evolves; ``indicator_version`` says how it
    was computed. Promote a key to a real column once it is filtered on.
    """

    __tablename__ = "bar_indicators"

    symbol: Mapped[str] = mapped_column(String(15), primary_key=True)
    bar_interval: Mapped[str] = mapped_column(String(4), primary_key=True)
    ts: Mapped[datetime] = mapped_column(TZ, primary_key=True)
    indicators: Mapped[dict[str, Any]] = mapped_column(JSONB)
    indicator_version: Mapped[str] = mapped_column(String(32))
    source_event_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True))
    computed_at: Mapped[datetime] = mapped_column(TZ, server_default=func.now())


class Anomaly(Base):
    __tablename__ = "anomalies"

    anomaly_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True)
    symbol: Mapped[str] = mapped_column(String(15))
    bar_interval: Mapped[str] = mapped_column(String(4))
    ts: Mapped[datetime] = mapped_column(TZ)
    anomaly_type: Mapped[str] = mapped_column(String(32))
    severity: Mapped[str] = mapped_column(String(16))
    observed_value: Mapped[float] = mapped_column(Double)
    expected_value: Mapped[float | None] = mapped_column(Double)
    score: Mapped[float] = mapped_column(Double)
    threshold: Mapped[float | None] = mapped_column(Double)
    detector: Mapped[str] = mapped_column(String(64))
    detector_version: Mapped[str] = mapped_column(String(32))
    source_event_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True))
    details: Mapped[dict[str, Any]] = mapped_column(JSONB, server_default=text("'{}'::jsonb"))
    created_at: Mapped[datetime] = mapped_column(TZ, server_default=func.now())

    __table_args__ = (
        CheckConstraint(_in("anomaly_type", _values(AnomalyType)), name="anomaly_type"),
        CheckConstraint(_in("severity", _values(Severity)), name="severity"),
        # One detector fires at most once per bar & type -> redelivery-safe.
        UniqueConstraint("source_event_id", "detector", "anomaly_type"),
        Index("ix_anomalies_symbol_ts", "symbol", text("ts DESC")),
        Index("ix_anomalies_ts", text("ts DESC")),
    )


class Prediction(Base):
    """Every served prediction, with enough context to audit and monitor it."""

    __tablename__ = "predictions"

    prediction_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, comment="Deterministic uuid5, see schemas"
    )
    symbol: Mapped[str] = mapped_column(String(15))
    bar_interval: Mapped[str] = mapped_column(String(4))
    ts: Mapped[datetime] = mapped_column(TZ, comment="As-of time of the features")
    target_ts: Mapped[datetime] = mapped_column(TZ)
    horizon_bars: Mapped[int] = mapped_column(Integer)
    task: Mapped[str] = mapped_column(String(16))
    predicted_direction: Mapped[str | None] = mapped_column(String(8))
    predicted_return: Mapped[float | None] = mapped_column(Double)
    class_probabilities: Mapped[dict[str, float] | None] = mapped_column(JSONB)
    confidence: Mapped[float | None] = mapped_column(Double)
    model_name: Mapped[str] = mapped_column(String(128))
    model_version: Mapped[str] = mapped_column(String(64))
    feature_set_version: Mapped[str] = mapped_column(String(64))
    features: Mapped[dict[str, Any] | None] = mapped_column(
        JSONB, comment="Model input snapshot, used for drift monitoring"
    )
    inference_latency_ms: Mapped[float | None] = mapped_column(Double)
    source_event_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True))
    created_at: Mapped[datetime] = mapped_column(TZ, server_default=func.now())

    __table_args__ = (
        CheckConstraint("target_ts > ts", name="target_after_asof"),
        CheckConstraint("horizon_bars >= 1", name="positive_horizon"),
        CheckConstraint(_in("task", _values(PredictionTask)), name="task"),
        CheckConstraint(
            f"predicted_direction IS NULL OR {_in('predicted_direction', _values(Direction))}",
            name="predicted_direction",
        ),
        CheckConstraint("confidence IS NULL OR confidence BETWEEN 0 AND 1", name="confidence"),
        Index("ix_predictions_symbol_ts", "symbol", text("ts DESC")),
        Index("ix_predictions_model_ts", "model_name", "model_version", "ts"),
        # Drives the outcome-resolution job: "predictions whose target is now in the past".
        Index("ix_predictions_target_ts", "target_ts"),
    )


class PredictionOutcome(Base):
    """Realised truth joined to a prediction once ``target_ts`` has passed."""

    __tablename__ = "prediction_outcomes"

    prediction_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("predictions.prediction_id", ondelete="CASCADE"),
        primary_key=True,
    )
    actual_return: Mapped[float] = mapped_column(Double)
    actual_direction: Mapped[str] = mapped_column(String(8))
    is_correct: Mapped[bool | None] = mapped_column(Boolean)
    abs_error: Mapped[float | None] = mapped_column(Double)
    resolved_at: Mapped[datetime] = mapped_column(TZ, server_default=func.now())

    __table_args__ = (
        CheckConstraint(_in("actual_direction", _values(Direction)), name="actual_direction"),
        Index("ix_prediction_outcomes_resolved_at", "resolved_at"),
    )


class ModelDeployment(Base):
    """Audit log of explicit model stage transitions (MLflow holds the artefacts).

    Promotion is a deliberate, attributable act - never a side effect of training.
    """

    __tablename__ = "model_deployments"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    model_name: Mapped[str] = mapped_column(String(128))
    model_version: Mapped[str] = mapped_column(String(64))
    from_stage: Mapped[str | None] = mapped_column(String(16))
    to_stage: Mapped[str] = mapped_column(String(16))
    reason: Mapped[str] = mapped_column(Text)
    actor: Mapped[str] = mapped_column(String(128))
    metrics: Mapped[dict[str, Any]] = mapped_column(JSONB, server_default=text("'{}'::jsonb"))
    created_at: Mapped[datetime] = mapped_column(TZ, server_default=func.now())

    __table_args__ = (
        CheckConstraint(_in("to_stage", MODEL_STAGES), name="to_stage"),
        CheckConstraint(
            f"from_stage IS NULL OR {_in('from_stage', MODEL_STAGES)}", name="from_stage"
        ),
        Index("ix_model_deployments_model_created", "model_name", text("created_at DESC")),
    )


class MonitoringReport(Base):
    """One row per computed monitoring metric (drift, performance, data quality)."""

    __tablename__ = "monitoring_reports"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    report_type: Mapped[str] = mapped_column(String(32))
    model_name: Mapped[str | None] = mapped_column(String(128))
    model_version: Mapped[str | None] = mapped_column(String(64))
    window_start: Mapped[datetime] = mapped_column(TZ)
    window_end: Mapped[datetime] = mapped_column(TZ)
    metric_name: Mapped[str] = mapped_column(String(64))
    feature_name: Mapped[str | None] = mapped_column(String(128))
    value: Mapped[float] = mapped_column(Double)
    threshold: Mapped[float | None] = mapped_column(Double)
    status: Mapped[str] = mapped_column(String(16))
    details: Mapped[dict[str, Any]] = mapped_column(JSONB, server_default=text("'{}'::jsonb"))
    created_at: Mapped[datetime] = mapped_column(TZ, server_default=func.now())

    __table_args__ = (
        CheckConstraint("window_end > window_start", name="window"),
        CheckConstraint(_in("report_type", REPORT_TYPES), name="report_type"),
        CheckConstraint(_in("status", REPORT_STATUSES), name="status"),
        Index("ix_monitoring_reports_type_created", "report_type", text("created_at DESC")),
    )


class WatchlistItem(Base):
    """Per-user watchlist. ``user_id`` is an opaque id until auth is added."""

    __tablename__ = "watchlist_items"

    user_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    symbol: Mapped[str] = mapped_column(
        String(15), ForeignKey("symbols.symbol", ondelete="CASCADE"), primary_key=True
    )
    position: Mapped[int] = mapped_column(Integer, server_default=text("0"))
    added_at: Mapped[datetime] = mapped_column(TZ, server_default=func.now())

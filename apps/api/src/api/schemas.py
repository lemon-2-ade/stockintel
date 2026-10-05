"""HTTP response models.

They deliberately keep the four kinds of information apart, so a client can
never mistake one for another:

* ``observation``: the raw market bar (what happened)
* ``analytics``: deterministic indicators computed from observations
* ``anomalies``: rule-based detections, with detector name and version
* ``prediction``: a model estimate, always with model version, horizon and a
  disclaimer; ``null`` when no fresh prediction exists
"""

from __future__ import annotations

from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from shared.schemas import (
    AnomalyType,
    BarInterval,
    Direction,
    IndicatorSnapshot,
    PredictionTask,
    Severity,
)

PREDICTION_DISCLAIMER = (
    "Statistical model estimate for research and demonstration only. "
    "Not investment advice; past accuracy does not imply future profitability."
)


class ApiModel(BaseModel):
    model_config = ConfigDict(frozen=True)


class Bar(ApiModel):
    timestamp: datetime = Field(description="Bar open time (UTC)")
    interval: BarInterval
    open: float
    high: float
    low: float
    close: float
    volume: int


class Freshness(ApiModel):
    as_of: datetime = Field(description="Event time the data refers to (bar close)")
    age_seconds: float
    stale: bool
    source: Literal["cache", "database"]


class StockSummary(ApiModel):
    symbol: str
    name: str | None = None
    last_price: float | None = None
    change: float | None = Field(default=None, description="vs previous bar close")
    change_pct: float | None = None
    volume: int | None = None
    timestamp: datetime | None = None
    stale: bool = True


class SessionStats(ApiModel):
    """Aggregates over today's (UTC) bars for the symbol's interval."""

    open: float
    high: float
    low: float
    volume: int
    change: float
    change_pct: float
    bars: int


class Prediction(ApiModel):
    kind: Literal["model_estimate"] = "model_estimate"
    prediction_id: UUID
    timestamp: datetime = Field(description="As-of time of the features")
    target_timestamp: datetime
    horizon_bars: int
    task: PredictionTask
    predicted_direction: Direction | None = None
    predicted_return: float | None = None
    class_probabilities: dict[str, float] | None = None
    confidence: float | None = None
    model_name: str
    model_version: str
    disclaimer: str = PREDICTION_DISCLAIMER


class StockDetail(ApiModel):
    symbol: str
    name: str | None = None
    observation: Bar
    analytics: IndicatorSnapshot | None = None
    session: SessionStats | None = None
    prediction: Prediction | None = None
    freshness: Freshness


class Anomaly(ApiModel):
    anomaly_id: UUID
    symbol: str
    timestamp: datetime
    interval: BarInterval
    anomaly_type: AnomalyType
    severity: Severity
    observed_value: float
    expected_value: float | None = None
    score: float
    threshold: float | None = None
    detector: str
    detector_version: str


class IndicatorPoint(ApiModel):
    timestamp: datetime
    indicator_version: str
    values: IndicatorSnapshot


class Page[T](ApiModel):
    items: list[T]
    next_before: datetime | None = Field(
        default=None,
        description="Pass as `before` to fetch the next (older) page; null when exhausted",
    )


class History(ApiModel):
    symbol: str
    interval: BarInterval
    bars: list[Bar] = Field(description="Ascending by timestamp")
    next_before: datetime | None = None


class WatchlistAdd(ApiModel):
    symbol: str = Field(pattern=r"^[A-Z][A-Z0-9.\-]{0,14}$")


class WatchlistItem(ApiModel):
    symbol: str
    position: int
    added_at: datetime
    quote: StockSummary | None = None


class Health(ApiModel):
    status: Literal["ok"] = "ok"


class Readiness(ApiModel):
    status: Literal["ready", "degraded", "unavailable"]
    checks: dict[str, str]

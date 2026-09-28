"""Anomaly detections (topic ``market.anomalies``)."""

from __future__ import annotations

from enum import StrEnum
from typing import Any, Literal
from uuid import UUID, uuid4

from pydantic import Field

from shared.schemas.base import BaseEvent, FiniteFloat, Identifier, Symbol, UtcDatetime
from shared.schemas.market import BarInterval


class AnomalyType(StrEnum):
    PRICE_SPIKE = "price_spike"
    """Large positive 1-bar return (threshold or volatility-adjusted)."""
    PRICE_DROP = "price_drop"
    """Large negative 1-bar return."""
    RETURN_ZSCORE = "return_zscore"
    """Return is a statistical outlier relative to its rolling distribution."""
    VOLUME_SPIKE = "volume_spike"
    """Volume far above its rolling baseline."""
    MULTIVARIATE_OUTLIER = "multivariate_outlier"
    """Flagged by an unsupervised model (e.g. Isolation Forest) over several features."""


class Severity(StrEnum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class AnomalyEvent(BaseEvent):
    """An unusual price or volume observation.

    ``observed_value`` / ``expected_value`` / ``threshold`` are in the unit of
    the detector (a return, a z-score, a volume ratio ...); ``detector`` and
    ``detector_version`` make every alert reproducible and attributable.
    """

    event_type: Literal["market.anomaly"] = "market.anomaly"
    schema_version: Literal[1] = 1

    anomaly_id: UUID = Field(default_factory=uuid4)
    symbol: Symbol
    timestamp: UtcDatetime = Field(description="Event time of the bar that triggered the anomaly.")
    interval: BarInterval
    anomaly_type: AnomalyType
    severity: Severity
    observed_value: FiniteFloat
    expected_value: FiniteFloat | None = Field(
        default=None, description="Baseline the observation was compared against, if any."
    )
    score: FiniteFloat = Field(description="Detector score, e.g. |z| or volume ratio.")
    threshold: FiniteFloat | None = None
    detector: Identifier
    detector_version: Identifier
    source_event_id: UUID
    details: dict[str, Any] = Field(default_factory=dict)

    def partition_key(self) -> str:
        return self.symbol

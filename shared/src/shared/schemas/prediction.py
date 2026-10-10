"""Model predictions (topic ``market.predictions``).

Predictions are **not** market observations and not investment advice: every
record carries the model identity and horizon so the UI and the monitoring
service can label and evaluate it as a statistical estimate.
"""

from __future__ import annotations

import math
from enum import StrEnum
from typing import Literal, Self
from uuid import NAMESPACE_URL, UUID, uuid5

from pydantic import Field, model_validator

from shared.schemas.base import (
    BaseEvent,
    FiniteFloat,
    Identifier,
    Probability,
    Symbol,
    UtcDatetime,
)
from shared.schemas.market import BarInterval

_PREDICTION_NAMESPACE = uuid5(NAMESPACE_URL, "https://stockintel.local/predictions")
_PROBABILITY_TOLERANCE = 1e-3


class PredictionTask(StrEnum):
    DIRECTION = "direction"
    """Classify the sign of the forward return over the horizon."""
    RETURN = "return"
    """Regress the forward return over the horizon."""


class Direction(StrEnum):
    UP = "up"
    DOWN = "down"
    NEUTRAL = "neutral"


def deterministic_prediction_id(
    source_event_id: UUID, model_name: str, model_version: str, horizon_bars: int
) -> UUID:
    """Stable id so that re-processing the same input with the same model is idempotent.

    A random uuid4 would turn every Kafka redelivery into a *second* prediction
    row and silently double-count it in monitoring metrics.
    """
    return uuid5(
        _PREDICTION_NAMESPACE, f"{source_event_id}:{model_name}:{model_version}:{horizon_bars}"
    )


class PredictionEvent(BaseEvent):
    event_type: Literal["market.prediction"] = "market.prediction"
    schema_version: Literal[1] = 1

    prediction_id: UUID
    symbol: Symbol
    timestamp: UtcDatetime = Field(
        description="As-of time: the latest bar whose information the features used."
    )
    interval: BarInterval
    horizon_bars: int = Field(ge=1, le=10_000)
    target_timestamp: UtcDatetime = Field(description="Event time the prediction is about.")
    task: PredictionTask
    predicted_direction: Direction | None = None
    predicted_return: FiniteFloat | None = None
    class_probabilities: dict[Direction, Probability] | None = None
    confidence: Probability | None = Field(
        default=None,
        description="Model's own score for the predicted class; NOT a calibrated guarantee.",
    )
    model_name: Identifier
    model_version: Identifier
    feature_set_version: Identifier
    inference_latency_ms: float | None = Field(default=None, ge=0)
    source_event_id: UUID
    features: dict[str, FiniteFloat] | None = Field(
        default=None,
        description="Model input snapshot, attached by the prediction pipeline for drift "
        "monitoring. Optional (tolerant-reader addition, schema_version stays 1).",
    )

    @model_validator(mode="after")
    def _check_consistency(self) -> Self:
        if self.target_timestamp <= self.timestamp:
            raise ValueError("target_timestamp must be after timestamp (no backward predictions)")
        if self.task is PredictionTask.DIRECTION and self.predicted_direction is None:
            raise ValueError("direction task requires predicted_direction")
        if self.task is PredictionTask.RETURN and self.predicted_return is None:
            raise ValueError("return task requires predicted_return")
        if self.class_probabilities is not None:
            total = math.fsum(self.class_probabilities.values())
            if abs(total - 1.0) > _PROBABILITY_TOLERANCE:
                raise ValueError(f"class_probabilities must sum to 1 (got {total:.6f})")
        return self

    def partition_key(self) -> str:
        return self.symbol

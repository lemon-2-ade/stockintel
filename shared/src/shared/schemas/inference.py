"""Request/response contract of the inference service (``POST /predict``).

Lives in ``shared`` because both sides use it: the inference service validates
requests with it, and the prediction pipeline (Phase 9) builds them.

The caller sends **features, not bars**: features are computed once, by the
same ``FeatureComputer`` used for training, next to the stream state that
feeds it. The request carries ``feature_set_version`` so the service can
refuse inputs computed by a different feature definition instead of silently
scoring them.
"""

from __future__ import annotations

import math
from typing import Annotated
from uuid import UUID

from pydantic import Field, field_validator

from shared.schemas.base import EventModel, Identifier, Symbol, UtcDatetime
from shared.schemas.market import BarInterval
from shared.schemas.prediction import PredictionEvent

MAX_BATCH = 256


class PredictInstance(EventModel):
    symbol: Symbol
    timestamp: UtcDatetime = Field(description="Close time of the bar the features describe.")
    interval: BarInterval
    source_event_id: UUID = Field(description="Event id of that bar, for idempotent ids.")
    feature_set_version: Identifier
    features: dict[str, float]

    @field_validator("features")
    @classmethod
    def _finite(cls, value: dict[str, float]) -> dict[str, float]:
        bad = sorted(k for k, v in value.items() if not math.isfinite(v))
        if bad:
            raise ValueError(f"non-finite feature values: {', '.join(bad)}")
        return value


class PredictRequest(EventModel):
    instances: Annotated[list[PredictInstance], Field(min_length=1, max_length=MAX_BATCH)]
    trace_id: str | None = Field(default=None, max_length=64)


class ModelInfo(EventModel):
    name: Identifier
    version: Identifier
    alias: Identifier
    feature_set_version: Identifier
    horizon_bars: int = Field(ge=1)
    run_id: str | None = None
    loaded_at: UtcDatetime


class PredictResponse(EventModel):
    model: ModelInfo
    predictions: list[PredictionEvent]

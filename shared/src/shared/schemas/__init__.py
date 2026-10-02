"""Versioned event contracts exchanged over Kafka."""

from shared.schemas.anomaly import AnomalyEvent, AnomalyType, Severity
from shared.schemas.base import BaseEvent, Symbol, UtcDatetime, derived_event_id, utcnow
from shared.schemas.dead_letter import DeadLetterEvent, FailureReason
from shared.schemas.market import (
    BarInterval,
    EnrichedBarEvent,
    IndicatorSnapshot,
    MarketBarEvent,
)
from shared.schemas.prediction import (
    Direction,
    PredictionEvent,
    PredictionTask,
    deterministic_prediction_id,
)
from shared.schemas.registry import EVENT_REGISTRY, UnknownEventTypeError, model_for

__all__ = [
    "EVENT_REGISTRY",
    "AnomalyEvent",
    "AnomalyType",
    "BarInterval",
    "BaseEvent",
    "DeadLetterEvent",
    "Direction",
    "EnrichedBarEvent",
    "FailureReason",
    "IndicatorSnapshot",
    "MarketBarEvent",
    "PredictionEvent",
    "PredictionTask",
    "Severity",
    "Symbol",
    "UnknownEventTypeError",
    "UtcDatetime",
    "derived_event_id",
    "deterministic_prediction_id",
    "model_for",
    "utcnow",
]

"""Turning a validated request into ``PredictionEvent`` records."""

from __future__ import annotations

import time
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

from inference.model import LoadedModel
from shared.schemas import (
    BarInterval,
    Direction,
    PredictionEvent,
    PredictionTask,
    deterministic_prediction_id,
)
from shared.schemas.inference import PredictInstance

SOURCE = "inference"


class InvalidRequestError(ValueError):
    """The request cannot be scored by this model (reported as HTTP 422)."""


def target_timestamp(as_of: datetime, interval: BarInterval, horizon: int) -> datetime:
    """Event time ``horizon`` bars after ``as_of``.

    Daily bars count **weekdays** (exchange holidays are not modelled, so a
    target can land on a holiday; it is resolved against the next available
    bar by the outcome job). Intraday bars use the fixed bar duration.
    """
    if interval is BarInterval.D1:
        day = np.busday_offset(np.datetime64(as_of.date()), horizon, roll="forward")
        target_date = pd.Timestamp(day).date()
        return as_of + timedelta(days=(target_date - as_of.date()).days)
    return as_of + horizon * interval.duration


def validate(instances: list[PredictInstance], model: LoadedModel) -> None:
    wrong = {i.feature_set_version for i in instances} - {model.feature_set_version}
    if wrong:
        raise InvalidRequestError(
            f"feature_set_version {', '.join(sorted(wrong))} does not match the served model "
            f"({model.feature_set_version})"
        )
    for index, instance in enumerate(instances):
        missing = set(model.feature_names) - instance.features.keys()
        if missing:
            raise InvalidRequestError(
                f"instance {index} is missing features: {', '.join(sorted(missing))}"
            )


def predict(
    instances: list[PredictInstance], model: LoadedModel, *, trace_id: str | None = None
) -> tuple[list[PredictionEvent], float]:
    """Score a batch. Returns the events and the model-call latency in seconds."""
    validate(instances, model)
    started = time.perf_counter()
    names = model.feature_names
    matrix = np.array([[i.features[n] for n in names] for i in instances], dtype=np.float64)
    p_up = model.predict_up(matrix)
    elapsed = time.perf_counter() - started
    latency_ms = elapsed * 1_000

    events = []
    for instance, p in zip(instances, p_up, strict=True):
        prediction_id = deterministic_prediction_id(
            instance.source_event_id, model.name, model.version, model.horizon_bars
        )
        up = bool(p >= 0.5)
        events.append(
            PredictionEvent(
                event_id=prediction_id,  # same input + model -> same event: idempotent
                source=SOURCE,
                trace_id=trace_id,
                prediction_id=prediction_id,
                symbol=instance.symbol,
                timestamp=instance.timestamp,
                interval=instance.interval,
                horizon_bars=model.horizon_bars,
                target_timestamp=target_timestamp(
                    instance.timestamp, instance.interval, model.horizon_bars
                ),
                task=PredictionTask.DIRECTION,
                predicted_direction=Direction.UP if up else Direction.DOWN,
                class_probabilities={Direction.UP: float(p), Direction.DOWN: float(1 - p)},
                confidence=float(max(p, 1 - p)),
                model_name=model.name,
                model_version=model.version,
                feature_set_version=model.feature_set_version,
                inference_latency_ms=latency_ms,
                source_event_id=instance.source_event_id,
            )
        )
    return events, elapsed

from __future__ import annotations

import base64
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from pydantic import ValidationError

from shared.schemas import (
    EVENT_REGISTRY,
    AnomalyEvent,
    AnomalyType,
    BarInterval,
    DeadLetterEvent,
    Direction,
    FailureReason,
    PredictionEvent,
    PredictionTask,
    Severity,
    UnknownEventTypeError,
    deterministic_prediction_id,
    model_for,
)
from shared.schemas.dead_letter import MAX_ERROR_MESSAGE_LENGTH

T0 = datetime(2026, 1, 5, 14, 30, tzinfo=UTC)
SOURCE_EVENT = uuid.UUID("12345678-1234-5678-1234-567812345678")


def prediction_kwargs(**overrides: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "source": "inference",
        "prediction_id": deterministic_prediction_id(SOURCE_EVENT, "lgbm-direction", "3", 5),
        "symbol": "MSFT",
        "timestamp": T0,
        "interval": BarInterval.M1,
        "horizon_bars": 5,
        "target_timestamp": T0 + timedelta(minutes=5),
        "task": PredictionTask.DIRECTION,
        "predicted_direction": Direction.UP,
        "class_probabilities": {Direction.UP: 0.55, Direction.DOWN: 0.45},
        "confidence": 0.55,
        "model_name": "lgbm-direction",
        "model_version": "3",
        "feature_set_version": "fs-1",
        "source_event_id": SOURCE_EVENT,
    }
    return base | overrides


class TestPrediction:
    def test_valid_prediction(self) -> None:
        p = PredictionEvent(**prediction_kwargs())
        assert p.partition_key() == "MSFT"
        assert p.class_probabilities == {Direction.UP: 0.55, Direction.DOWN: 0.45}

    def test_prediction_id_is_deterministic(self) -> None:
        a = deterministic_prediction_id(SOURCE_EVENT, "m", "1", 5)
        assert a == deterministic_prediction_id(SOURCE_EVENT, "m", "1", 5)
        assert a != deterministic_prediction_id(SOURCE_EVENT, "m", "2", 5)
        assert a != deterministic_prediction_id(SOURCE_EVENT, "m", "1", 1)

    @pytest.mark.parametrize(
        ("overrides", "message"),
        [
            ({"target_timestamp": T0}, "target_timestamp"),
            ({"predicted_direction": None}, "predicted_direction"),
            ({"task": PredictionTask.RETURN}, "predicted_return"),
            ({"class_probabilities": {Direction.UP: 0.7, Direction.DOWN: 0.7}}, "sum to 1"),
            ({"class_probabilities": {Direction.UP: 1.2}}, "less than or equal to 1"),
            ({"confidence": 1.5}, "less than or equal to 1"),
            ({"horizon_bars": 0}, "greater than or equal to 1"),
        ],
    )
    def test_invalid_predictions(self, overrides: dict[str, Any], message: str) -> None:
        with pytest.raises(ValidationError, match=message):
            PredictionEvent(**prediction_kwargs(**overrides))

    def test_return_task(self) -> None:
        p = PredictionEvent(
            **prediction_kwargs(
                task=PredictionTask.RETURN,
                predicted_direction=None,
                class_probabilities=None,
                confidence=None,
                predicted_return=-0.0012,
            )
        )
        assert p.predicted_return == pytest.approx(-0.0012)


class TestAnomaly:
    def test_valid_anomaly(self) -> None:
        a = AnomalyEvent(
            source="stream-processor",
            symbol="TSLA",
            timestamp=T0,
            interval=BarInterval.M1,
            anomaly_type=AnomalyType.VOLUME_SPIKE,
            severity=Severity.HIGH,
            observed_value=5.2,
            expected_value=1.0,
            score=5.2,
            threshold=3.0,
            detector="volume_ratio",
            detector_version="1.0.0",
            source_event_id=SOURCE_EVENT,
        )
        assert a.partition_key() == "TSLA"
        assert a.anomaly_id != SOURCE_EVENT

    def test_unknown_anomaly_type_rejected(self) -> None:
        with pytest.raises(ValidationError):
            AnomalyEvent.model_validate(
                {
                    "source": "x",
                    "symbol": "TSLA",
                    "timestamp": T0,
                    "interval": "1m",
                    "anomaly_type": "vibes",
                    "severity": "low",
                    "observed_value": 1,
                    "score": 1,
                    "detector": "d",
                    "detector_version": "1",
                    "source_event_id": str(SOURCE_EVENT),
                }
            )


class TestDeadLetter:
    def test_wraps_non_utf8_payload_losslessly(self) -> None:
        garbage = b"\xff\xfe\x00not-json"
        dlq = DeadLetterEvent.from_failure(
            topic="market.raw",
            partition=3,
            offset=42,
            key=b"AAPL",
            value=garbage,
            reason=FailureReason.DESERIALIZATION,
            error=ValueError("bad bytes"),
            consumer_group="stream-processor",
            source="stream-processor",
        )
        assert dlq.original_value() == garbage
        assert base64.b64decode(dlq.original_value_b64) == garbage
        assert dlq.partition_key() == "AAPL"
        assert dlq.attempts == 1

    def test_missing_key_and_value(self) -> None:
        dlq = DeadLetterEvent.from_failure(
            topic="market.raw",
            partition=0,
            offset=0,
            key=None,
            value=None,
            reason=FailureReason.DESERIALIZATION,
            error="empty",
            consumer_group="g",
            source="s",
        )
        assert dlq.original_value() == b""
        assert dlq.partition_key() == "market.raw"

    def test_long_error_messages_are_truncated(self) -> None:
        dlq = DeadLetterEvent.from_failure(
            topic="t",
            partition=0,
            offset=0,
            key=None,
            value=b"x",
            reason=FailureReason.PROCESSING,
            error="e" * 10_000,
            consumer_group="g",
            source="s",
            attempts=5,
        )
        assert len(dlq.error_message) == MAX_ERROR_MESSAGE_LENGTH
        assert dlq.error_message.endswith("...")


class TestRegistry:
    def test_every_registered_model_round_trips_its_key(self) -> None:
        for key, model in EVENT_REGISTRY.items():
            assert model.type_key() == key
            assert model_for(*key) is model

    def test_event_types_are_unique(self) -> None:
        types = [event_type for event_type, _ in EVENT_REGISTRY]
        assert len(types) == len(set(types)), "two versions registered; update this test"

    def test_unknown_key(self) -> None:
        with pytest.raises(UnknownEventTypeError):
            model_for("market.bar", 99)

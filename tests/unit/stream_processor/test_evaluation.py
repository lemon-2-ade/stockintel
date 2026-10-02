from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

from shared.schemas import AnomalyEvent, AnomalyType, BarInterval, Severity
from stream_processor.evaluation import DetectionOutcome, parse_injected, score_bar


def anomaly(detector: str, kind: AnomalyType) -> AnomalyEvent:
    return AnomalyEvent(
        source="t",
        symbol="AAPL",
        timestamp=datetime(2026, 1, 1, tzinfo=UTC),
        interval=BarInterval.S1,
        anomaly_type=kind,
        severity=Severity.LOW,
        observed_value=1,
        score=1,
        detector=detector,
        detector_version="1",
        source_event_id=uuid4(),
    )


def test_parse_injected() -> None:
    assert parse_injected(None) == frozenset()
    assert parse_injected(b"price_spike,volume_spike") == {"price_spike", "volume_spike"}


def test_scoring() -> None:
    outcomes = dict(
        score_bar(
            frozenset({"price_drop"}),
            [
                anomaly("return_threshold", AnomalyType.PRICE_DROP),
                anomaly("volume_median", AnomalyType.VOLUME_SPIKE),
            ],
        )
    )
    # The volume detection on a price-jump bar is ambiguous, hence not scored.
    assert outcomes == {
        "return_threshold": DetectionOutcome.TRUE_POSITIVE,
        "return_zscore": DetectionOutcome.FALSE_NEGATIVE,
    }
    assert score_bar(frozenset(), []) == []


def test_volume_false_positive_on_a_quiet_bar() -> None:
    outcomes = score_bar(frozenset(), [anomaly("volume_median", AnomalyType.VOLUME_SPIKE)])
    assert outcomes == [("volume_median", DetectionOutcome.FALSE_POSITIVE)]

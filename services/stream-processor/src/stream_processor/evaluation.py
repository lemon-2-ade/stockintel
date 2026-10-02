"""Score detections against simulator ground truth (``x-sim-injected-anomaly``).

Each bar is a trial per detector: TP if the injected anomaly was detected, FN
if it was missed, FP if the detector fired with nothing injected. Used both
live (Prometheus counters) and offline (``python -m stream_processor.evaluate``).
"""

from __future__ import annotations

from collections.abc import Iterable
from enum import StrEnum

from shared.schemas import AnomalyEvent

INJECTED_HEADER = "x-sim-injected-anomaly"

# Which injected label each detector is responsible for finding.
DETECTOR_TRUTH: dict[str, frozenset[str]] = {
    "return_threshold": frozenset({"price_spike", "price_drop"}),
    "return_zscore": frozenset({"price_spike", "price_drop"}),
    "volume_median": frozenset({"volume_spike"}),
}


# Bars whose ground truth is ambiguous for a detector are not scored for it.
# The simulator deliberately raises volume (x2-4, plus move-size coupling) on
# price-jump bars, so a volume detection there is neither clearly right nor wrong.
DETECTOR_IGNORES: dict[str, frozenset[str]] = {
    "volume_median": frozenset({"price_spike", "price_drop"}),
}


class DetectionOutcome(StrEnum):
    TRUE_POSITIVE = "tp"
    FALSE_POSITIVE = "fp"
    FALSE_NEGATIVE = "fn"


def parse_injected(header: bytes | str | None) -> frozenset[str]:
    if not header:
        return frozenset()
    text = header.decode() if isinstance(header, bytes) else header
    return frozenset(part for part in text.split(",") if part)


def score_bar(
    injected: frozenset[str], anomalies: Iterable[AnomalyEvent]
) -> list[tuple[str, DetectionOutcome]]:
    """Outcomes per detector for one bar (true negatives are not reported)."""
    fired = {a.detector for a in anomalies}
    outcomes: list[tuple[str, DetectionOutcome]] = []
    for detector, truth in DETECTOR_TRUTH.items():
        expected = bool(injected & truth)
        if not expected and injected & DETECTOR_IGNORES.get(detector, frozenset()):
            continue
        detected = detector in fired
        if expected and detected:
            outcomes.append((detector, DetectionOutcome.TRUE_POSITIVE))
        elif expected:
            outcomes.append((detector, DetectionOutcome.FALSE_NEGATIVE))
        elif detected:
            outcomes.append((detector, DetectionOutcome.FALSE_POSITIVE))
    return outcomes

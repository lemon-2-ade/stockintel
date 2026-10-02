"""Rule-based, statistical anomaly detectors (one set per symbol).

Every detector compares the current bar against a baseline built **only from
earlier bars**: a spike must never be part of the yardstick it is measured
against. Baselines are also protected from contamination afterwards:

* the return z-score window stores flagged returns *clipped* to the threshold
  (winsorised), so one jump does not inflate the std-dev and mask the next;
* the volume baseline is a rolling *median*, which a few spikes cannot move.

Detectors (version ``DETECTOR_VERSION``):

=====================  =====================================  =================
detector               fires when                             anomaly type
=====================  =====================================  =================
``return_threshold``   ``|close / prev_close - 1| > T``       price_spike / drop
``return_zscore``      ``|log r - mean| / std > Z`` (prior)   return_zscore
``volume_median``      ``volume / median(prior volumes) > V`` volume_spike
=====================  =====================================  =================
"""

from __future__ import annotations

import math
import statistics
from collections import deque
from dataclasses import dataclass, field
from typing import Any

from shared.schemas import AnomalyType, MarketBarEvent, Severity
from stream_processor.indicators import RollingWindow

DETECTOR_VERSION = "1.0.0"


@dataclass(frozen=True, slots=True)
class DetectorConfig:
    return_threshold: float = 0.01
    zscore_threshold: float = 5.0
    zscore_window: int = 120
    zscore_min_samples: int = 30
    volume_ratio_threshold: float = 6.0
    volume_window: int = 60
    volume_min_samples: int = 20

    def __post_init__(self) -> None:
        if self.return_threshold <= 0 or self.zscore_threshold <= 0:
            raise ValueError("thresholds must be > 0")
        if self.volume_ratio_threshold <= 1:
            raise ValueError("volume_ratio_threshold must be > 1")
        if not 2 <= self.zscore_min_samples <= self.zscore_window:
            raise ValueError("require 2 <= zscore_min_samples <= zscore_window")
        if not 1 <= self.volume_min_samples <= self.volume_window:
            raise ValueError("require 1 <= volume_min_samples <= volume_window")


@dataclass(frozen=True, slots=True)
class Detection:
    detector: str
    anomaly_type: AnomalyType
    severity: Severity
    observed: float
    expected: float | None
    score: float
    threshold: float
    details: dict[str, Any] = field(default_factory=dict)


def severity_for(score: float, threshold: float) -> Severity:
    """Map how far past its threshold a score is onto a severity band."""
    multiple = score / threshold
    if multiple < 1.5:
        return Severity.LOW
    if multiple < 2.5:
        return Severity.MEDIUM
    if multiple < 4.0:
        return Severity.HIGH
    return Severity.CRITICAL


class SymbolDetectors:
    def __init__(self, config: DetectorConfig | None = None) -> None:
        self.config = config or DetectorConfig()
        self._log_returns = RollingWindow(self.config.zscore_window)
        self._volumes: deque[float] = deque(maxlen=self.config.volume_window)

    def evaluate(self, bar: MarketBarEvent, prev_close: float | None) -> list[Detection]:
        """Score ``bar`` against prior baselines, then fold it into them."""
        cfg = self.config
        found: list[Detection] = []

        if prev_close is not None:
            simple = bar.close / prev_close - 1.0
            if abs(simple) > cfg.return_threshold:
                found.append(
                    Detection(
                        detector="return_threshold",
                        anomaly_type=AnomalyType.PRICE_SPIKE
                        if simple > 0
                        else AnomalyType.PRICE_DROP,
                        severity=severity_for(abs(simple), cfg.return_threshold),
                        observed=simple,
                        expected=0.0,
                        score=abs(simple),
                        threshold=cfg.return_threshold,
                        details={"prev_close": prev_close},
                    )
                )

            log_ret = math.log(bar.close / prev_close)
            to_store = log_ret
            mean, std = self._log_returns.mean(), self._log_returns.std(ddof=1)
            if (
                len(self._log_returns) >= cfg.zscore_min_samples
                and mean is not None
                and std is not None
                and std > 0
            ):
                z = (log_ret - mean) / std
                if abs(z) > cfg.zscore_threshold:
                    found.append(
                        Detection(
                            detector="return_zscore",
                            anomaly_type=AnomalyType.RETURN_ZSCORE,
                            severity=severity_for(abs(z), cfg.zscore_threshold),
                            observed=log_ret,
                            expected=mean,
                            score=abs(z),
                            threshold=cfg.zscore_threshold,
                            details={"z": z, "baseline_std": std, "window": len(self._log_returns)},
                        )
                    )
                    # Winsorise: keep the outlier from inflating future baselines.
                    to_store = mean + math.copysign(cfg.zscore_threshold * std, z)
            self._log_returns.push(to_store)

        if len(self._volumes) >= cfg.volume_min_samples:
            baseline = statistics.median(self._volumes)
            if baseline > 0:
                ratio = bar.volume / baseline
                if ratio > cfg.volume_ratio_threshold:
                    found.append(
                        Detection(
                            detector="volume_median",
                            anomaly_type=AnomalyType.VOLUME_SPIKE,
                            severity=severity_for(ratio, cfg.volume_ratio_threshold),
                            observed=float(bar.volume),
                            expected=baseline,
                            score=ratio,
                            threshold=cfg.volume_ratio_threshold,
                            details={"window": len(self._volumes)},
                        )
                    )
        self._volumes.append(float(bar.volume))
        return found

"""Monitoring checks over a window of served predictions (pure functions).

For each served model version in the window:

* **data drift**: PSI and KS of every input feature against the training
  reference profile stored with the model;
* **prediction drift**: PSI of P(up) against the model's output on recent
  training data;
* **performance**: on predictions whose outcome is known, accuracy, the live
  base rate, log loss, and the log loss of the null forecast (the training
  base rate as a constant probability). A model is only useful while it beats
  that null;
* **retraining signal**: an ``operational`` report recommending retraining
  when a large share of inputs drifted or performance fell below the null.
  It recommends; it never retrains or promotes anything itself.

Checks need a minimum sample (``min_samples`` inputs, ``min_outcomes``
outcomes) and report nothing below it: a PSI on 15 rows is noise.
"""

from __future__ import annotations

import math
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Final, Literal

import numpy as np

from shared.monitoring import PSI_ALERT, ReferenceProfile, ks, psi, psi_status
from shared.monitoring.drift import KS_ALERT_P

ReportType = Literal["data_drift", "prediction_drift", "performance", "operational"]
Status = Literal["ok", "warning", "alert"]

LOG_LOSS_ALERT_MARGIN: Final = 0.01
"""Alert when live log loss exceeds the null forecast's by more than this."""
RETRAIN_DRIFT_SHARE: Final = 0.3
"""Recommend retraining when at least this share of inputs is in PSI alert."""


@dataclass(frozen=True, slots=True)
class ServedRow:
    model_name: str
    model_version: str
    features: dict[str, float] | None
    p_up: float | None
    actual_direction: str | None
    """'up' / 'down' / 'neutral' once resolved, else None."""


@dataclass(frozen=True, slots=True)
class Report:
    report_type: ReportType
    model_name: str
    model_version: str
    metric_name: str
    value: float
    status: Status
    feature_name: str | None = None
    threshold: float | None = None
    details: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class CheckConfig:
    min_samples: int = 200
    min_outcomes: int = 100


def _finite(values: list[float | None]) -> np.ndarray:
    array = np.array([v for v in values if v is not None], dtype=float)
    return array[np.isfinite(array)]


def drift_reports(
    rows: list[ServedRow], reference: ReferenceProfile, cfg: CheckConfig
) -> list[Report]:
    name, version = rows[0].model_name, rows[0].model_version
    reports: list[Report] = []
    with_features = [r.features for r in rows if r.features]
    if len(with_features) >= cfg.min_samples:
        for feature, dist in reference.features.items():
            values = _finite([f.get(feature) for f in with_features])
            if values.size < cfg.min_samples:
                continue
            value = psi(dist, values)
            statistic, p_value = ks(dist, values)
            reports.append(
                Report(
                    "data_drift",
                    name,
                    version,
                    "psi",
                    value,
                    psi_status(value),
                    feature_name=feature,
                    threshold=PSI_ALERT,
                    details={
                        "n": int(values.size),
                        "ks_statistic": statistic,
                        "ks_p_value": p_value,
                        "ks_reject": bool(p_value < KS_ALERT_P),
                        "live_median": float(np.median(values)),
                        "reference_median": dist.quantiles[50],
                    },
                )
            )
    p_up = _finite([r.p_up for r in rows])
    if p_up.size >= cfg.min_samples:
        value = psi(reference.prediction, p_up)
        reports.append(
            Report(
                "prediction_drift",
                name,
                version,
                "psi",
                value,
                psi_status(value),
                threshold=PSI_ALERT,
                details={
                    "n": int(p_up.size),
                    "live_mean_p_up": float(p_up.mean()),
                    "reference_median_p_up": reference.prediction.quantiles[50],
                },
            )
        )
    return reports


def performance_reports(rows: list[ServedRow], base_rate: float, cfg: CheckConfig) -> list[Report]:
    resolved = [r for r in rows if r.actual_direction in ("up", "down") and r.p_up is not None]
    if len(resolved) < cfg.min_outcomes:
        return []
    name, version = rows[0].model_name, rows[0].model_version
    y = np.array([r.actual_direction == "up" for r in resolved], dtype=float)
    p = np.clip(np.array([r.p_up for r in resolved], dtype=float), 1e-6, 1 - 1e-6)
    q = min(max(base_rate, 1e-6), 1 - 1e-6)
    log_loss = float(-np.mean(y * np.log(p) + (1 - y) * np.log(1 - p)))
    null_log_loss = float(-np.mean(y * math.log(q) + (1 - y) * math.log(1 - q)))
    accuracy = float(np.mean((p >= 0.5) == (y == 1)))
    live_base = float(max(y.mean(), 1 - y.mean()))
    gap = log_loss - null_log_loss
    status: Status = "alert" if gap > LOG_LOSS_ALERT_MARGIN else "ok"
    details = {
        "n": len(resolved),
        "null_log_loss": null_log_loss,
        "training_base_rate": base_rate,
        "accuracy": accuracy,
        "live_majority_rate": live_base,
    }
    return [
        Report(
            "performance",
            name,
            version,
            "log_loss_minus_null",
            gap,
            status,
            threshold=LOG_LOSS_ALERT_MARGIN,
            details=details,
        ),
        Report(
            "performance",
            name,
            version,
            "accuracy_minus_majority",
            accuracy - live_base,
            "ok",
            details={"n": len(resolved), "accuracy": accuracy, "live_majority_rate": live_base},
        ),
    ]


def retrain_signal(reports: list[Report], name: str, version: str) -> Report | None:
    drift = [r for r in reports if r.report_type == "data_drift"]
    alerts = [r for r in drift if r.status == "alert"]
    share = len(alerts) / len(drift) if drift else 0.0
    perf_alert = any(r.report_type == "performance" and r.status == "alert" for r in reports)
    if share < RETRAIN_DRIFT_SHARE and not perf_alert:
        return None
    reasons = []
    if share >= RETRAIN_DRIFT_SHARE:
        reasons.append(f"{len(alerts)}/{len(drift)} inputs in PSI alert")
    if perf_alert:
        reasons.append("live log loss worse than the base-rate forecast")
    return Report(
        "operational",
        name,
        version,
        "retrain_recommended",
        1.0,
        "warning",
        threshold=RETRAIN_DRIFT_SHARE,
        details={
            "reasons": reasons,
            "drifted_features": sorted(r.feature_name or "" for r in alerts),
            "action": "run `make retrain`; the result is a candidate that still needs promotion",
        },
    )


def run_checks(
    rows: list[ServedRow],
    references: dict[tuple[str, str], ReferenceProfile | None],
    cfg: CheckConfig,
) -> list[Report]:
    """All reports for a window, grouped by served model version."""
    by_model: dict[tuple[str, str], list[ServedRow]] = defaultdict(list)
    for row in rows:
        by_model[(row.model_name, row.model_version)].append(row)
    reports: list[Report] = []
    for (name, version), group in sorted(by_model.items()):
        reference = references.get((name, version))
        model_reports: list[Report] = []
        if reference is None:
            model_reports.append(
                Report(
                    "operational",
                    name,
                    version,
                    "reference_profile_missing",
                    1.0,
                    "warning",
                    details={"effect": "drift checks skipped for this version"},
                )
            )
            base_rate = 0.5
        else:
            model_reports += drift_reports(group, reference, cfg)
            base_rate = reference.base_rate
        model_reports += performance_reports(group, base_rate, cfg)
        signal = retrain_signal(model_reports, name, version)
        if signal is not None:
            model_reports.append(signal)
        reports += model_reports
    return reports

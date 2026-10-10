"""Drift statistics and monitoring checks, including synthetic drift detection."""

from __future__ import annotations

import numpy as np
import pytest

from model_monitor.checks import CheckConfig, Report, ServedRow, run_checks
from model_monitor.main import MonitorMetrics
from shared.monitoring import Distribution, ReferenceProfile, ks, profile, psi, psi_status
from shared.monitoring.drift import _kolmogorov_sf

FEATURES = ("ret_5", "vol_10", "rsi_14")
RNG = np.random.default_rng(42)


def reference() -> ReferenceProfile:
    train = {
        "ret_5": RNG.normal(0.002, 0.03, 20_000),
        "vol_10": RNG.lognormal(-4.2, 0.4, 20_000),
        "rsi_14": RNG.beta(5, 5, 20_000),
    }
    return ReferenceProfile(
        feature_set_version="fs-test",
        features={k: profile(v) for k, v in train.items()},
        prediction=profile(RNG.normal(0.55, 0.02, 5_000)),
        base_rate=0.55,
    )


REF = reference()


def served(
    n: int,
    *,
    shift: float = 0.0,
    vol_scale: float = 1.0,
    p_up: float | None = None,
    outcomes: str | None = None,
    seed: int = 0,
) -> list[ServedRow]:
    rng = np.random.default_rng(seed)
    rows = []
    for _ in range(n):
        features = {
            "ret_5": float(rng.normal(0.002 + shift, 0.03)),
            "vol_10": float(rng.lognormal(-4.2, 0.4) * vol_scale),
            "rsi_14": float(rng.beta(5, 5)),
        }
        prob = p_up if p_up is not None else float(rng.normal(0.55, 0.02))
        actual = None
        if outcomes == "random":
            actual = "up" if rng.random() < 0.55 else "down"
        elif outcomes == "opposite":  # confidently wrong
            actual = "down" if prob >= 0.5 else "up"
        rows.append(ServedRow("m", "1", features, prob, actual))
    return rows


class TestStatistics:
    def test_psi_is_near_zero_for_the_same_distribution(self) -> None:
        value = psi(REF.features["ret_5"], RNG.normal(0.002, 0.03, 5_000))
        assert value < 0.02
        assert psi_status(value) == "ok"

    def test_psi_grows_with_the_shift(self) -> None:
        small = psi(REF.features["ret_5"], RNG.normal(0.012, 0.03, 5_000))
        large = psi(REF.features["ret_5"], RNG.normal(0.05, 0.03, 5_000))
        assert 0.02 < small < large
        assert psi_status(large) == "alert"

    def test_ks_p_value(self) -> None:
        _, p_same = ks(REF.features["rsi_14"], RNG.beta(5, 5, 2_000))
        stat, p_shift = ks(REF.features["rsi_14"], RNG.beta(6, 4, 2_000))
        assert p_same > 0.01
        assert p_shift < 1e-6
        assert 0 < stat <= 1

    def test_kolmogorov_tail_matches_known_critical_value(self) -> None:
        # The 5% critical value of the Kolmogorov distribution is 1.358.
        assert _kolmogorov_sf(1.358) == pytest.approx(0.05, abs=0.001)

    def test_discrete_inputs_and_round_trip(self) -> None:
        days = profile(RNG.integers(0, 5, 10_000).astype(float))
        assert len(days.edges) < 9, "repeated deciles are merged"
        assert psi(days, RNG.integers(0, 5, 2_000)) < 0.02
        assert Distribution.from_dict(days.to_dict()) == days
        assert ReferenceProfile.from_dict(REF.to_dict()) == REF


class TestChecks:
    cfg = CheckConfig(min_samples=200, min_outcomes=100)

    def reports(self, rows: list[ServedRow]) -> dict[tuple[str, str | None], Report]:
        out = run_checks(rows, {("m", "1"): REF}, self.cfg)
        return {(r.metric_name, r.feature_name): r for r in out}

    def test_no_drift_when_serving_matches_training(self) -> None:
        reports = self.reports(served(1_000))
        drift = [r for k, r in reports.items() if k[1] in FEATURES]
        assert len(drift) == 3
        assert all(r.status == "ok" for r in drift)
        assert ("retrain_recommended", None) not in reports

    def test_synthetic_drift_is_detected_and_recommends_retraining(self) -> None:
        reports = self.reports(served(1_000, shift=0.04, vol_scale=3.0))
        assert reports[("psi", "ret_5")].status == "alert"
        assert reports[("psi", "vol_10")].status == "alert"
        assert reports[("psi", "rsi_14")].status == "ok"
        signal = reports[("retrain_recommended", None)]
        assert signal.details["drifted_features"] == ["ret_5", "vol_10"]

    def test_prediction_drift(self) -> None:
        reports = self.reports(served(1_000, p_up=0.7))
        assert reports[("psi", None)].status == "alert"

    def test_performance_against_the_base_rate_forecast(self) -> None:
        fine = self.reports(served(1_000, outcomes="random"))
        assert fine[("log_loss_minus_null", None)].status == "ok"
        bad = self.reports(served(1_000, p_up=0.9, outcomes="opposite"))
        gap = bad[("log_loss_minus_null", None)]
        assert gap.status == "alert"
        assert gap.value > 1.0
        assert ("retrain_recommended", None) in bad

    def test_small_samples_report_nothing(self) -> None:
        assert run_checks(served(50, outcomes="random"), {("m", "1"): REF}, self.cfg) == []

    def test_missing_reference_is_reported_not_guessed(self) -> None:
        reports = run_checks(served(300), {("m", "1"): None}, self.cfg)
        assert [r.metric_name for r in reports] == ["reference_profile_missing"]

    def test_metrics_publish_status(self) -> None:
        metrics = MonitorMetrics()
        metrics.publish(run_checks(served(1_000, shift=0.04), {("m", "1"): REF}, self.cfg))
        value = metrics.registry.get_sample_value(
            "sip_monitor_status",
            {"report_type": "data_drift", "metric": "psi", "feature": "ret_5"},
        )
        assert value == 2
        alerts = metrics.registry.get_sample_value(
            "sip_monitor_alerts_total", {"report_type": "data_drift"}
        )
        assert alerts is not None
        assert alerts >= 1

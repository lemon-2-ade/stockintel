"""Gradient-boosted models, the backtest engine, and the Phase 7 runner."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest
from ml_fixtures import cleaned_frame

from stockml.evaluation.backtest import (
    BacktestConfig,
    backtest_all_offsets,
    run_backtest,
    summarize,
    target_weights,
)
from stockml.evaluation.metrics import classification_metrics
from stockml.features.dataset import build_dataset
from stockml.training.baselines import Logistic, RunConfig
from stockml.training.gbm import (
    GbmParams,
    LightGBMDirection,
    XGBoostDirection,
    early_stopping_split,
)
from stockml.training.models import (
    PROBABILISTIC,
    ModelsConfig,
    render_markdown,
    run_development,
    run_final_test,
)
from stockml.training.splits import SplitConfig, chronological_split

NAN = float("nan")


# --------------------------------------------------------------------------- weights


class TestTargetWeights:
    def test_buy_hold_is_equal_weight_over_available_symbols(self) -> None:
        w = target_weights(np.array([0.1, NAN, 0.3, 0.2]), "buy_hold", 2)
        np.testing.assert_allclose(w, [1 / 3, 0, 1 / 3, 1 / 3])

    def test_top_k_picks_the_highest_scores(self) -> None:
        w = target_weights(np.array([0.1, 0.9, NAN, 0.5, 0.7]), "top_k", 2)
        np.testing.assert_allclose(w, [0, 0.5, 0, 0, 0.5])

    def test_long_short_is_dollar_neutral_with_unit_gross(self) -> None:
        w = target_weights(np.array([0.1, 0.9, 0.5, 0.7, 0.3, 0.2]), "long_short", 2)
        assert w.sum() == pytest.approx(0)
        assert np.abs(w).sum() == pytest.approx(1)
        np.testing.assert_allclose(w, [-0.25, 0.25, 0, 0.25, 0, -0.25])

    def test_ties_break_by_symbol_order(self) -> None:
        w = target_weights(np.array([0.5, 0.5, 0.5]), "top_k", 1)
        np.testing.assert_allclose(w, [1, 0, 0])

    def test_k_is_capped_and_empty_dates_hold_cash(self) -> None:
        assert target_weights(np.array([0.2, 0.4, 0.6]), "long_short", 5).sum() == pytest.approx(0)
        assert not target_weights(np.array([NAN, NAN]), "top_k", 1).any()


# --------------------------------------------------------------------------- backtest


def predictions(rows: list[tuple[str, str, float, float]]) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"symbol": s, "session_date": pd.Timestamp(d), "score": sc, "y_return": np.log1p(r)}
            for s, d, sc, r in rows
        ]
    )


class TestBacktest:
    PREDS = predictions(
        [
            ("AAA", "2020-01-01", 0.9, 0.10),
            ("BBB", "2020-01-01", 0.1, -0.05),
            ("AAA", "2020-01-02", 0.2, -0.02),
            ("BBB", "2020-01-02", 0.8, 0.04),
        ]
    )

    def test_period_returns_turnover_and_costs_by_hand(self) -> None:
        cfg = BacktestConfig(horizon=1, cost_bps=10, k=1)
        periods = run_backtest(self.PREDS, "top_k", cfg)
        # Day 1: buy AAA from cash (turnover 1) and earn +10%.
        # Day 2: switch to BBB (sell 1, buy 1: turnover 2) and earn +4%.
        np.testing.assert_allclose(periods["gross"], [0.10, 0.04])
        np.testing.assert_allclose(periods["turnover"], [1.0, 2.0])
        np.testing.assert_allclose(periods["cost"], [0.001, 0.002])
        np.testing.assert_allclose(periods["net"], [0.099, 0.038])

    def test_long_short_earns_the_spread(self) -> None:
        periods = run_backtest(self.PREDS, "long_short", BacktestConfig(horizon=1, cost_bps=0, k=1))
        expected = [0.5 * 0.10 + 0.5 * 0.05, 0.5 * 0.04 + 0.5 * 0.02]
        np.testing.assert_allclose(periods["gross"], expected)

    def test_buy_hold_pays_cost_only_on_entry(self) -> None:
        periods = run_backtest(self.PREDS, "buy_hold", BacktestConfig(horizon=1, cost_bps=10))
        np.testing.assert_allclose(periods["turnover"], [1.0, 0.0])

    def test_rebalances_every_horizon_sessions_from_the_offset(self) -> None:
        dates = pd.bdate_range("2020-01-01", periods=12)
        preds = pd.DataFrame(
            {"symbol": "AAA", "session_date": dates, "score": 1.0, "y_return": 0.0}
        )
        cfg = BacktestConfig(horizon=5)
        assert list(run_backtest(preds, "top_k", cfg, offset=0)["session_date"]) == [
            dates[0],
            dates[5],
            dates[10],
        ]
        assert list(run_backtest(preds, "top_k", cfg, offset=3)["session_date"]) == [
            dates[3],
            dates[8],
        ]

    def test_summary_statistics(self) -> None:
        periods = pd.DataFrame(
            {"net": [0.1, -0.5, 0.2], "cost": [0.0] * 3, "turnover": [1.0] * 3, "exposure": 1.0}
        )
        s = summarize(periods, BacktestConfig(horizon=5, sessions_per_year=15))  # 3 periods/year
        assert s["ann_return"] == pytest.approx(1.1 * 0.5 * 1.2 - 1)
        assert s["max_drawdown"] == pytest.approx(-0.5)
        assert s["hit_rate"] == pytest.approx(2 / 3)
        net = np.array([0.1, -0.5, 0.2])
        assert s["sharpe"] == pytest.approx(net.mean() / net.std(ddof=1) * np.sqrt(3))

    def test_offsets_are_all_evaluated(self) -> None:
        dates = pd.bdate_range("2020-01-01", periods=40)
        rng = np.random.default_rng(1)
        preds = pd.DataFrame(
            {
                "symbol": np.repeat(["AAA", "BBB"], 40),
                "session_date": np.tile(dates, 2),
                "score": rng.random(80),
                "y_return": rng.normal(0, 0.02, 80),
            }
        )
        result = backtest_all_offsets(preds, "top_k", BacktestConfig(horizon=5, k=1))
        assert result["sharpe_min"] <= result["sharpe_median"] <= result["sharpe_max"]


# --------------------------------------------------------------------------- GBMs


@pytest.fixture(scope="module", name="dataset")
def dataset_fixture() -> pd.DataFrame:
    return build_dataset(cleaned_frame(1_400, ("AAA", "BBB", "CCC"), seed=3))


def nonlinear_signal(dataset: pd.DataFrame) -> pd.DataFrame:
    """XOR of two independent lagged returns' signs: invisible to a linear model."""
    interaction = np.sign(dataset["ret_lag_1"] * dataset["ret_lag_3"])
    rng = np.random.default_rng(0)
    planted = 0.02 * interaction + rng.normal(0, 0.01, len(dataset))
    return dataset.assign(y_return=planted, y_up=(planted > 0).astype(float))


def auc(model: object, train: pd.DataFrame, test: pd.DataFrame) -> float:
    fitted = model.fit(train)  # type: ignore[attr-defined]
    pred, score = fitted.predict(test)
    return float(
        classification_metrics(test["y_up"].to_numpy().astype(int), pred, score)["roc_auc"]
    )


def test_early_stopping_set_is_purged_and_inside_the_fold(dataset: pd.DataFrame) -> None:
    fold = dataset[dataset["label_end"] <= pd.Timestamp("2019-12-31")]
    inner, stopping = early_stopping_split(fold, years=1, embargo=5)
    boundary = pd.Timestamp("2018-12-31")
    assert inner["label_end"].max() <= boundary
    assert stopping["session_date"].min() > boundary
    assert stopping["label_end"].max() <= fold["session_date"].max()
    gap = stopping["session_date"].min() - inner["session_date"].max()
    assert gap >= pd.Timedelta(days=7), "purge + embargo leaves a gap of about two weeks"


@pytest.mark.parametrize("model_cls", [LightGBMDirection, XGBoostDirection])
def test_gbm_finds_a_nonlinear_signal_that_logistic_misses(
    dataset: pd.DataFrame, model_cls: type[LightGBMDirection] | type[XGBoostDirection]
) -> None:
    planted = nonlinear_signal(dataset)
    parts = chronological_split(planted, SplitConfig("2019-12-31", "2020-12-31", 5))
    train, validation = parts["train"], parts["validation"]
    assert auc(model_cls(), train, validation) > 0.8
    assert auc(Logistic(), train, validation) < 0.7


@pytest.mark.parametrize("model_cls", [LightGBMDirection, XGBoostDirection])
def test_gbm_training_is_deterministic(
    dataset: pd.DataFrame, model_cls: type[LightGBMDirection] | type[XGBoostDirection]
) -> None:
    train = dataset[dataset["label_end"] <= pd.Timestamp("2019-12-31")]
    test = dataset[dataset["session_date"] > pd.Timestamp("2020-01-10")]
    first = model_cls().fit(train).predict_proba(test)
    second = model_cls().fit(train).predict_proba(test)
    np.testing.assert_array_equal(first, second)


def test_runner_end_to_end_on_synthetic_data(dataset: pd.DataFrame) -> None:
    cfg = ModelsConfig(
        run=RunConfig(
            splits=SplitConfig("2018-12-31", "2019-12-31", 5),
            walk_forward_first_year=2018,
            walk_forward_last_year=2019,
        ),
        tuning_years=(2018, 2018),
        untuned_years=(2019, 2019),
        grid=(GbmParams(max_rounds=50), GbmParams(num_leaves=7, max_depth=3, max_rounds=50)),
    )
    report = run_development(dataset, cfg)
    assert report["candidate"]["name"] in PROBABILISTIC
    assert set(report["walk_forward"]["xgboost"]) == {"2018", "2019"}
    assert len(report["tuning"]["lightgbm"]) == 2
    assert "buy_hold" in report["backtest_walk_forward"]["strategies"]
    assert "test" not in report, "the test period is only touched on request"
    run_final_test(dataset, cfg, report)
    assert report["test"]["period"][0] > "2019-12-31"
    markdown = render_markdown(report)
    assert "## Held-out test (" in markdown

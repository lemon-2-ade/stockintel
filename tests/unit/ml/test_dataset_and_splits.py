"""Labels, purged/embargoed splits, metrics, and a planted-signal sanity check."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from shared.features import FEATURE_NAMES
from stockml.evaluation.metrics import accuracy_interval, classification_metrics, regression_metrics
from stockml.features.dataset import LabelConfig, add_labels, build_dataset, compute_features
from stockml.training.baselines import Logistic, Majority, RunConfig, run
from stockml.training.splits import SplitConfig, chronological_split, walk_forward


def cleaned_frame(
    n: int = 400, symbols: tuple[str, ...] = ("AAA", "BBB"), seed: int = 0
) -> pd.DataFrame:
    """A cleaned.parquet-shaped frame of business-day bars."""
    rng = np.random.default_rng(seed)
    dates = pd.bdate_range("2016-01-04", periods=n)
    blocks = []
    for i, symbol in enumerate(symbols):
        close = 100 * np.exp(np.cumsum(rng.normal(0, 0.01, n)))
        blocks.append(
            pd.DataFrame(
                {
                    "symbol": symbol,
                    "session_date": dates,
                    "timestamp": (dates + pd.Timedelta(hours=14, minutes=30)).tz_localize("UTC"),
                    "open": close,
                    "high": close * 1.01,
                    "low": close * 0.99,
                    "close": close,
                    "volume": rng.integers(1_000, 2_000, n) + i,
                    "missing_sessions_before": 0,
                }
            )
        )
    return pd.concat(blocks, ignore_index=True)


class TestLabels:
    def test_forward_return_and_classes(self) -> None:
        cleaned = cleaned_frame(120, ("AAA",))
        labelled = add_labels(compute_features(cleaned), cleaned, LabelConfig(horizon=5))
        close = labelled["close"]
        expected = np.log(close.shift(-5) / close)
        np.testing.assert_allclose(labelled["y_return"], expected)
        assert labelled["y_return"].tail(5).isna().all(), "no label past the end of data"
        up = labelled["y_return"] > 0
        assert (labelled.loc[up, "y_up"] == 1).all()
        assert (labelled.loc[labelled["y_return"] < 0, "y_up"] == 0).all()
        assert (
            labelled["label_end"].iloc[:-5] == labelled["session_date"].shift(-5).iloc[:-5]
        ).all()

    def test_neutral_band(self) -> None:
        cleaned = cleaned_frame(120, ("AAA",))
        labelled = add_labels(compute_features(cleaned), cleaned, LabelConfig(neutral_band=0.01))
        small = labelled["y_return"].abs() <= 0.01
        assert (labelled.loc[small, "y_class"] == 0).all()
        assert labelled.loc[small, "y_up"].isna().all()

    def test_label_window_never_crosses_a_gap(self) -> None:
        cleaned = cleaned_frame(120, ("AAA",))
        cleaned.loc[80, "missing_sessions_before"] = 2
        labelled = add_labels(compute_features(cleaned), cleaned, LabelConfig(horizon=5))
        assert labelled["y_return"].iloc[75:80].isna().all()
        assert pd.notna(labelled["y_return"].iloc[74])
        assert pd.notna(labelled["y_return"].iloc[80])

    def test_dataset_keeps_only_complete_rows(self) -> None:
        dataset = build_dataset(cleaned_frame(200))
        assert dataset[list(FEATURE_NAMES)].notna().all().all()
        assert dataset["y_return"].notna().all()
        assert len(dataset) == 2 * (200 - 60 - 5)
        assert dataset.attrs["feature_set_version"].startswith("fs-")


@pytest.fixture(scope="module", name="dataset")
def dataset_fixture() -> pd.DataFrame:
    return build_dataset(cleaned_frame(1_400))  # 2016 .. mid 2021


class TestSplits:
    def test_chronological_purged_and_embargoed(self, dataset: pd.DataFrame) -> None:
        cfg = SplitConfig(train_end="2018-12-31", validation_end="2019-12-31", embargo_sessions=5)
        parts = chronological_split(dataset, cfg)
        train, val, test = parts["train"], parts["validation"], parts["test"]
        assert train["label_end"].max() <= pd.Timestamp("2018-12-31"), "purged: no label crosses"
        assert val["label_end"].max() <= pd.Timestamp("2019-12-31")
        assert train["session_date"].max() < val["session_date"].min()
        assert val["session_date"].max() < test["session_date"].min()
        first_val_sessions = pd.bdate_range("2019-01-01", periods=6)
        assert val["session_date"].min() == first_val_sessions[5], "5-session embargo"
        assert set(train.index).isdisjoint(val.index)
        assert set(val.index).isdisjoint(test.index)

    def test_same_boundaries_for_every_symbol(self, dataset: pd.DataFrame) -> None:
        val = chronological_split(dataset, SplitConfig("2018-12-31", "2019-12-31", 5))["validation"]
        bounds = val.groupby("symbol")["session_date"].agg(["min", "max"])
        assert bounds["min"].nunique() == 1
        assert bounds["max"].nunique() == 1

    def test_walk_forward_expands_and_never_overlaps(self, dataset: pd.DataFrame) -> None:
        folds = list(walk_forward(dataset, first_test_year=2018, last_test_year=2020))
        assert [f.name for f in folds] == ["2018", "2019", "2020"]
        sizes = [len(f.train) for f in folds]
        assert sizes == sorted(sizes), "expanding window"
        for f in folds:
            assert f.train["label_end"].max() < f.test["session_date"].min()
            assert f.test["session_date"].dt.year.unique().tolist() == [int(f.name)]


class TestMetrics:
    def test_accuracy_interval_uses_effective_sample_size(self) -> None:
        narrow = accuracy_interval(0.55, 10_000, horizon=1)
        wide = accuracy_interval(0.55, 10_000, horizon=5)
        assert (wide[1] - wide[0]) == pytest.approx((narrow[1] - narrow[0]) * np.sqrt(5))

    def test_classification_metrics_report_base_rate(self) -> None:
        y = np.array([1, 1, 1, 0])
        m = classification_metrics(y, np.array([1, 1, 1, 1]), horizon=1)
        assert m["base_rate"] == 0.75
        assert m["accuracy_minus_base_rate"] == 0
        assert m["confusion_matrix"] == [[0, 1], [0, 3]]

    def test_directional_accuracy(self) -> None:
        m = regression_metrics(np.array([0.1, -0.2, 0.3, 0.0]), np.array([0.2, 0.1, 0.1, 0.5]))
        assert m["directional_accuracy"] == pytest.approx(2 / 3)


def test_pipeline_finds_a_planted_signal() -> None:
    """If the next week's return is partly predictable, the logistic baseline must find it.

    Guards against a pipeline bug (misaligned labels, wrong split) that would make
    every model look like noise on the real data.
    """
    cleaned = cleaned_frame(1_400, ("AAA", "BBB", "CCC"), seed=3)
    dataset = build_dataset(cleaned)
    rng = np.random.default_rng(0)
    planted = 0.02 * np.sign(dataset["rsi_14"] - 0.5) + rng.normal(0, 0.01, len(dataset))
    dataset = dataset.assign(y_return=planted, y_up=(planted > 0).astype(float))
    cfg = RunConfig(
        splits=SplitConfig("2018-12-31", "2019-12-31", 5),
        walk_forward_first_year=2018,
        walk_forward_last_year=2019,
    )
    report = run(dataset, cfg)
    logistic = report["validation"]["direction"]["logistic"]
    majority = report["validation"]["direction"]["majority"]
    assert logistic["roc_auc"] > 0.85
    assert logistic["accuracy"] > majority["accuracy"] + 0.2
    assert report["validation"]["return"]["ridge"]["r2"] > 0.3


def test_models_never_see_test_labels() -> None:
    dataset = build_dataset(cleaned_frame(600))
    train = dataset.iloc[:400].dropna(subset=["y_up"])
    model = Majority().fit(train)
    assert model.p_up == pytest.approx(train["y_up"].mean())
    logistic = Logistic().fit(train)
    assert set(logistic.coefficients()) == set(FEATURE_NAMES)

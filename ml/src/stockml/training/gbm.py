"""Gradient-boosted direction models (LightGBM, XGBoost).

Both wrappers follow the baseline ``DirectionModel`` protocol: ``fit(frame)``
on a training fold, ``predict(frame)`` -> (class, P(up)).

Early stopping needs data the model has not been fitted on, and that data must
not be the evaluation fold (choosing the number of trees on the test year is
tuning on the test year). So each ``fit`` carves an **inner** split from its
own training fold: the last ``early_stopping_years`` become the stopping set,
with the same purge + embargo as every other boundary. The final model keeps
the trees chosen there; it is not refitted on the stopping set, which costs a
year of data but keeps the procedure simple and leak-free.

Reproducibility: fixed seeds, deterministic LightGBM, one thread. One thread is
slower but gives identical trees on any machine (multi-threaded histogram
construction can differ in the last bits of floating-point sums).
"""

from __future__ import annotations

from dataclasses import asdict, dataclass

import lightgbm as lgb
import numpy as np
import pandas as pd
import xgboost as xgb

from shared.features import FEATURE_NAMES
from stockml.training.splits import purge_and_embargo

FEATURES = list(FEATURE_NAMES)
SEED = 7


@dataclass(frozen=True, slots=True)
class GbmParams:
    """Shared hyperparameters, mapped to each library's names."""

    learning_rate: float = 0.03
    num_leaves: int = 15
    max_depth: int = 4
    min_child_samples: int = 200
    subsample: float = 0.8
    colsample: float = 0.8
    reg_lambda: float = 10.0
    max_rounds: int = 2_000
    early_stopping_rounds: int = 100

    def label(self) -> str:
        return (
            f"lr={self.learning_rate} leaves={self.num_leaves} depth={self.max_depth} "
            f"min_child={self.min_child_samples} lambda={self.reg_lambda} "
            f"subsample={self.subsample} colsample={self.colsample}"
        )


PARAM_GRID: tuple[GbmParams, ...] = (
    GbmParams(),
    GbmParams(num_leaves=7, max_depth=3),
    GbmParams(num_leaves=31, max_depth=6, min_child_samples=100),
    GbmParams(min_child_samples=500, reg_lambda=50.0),
    GbmParams(learning_rate=0.01, num_leaves=7, max_depth=3, min_child_samples=500),
    GbmParams(subsample=0.5, colsample=0.5),
)
"""Small, deliberately regularised grid: with ~30k noisy rows the risk is
overfitting, not underfitting. Kept small so that selection itself does not
become a source of overfitting."""


def early_stopping_split(
    frame: pd.DataFrame, years: int = 1, embargo: int = 5
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """(inner train, stopping set): the last ``years`` of the fold, purged + embargoed."""
    end = frame["session_date"].max()
    boundary = pd.Timestamp(year=end.year - years, month=12, day=31)
    if boundary >= end or boundary <= frame["session_date"].min():
        boundary = end - pd.DateOffset(years=years)
    inner_train, stopping = purge_and_embargo(frame, boundary, None, embargo)
    return inner_train, stopping[stopping["label_end"] <= end]


def _xy(frame: pd.DataFrame) -> tuple[np.ndarray, np.ndarray]:
    return frame[FEATURES].to_numpy(dtype=float), frame["y_up"].to_numpy(dtype=int)


class LightGBMDirection:
    name = "lightgbm"
    probabilistic = True

    def __init__(self, params: GbmParams | None = None, *, embargo: int = 5) -> None:
        self.params = params or GbmParams()
        self.embargo = embargo
        self.booster: lgb.Booster | None = None
        self.best_iteration = 0

    def _lgb_params(self) -> dict[str, object]:
        p = self.params
        return {
            "objective": "binary",
            "metric": "binary_logloss",
            "learning_rate": p.learning_rate,
            "num_leaves": p.num_leaves,
            "max_depth": p.max_depth,
            "min_child_samples": p.min_child_samples,
            "bagging_fraction": p.subsample,
            "bagging_freq": 1,
            "feature_fraction": p.colsample,
            "lambda_l2": p.reg_lambda,
            "seed": SEED,
            "deterministic": True,
            "force_row_wise": True,
            "num_threads": 1,
            "verbosity": -1,
        }

    def fit(self, frame: pd.DataFrame) -> LightGBMDirection:
        frame = frame.dropna(subset=["y_up"])  # exact-zero returns have no direction
        inner, stopping = early_stopping_split(frame, embargo=self.embargo)
        x_tr, y_tr = _xy(inner)
        x_es, y_es = _xy(stopping)
        train_set = lgb.Dataset(x_tr, y_tr, feature_name=FEATURES, free_raw_data=False)
        valid_set = lgb.Dataset(x_es, y_es, reference=train_set)
        self.booster = lgb.train(
            self._lgb_params(),
            train_set,
            num_boost_round=self.params.max_rounds,
            valid_sets=[valid_set],
            callbacks=[lgb.early_stopping(self.params.early_stopping_rounds, verbose=False)],
        )
        self.best_iteration = int(self.booster.best_iteration or self.booster.current_iteration())
        return self

    def predict_proba(self, frame: pd.DataFrame) -> np.ndarray:
        if self.booster is None:
            raise RuntimeError("model is not fitted")
        x = frame[FEATURES].to_numpy(dtype=float)
        return np.asarray(self.booster.predict(x, num_iteration=self.best_iteration), dtype=float)

    def predict(self, frame: pd.DataFrame) -> tuple[np.ndarray, np.ndarray]:
        proba = self.predict_proba(frame)
        return (proba >= 0.5).astype(int), proba

    def importance(self) -> dict[str, float]:
        """Share of total split gain per feature."""
        if self.booster is None:
            raise RuntimeError("model is not fitted")
        gain = self.booster.feature_importance(importance_type="gain")
        total = float(gain.sum()) or 1.0
        return {f: float(g) / total for f, g in zip(FEATURES, gain, strict=True)}

    def describe(self) -> dict[str, object]:
        return {**asdict(self.params), "best_iteration": self.best_iteration}


class XGBoostDirection:
    name = "xgboost"
    probabilistic = True

    def __init__(self, params: GbmParams | None = None, *, embargo: int = 5) -> None:
        self.params = params or GbmParams()
        self.embargo = embargo
        self.booster: xgb.Booster | None = None
        self.best_iteration = 0

    def _xgb_params(self) -> dict[str, object]:
        p = self.params
        return {
            "objective": "binary:logistic",
            "eval_metric": "logloss",
            "eta": p.learning_rate,
            "max_depth": p.max_depth,
            "max_leaves": p.num_leaves,
            "min_child_weight": p.min_child_samples * 0.25,  # hessian of p(1-p) ~ 0.25
            "subsample": p.subsample,
            "colsample_bytree": p.colsample,
            "lambda": p.reg_lambda,
            "tree_method": "hist",
            "seed": SEED,
            "nthread": 1,
            "verbosity": 0,
        }

    def fit(self, frame: pd.DataFrame) -> XGBoostDirection:
        frame = frame.dropna(subset=["y_up"])  # exact-zero returns have no direction
        inner, stopping = early_stopping_split(frame, embargo=self.embargo)
        x_tr, y_tr = _xy(inner)
        x_es, y_es = _xy(stopping)
        d_train = xgb.DMatrix(x_tr, label=y_tr, feature_names=FEATURES)
        d_stop = xgb.DMatrix(x_es, label=y_es, feature_names=FEATURES)
        self.booster = xgb.train(
            self._xgb_params(),
            d_train,
            num_boost_round=self.params.max_rounds,
            evals=[(d_stop, "stopping")],
            early_stopping_rounds=self.params.early_stopping_rounds,
            verbose_eval=False,
        )
        self.best_iteration = int(self.booster.best_iteration) + 1
        return self

    def predict_proba(self, frame: pd.DataFrame) -> np.ndarray:
        if self.booster is None:
            raise RuntimeError("model is not fitted")
        d = xgb.DMatrix(frame[FEATURES].to_numpy(dtype=float), feature_names=FEATURES)
        return np.asarray(
            self.booster.predict(d, iteration_range=(0, self.best_iteration)), dtype=float
        )

    def predict(self, frame: pd.DataFrame) -> tuple[np.ndarray, np.ndarray]:
        proba = self.predict_proba(frame)
        return (proba >= 0.5).astype(int), proba

    def describe(self) -> dict[str, object]:
        return {**asdict(self.params), "best_iteration": self.best_iteration}

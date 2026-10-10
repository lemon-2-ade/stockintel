"""Baseline models: the bar every later model has to clear.

    python -m stockml.training.baselines     # or: make baselines

Primary task: **direction of the next 5-session (one week) log return**,
binary up/down. Secondary task: the return itself (regression).

Direction baselines
    majority     always predict the training set's majority class (the base rate)
    momentum     predict that the last 5 sessions' direction continues
    logistic     L2 logistic regression on standardised features

Return baselines
    zero         predict 0 (random-walk / efficient-market null)
    train_mean   predict the training-period mean return
    ridge        ridge regression on standardised features

Everything is fitted on the training period only (scalers included, via
sklearn pipelines), evaluated on validation and in walk-forward folds. The
held-out test period is **not** used here.
"""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Protocol

import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression, Ridge
from sklearn.pipeline import Pipeline, make_pipeline
from sklearn.preprocessing import StandardScaler

from shared.config import LogSettings
from shared.features import FEATURE_NAMES, FEATURE_SET_VERSION
from shared.observability.logs import configure_logging, get_logger
from stockml.data.catalog import REPO_ROOT, DatasetSpec
from stockml.evaluation.metrics import Metrics, classification_metrics, regression_metrics
from stockml.features.dataset import LabelConfig, build_dataset
from stockml.training.splits import SplitConfig, chronological_split, walk_forward

log = get_logger(__name__)

FEATURES = list(FEATURE_NAMES)
DEFAULT_REPORT_MD = REPO_ROOT / "docs" / "BASELINES.md"
SEED = 7


class DirectionModel(Protocol):
    name: str

    def fit(self, frame: pd.DataFrame) -> DirectionModel: ...

    def predict(self, frame: pd.DataFrame) -> tuple[np.ndarray, np.ndarray]:
        """(predicted class 0/1, score where higher = more likely up)."""
        ...


class ReturnModel(Protocol):
    name: str

    def fit(self, frame: pd.DataFrame) -> ReturnModel: ...

    def predict(self, frame: pd.DataFrame) -> np.ndarray: ...


class Majority:
    name = "majority"

    def __init__(self) -> None:
        self.p_up = 0.5

    def fit(self, frame: pd.DataFrame) -> Majority:
        self.p_up = float(frame["y_up"].mean())
        return self

    def predict(self, frame: pd.DataFrame) -> tuple[np.ndarray, np.ndarray]:
        n = len(frame)
        return np.full(n, int(self.p_up >= 0.5)), np.full(n, self.p_up)


class Momentum:
    name = "momentum"

    def fit(self, frame: pd.DataFrame) -> Momentum:
        return self

    def predict(self, frame: pd.DataFrame) -> tuple[np.ndarray, np.ndarray]:
        score = frame["ret_5"].to_numpy()
        return (score > 0).astype(int), score


class Logistic:
    name = "logistic"
    probabilistic = True

    def __init__(self, c: float = 0.1) -> None:
        self.pipeline: Pipeline = make_pipeline(
            StandardScaler(), LogisticRegression(C=c, max_iter=2_000, random_state=SEED)
        )

    def fit(self, frame: pd.DataFrame) -> Logistic:
        self.pipeline.fit(frame[FEATURES], frame["y_up"].astype(int))
        return self

    def predict(self, frame: pd.DataFrame) -> tuple[np.ndarray, np.ndarray]:
        score = self.pipeline.predict_proba(frame[FEATURES])[:, 1]
        return (score >= 0.5).astype(int), score

    def coefficients(self) -> dict[str, float]:
        model: LogisticRegression = self.pipeline[-1]
        return dict(zip(FEATURES, model.coef_[0].round(4).tolist(), strict=True))


class ZeroReturn:
    name = "zero"

    def fit(self, frame: pd.DataFrame) -> ZeroReturn:
        return self

    def predict(self, frame: pd.DataFrame) -> np.ndarray:
        return np.zeros(len(frame))


class TrainMean:
    name = "train_mean"

    def __init__(self) -> None:
        self.mean = 0.0

    def fit(self, frame: pd.DataFrame) -> TrainMean:
        self.mean = float(frame["y_return"].mean())
        return self

    def predict(self, frame: pd.DataFrame) -> np.ndarray:
        return np.full(len(frame), self.mean)


class RidgeReturn:
    name = "ridge"

    def __init__(self, alpha: float = 10.0) -> None:
        self.pipeline: Pipeline = make_pipeline(StandardScaler(), Ridge(alpha=alpha))

    def fit(self, frame: pd.DataFrame) -> RidgeReturn:
        self.pipeline.fit(frame[FEATURES], frame["y_return"])
        return self

    def predict(self, frame: pd.DataFrame) -> np.ndarray:
        return np.asarray(self.pipeline.predict(frame[FEATURES]))


def direction_models() -> list[DirectionModel]:
    return [Majority(), Momentum(), Logistic()]


def return_models() -> list[ReturnModel]:
    return [ZeroReturn(), TrainMean(), RidgeReturn()]


def evaluate_direction(
    model: DirectionModel, train: pd.DataFrame, test: pd.DataFrame, horizon: int
) -> Metrics:
    train, test = train.dropna(subset=["y_up"]), test.dropna(subset=["y_up"])
    model.fit(train)
    pred, score = model.predict(test)
    use_score = model.name != "majority"  # a constant score has no ranking information
    return classification_metrics(
        test["y_up"].to_numpy().astype(int),
        pred,
        score if use_score else None,
        horizon=horizon,
        score_is_probability=getattr(model, "probabilistic", False),
    )


def evaluate_return(model: ReturnModel, train: pd.DataFrame, test: pd.DataFrame) -> Metrics:
    model.fit(train)
    return regression_metrics(test["y_return"].to_numpy(), model.predict(test))


@dataclass(frozen=True, slots=True)
class RunConfig:
    labels: LabelConfig = field(default_factory=LabelConfig)
    splits: SplitConfig = field(default_factory=SplitConfig)
    walk_forward_first_year: int = 2015
    walk_forward_last_year: int = 2022  # stays inside the development period


def run(dataset: pd.DataFrame, cfg: RunConfig) -> dict[str, Any]:
    h = cfg.labels.horizon
    parts = chronological_split(dataset, cfg.splits)
    train, validation = parts["train"], parts["validation"]
    report: dict[str, Any] = {
        "feature_set_version": FEATURE_SET_VERSION,
        "config": asdict(cfg),
        "rows": {name: len(part) for name, part in parts.items()},
        "periods": {
            name: [str(part["session_date"].min().date()), str(part["session_date"].max().date())]
            for name, part in parts.items()
        },
        "validation": {"direction": {}, "return": {}},
        "walk_forward": {"direction": {}, "return": {}},
    }
    for model in direction_models():
        report["validation"]["direction"][model.name] = evaluate_direction(
            model, train, validation, h
        )
    for reg in return_models():
        report["validation"]["return"][reg.name] = evaluate_return(reg, train, validation)

    development = pd.concat([train, validation])
    for fold in walk_forward(
        development,
        first_test_year=cfg.walk_forward_first_year,
        last_test_year=cfg.walk_forward_last_year,
        embargo=cfg.splits.embargo_sessions,
    ):
        for model in direction_models():
            m = evaluate_direction(model, fold.train, fold.test, h)
            report["walk_forward"]["direction"].setdefault(model.name, {})[fold.name] = m
        for reg in return_models():
            m = evaluate_return(reg, fold.train, fold.test)
            report["walk_forward"]["return"].setdefault(reg.name, {})[fold.name] = m

    logistic = Logistic().fit(train.dropna(subset=["y_up"]))
    coefficients = logistic.coefficients()
    report["logistic_top_coefficients"] = dict(
        sorted(coefficients.items(), key=lambda kv: -abs(kv[1]))[:10]
    )
    return report


def _fold_summary(folds: dict[str, Metrics], key: str) -> tuple[float, float]:
    values = [m[key] for m in folds.values() if key in m]
    return float(np.mean(values)), float(np.std(values))


def render_markdown(report: dict[str, Any]) -> str:
    v_dir, v_ret = report["validation"]["direction"], report["validation"]["return"]
    wf_dir, wf_ret = report["walk_forward"]["direction"], report["walk_forward"]["return"]
    periods = report["periods"]
    rows = report["rows"]

    def fmt(x: float | None, digits: int = 3) -> str:
        return "n/a" if x is None or (isinstance(x, float) and np.isnan(x)) else f"{x:.{digits}f}"

    lines = [
        "# Baseline Results",
        "",
        "<!-- Generated by `make baselines` (stockml.training.baselines). Do not edit by hand. -->",
        "",
        f"Feature set `{report['feature_set_version']}`; horizon "
        f"{report['config']['labels']['horizon']} sessions; train "
        f"{periods['train'][0]}..{periods['train'][1]} ({rows['train']:,} rows), validation "
        f"{periods['validation'][0]}..{periods['validation'][1]} ({rows['validation']:,} rows). "
        f"The test period ({periods['test'][0]}..{periods['test'][1]}, {rows['test']:,} rows) "
        "is held out and not used here.",
        "",
        "## Direction (up/down over the next 5 sessions): validation",
        "",
        "| Model | Accuracy (95% CI) | Base rate | Balanced acc. | F1 up | ROC-AUC | Log loss |",
        "| --- | --- | ---: | ---: | ---: | ---: | ---: |",
    ]
    for name, m in v_dir.items():
        ci = m["accuracy_ci95"]
        lines.append(
            f"| {name} | {fmt(m['accuracy'])} ({fmt(ci[0])}-{fmt(ci[1])}) | {fmt(m['base_rate'])} "
            f"| {fmt(m['balanced_accuracy'])} | {fmt(m['f1_up'])} | {fmt(m.get('roc_auc'))} "
            f"| {fmt(m.get('log_loss'))} |"
        )
    lines += [
        "",
        "## Direction: walk-forward (expanding window, one fold per year)",
        "",
        "| Model | Accuracy mean ± sd | Accuracy - base rate | Balanced acc. | ROC-AUC |",
        "| --- | --- | ---: | ---: | ---: |",
    ]
    for name, folds in wf_dir.items():
        acc = _fold_summary(folds, "accuracy")
        edge = _fold_summary(folds, "accuracy_minus_base_rate")
        bal = _fold_summary(folds, "balanced_accuracy")
        auc = _fold_summary(folds, "roc_auc") if name != "majority" else (float("nan"), 0.0)
        lines.append(
            f"| {name} | {fmt(acc[0])} ± {fmt(acc[1])} | {edge[0]:+.3f} | {fmt(bal[0])} "
            f"| {fmt(auc[0])} |"
        )
    lines += [
        "",
        "## Return (5-session log return): validation",
        "",
        "| Model | MAE | RMSE | R² | Directional acc. |",
        "| --- | ---: | ---: | ---: | ---: |",
    ]
    for name, m in v_ret.items():
        lines.append(
            f"| {name} | {fmt(m['mae'], 4)} | {fmt(m['rmse'], 4)} | {fmt(m['r2'], 4)} "
            f"| {fmt(m['directional_accuracy'])} |"
        )
    lines += [
        "",
        "## Return: walk-forward",
        "",
        "| Model | RMSE mean | R² mean ± sd |",
        "| --- | ---: | --- |",
    ]
    for name, folds in wf_ret.items():
        rmse = _fold_summary(folds, "rmse")
        r2 = _fold_summary(folds, "r2")
        lines.append(f"| {name} | {fmt(rmse[0], 4)} | {r2[0]:+.4f} ± {fmt(r2[1], 4)} |")
    lines += [
        "",
        "## Logistic regression: largest standardised coefficients",
        "",
        "| Feature | Coefficient |",
        "| --- | ---: |",
        *(f"| `{k}` | {v:+.4f} |" for k, v in report["logistic_top_coefficients"].items()),
        "",
    ]
    return "\n".join(lines)


def load_dataset(name: str, labels: LabelConfig) -> pd.DataFrame | None:
    """Build the labelled feature dataset from the cleaned snapshot and cache it."""
    spec = DatasetSpec.load(name)
    processed = REPO_ROOT / "data" / "processed" / spec.name / spec.revision
    cleaned_path = processed / "cleaned.parquet"
    if not cleaned_path.exists():
        log.error("dataset.missing_input", path=str(cleaned_path), hint="run make data-quality")
        return None
    dataset = build_dataset(pd.read_parquet(cleaned_path), labels)
    features_dir = REPO_ROOT / "data" / "features" / spec.name / spec.revision / FEATURE_SET_VERSION
    features_dir.mkdir(parents=True, exist_ok=True)
    dataset.to_parquet(features_dir / "dataset.parquet", index=False)
    return dataset


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Train and evaluate baseline models")
    parser.add_argument("--dataset", default="us-equities-daily")
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT_MD)
    args = parser.parse_args(argv)
    configure_logging("baselines", level=LogSettings().level, fmt="console")

    cfg = RunConfig()
    dataset = load_dataset(args.dataset, cfg.labels)
    if dataset is None:
        return 1

    report = run(dataset, cfg)
    reports_dir = REPO_ROOT / "data" / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    (reports_dir / "baselines.json").write_text(json.dumps(report, indent=2, default=str) + "\n")
    args.report.write_text(render_markdown(report))
    log.info("baselines.done", report=str(args.report), rows=report["rows"])
    return 0


if __name__ == "__main__":
    sys.exit(main())

"""Phase 7: gradient-boosted models vs the baselines, plus a cost-aware backtest.

    python -m stockml.training.models               # development: tuning, validation, walk-forward
    python -m stockml.training.models --final-test  # also the one-time held-out test evaluation

Protocol (fixed before any result was seen, so it cannot be bent to fit one):

1. **Tuning** uses only walk-forward folds 2015-2019: each GBM grid point is
   scored by mean log loss over those folds.
2. **Reporting** shows the validation split (train <= 2019, evaluate
   2020-2022) and all walk-forward folds 2015-2022, with the 2020-2022 folds
   summarised separately because they played no part in tuning.
3. **Candidate selection**: the probabilistic model with the lowest mean
   walk-forward log loss over 2020-2022. Log loss is a proper scoring rule, so
   it rewards calibrated probabilities instead of just a lucky threshold.
4. **Test**: every model is refitted on 2010-2022 (purged) and evaluated once
   on 2023+. The test result is reported whatever it is; nothing is re-tuned
   after seeing it.
"""

from __future__ import annotations

import argparse
import functools
import json
import sys
from collections.abc import Callable, Iterable
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from shared.config import LogSettings
from shared.features import FEATURE_SET_VERSION
from shared.observability.logs import configure_logging, get_logger
from stockml.data.catalog import REPO_ROOT
from stockml.evaluation.backtest import STRATEGIES, BacktestConfig, backtest_all_offsets
from stockml.evaluation.metrics import Metrics, classification_metrics
from stockml.training.baselines import (
    DirectionModel,
    Logistic,
    Majority,
    Momentum,
    RunConfig,
    load_dataset,
)
from stockml.training.gbm import (
    PARAM_GRID,
    GbmParams,
    LightGBMDirection,
    XGBoostDirection,
)
from stockml.training.splits import chronological_split, purge_and_embargo, walk_forward

log = get_logger(__name__)

DEFAULT_REPORT_MD = REPO_ROOT / "docs" / "MODELS.md"
PROBABILISTIC = ("logistic", "lightgbm", "xgboost")
COST_GRID_BPS = (0.0, 5.0, 10.0, 20.0)


class RandomScore:
    """Control: a random ranking. Shows what costs alone do to a strategy."""

    name = "random"
    probabilistic = False

    def __init__(self, seed: int = 7) -> None:
        self.seed = seed

    def fit(self, frame: pd.DataFrame) -> RandomScore:
        return self

    def predict(self, frame: pd.DataFrame) -> tuple[np.ndarray, np.ndarray]:
        score = np.random.default_rng(self.seed).random(len(frame))
        return (score >= 0.5).astype(int), score


@dataclass(frozen=True, slots=True)
class ModelsConfig:
    run: RunConfig = field(default_factory=RunConfig)
    tuning_years: tuple[int, int] = (2015, 2019)
    untuned_years: tuple[int, int] = (2020, 2022)
    backtest: BacktestConfig = field(default_factory=BacktestConfig)
    grid: tuple[GbmParams, ...] = PARAM_GRID


ModelFactory = Callable[[], DirectionModel]


def _labelled(frame: pd.DataFrame) -> pd.DataFrame:
    return frame.dropna(subset=["y_up"])


def fit_predict(
    factory: ModelFactory, train: pd.DataFrame, test: pd.DataFrame, horizon: int
) -> tuple[Metrics, pd.DataFrame]:
    """Fit on ``train``, score ``test``; returns metrics and the per-row predictions."""
    model = factory()
    train, test = _labelled(train), _labelled(test)
    model.fit(train)
    pred, score = model.predict(test)
    metrics = classification_metrics(
        test["y_up"].to_numpy().astype(int),
        pred,
        score,
        horizon=horizon,
        # The majority model's constant P(up) = training base rate is the null
        # forecast for log loss: a model is only useful if it beats it.
        score_is_probability=getattr(model, "probabilistic", False) or model.name == "majority",
    )
    if model.name == "majority":
        metrics.pop("roc_auc", None)  # a constant score has no ranking information
    describe = getattr(model, "describe", None)
    if describe is not None:
        metrics["model"] = describe()
    predictions = test[["symbol", "session_date", "y_up", "y_return"]].assign(score=score)
    return metrics, predictions


def _mean(values: Iterable[float]) -> float:
    items = [v for v in values if not np.isnan(v)]
    return float(np.mean(items)) if items else float("nan")


def tune(
    development: pd.DataFrame,
    make: Callable[[GbmParams], DirectionModel],
    cfg: ModelsConfig,
) -> list[dict[str, Any]]:
    """Score every grid point on the tuning folds; best (lowest log loss) first."""
    h, embargo = cfg.run.labels.horizon, cfg.run.splits.embargo_sessions
    folds = list(
        walk_forward(
            development,
            first_test_year=cfg.tuning_years[0],
            last_test_year=cfg.tuning_years[1],
            embargo=embargo,
        )
    )
    rows = []
    for params in cfg.grid:
        factory = functools.partial(make, params)
        scores = [fit_predict(factory, f.train, f.test, h)[0] for f in folds]
        rows.append(
            {
                "params": asdict(params),
                "label": params.label(),
                "log_loss": _mean(s["log_loss"] for s in scores),
                "roc_auc": _mean(s["roc_auc"] for s in scores),
                "rounds": _mean(float(s["model"]["best_iteration"]) for s in scores),
            }
        )
        log.info(
            "tune.point",
            model=make(params).name,
            params=params.label(),
            log_loss=rows[-1]["log_loss"],
        )
    return sorted(rows, key=lambda r: r["log_loss"])


def factories(best_lgb: GbmParams, best_xgb: GbmParams, embargo: int) -> dict[str, ModelFactory]:
    return {
        "majority": Majority,
        "momentum": Momentum,
        "random": RandomScore,
        "logistic": Logistic,
        "lightgbm": lambda: LightGBMDirection(best_lgb, embargo=embargo),
        "xgboost": lambda: XGBoostDirection(best_xgb, embargo=embargo),
    }


def backtests(
    predictions: dict[str, pd.DataFrame], cfg: ModelsConfig, candidate: str | None
) -> dict[str, Any]:
    """Every ranking model x strategy at the default cost, plus a cost sweep for the candidate."""
    bt = cfg.backtest
    out: dict[str, Any] = {"config": asdict(bt), "strategies": {}, "cost_sweep": {}}
    any_model = next(iter(predictions.values()))
    out["strategies"]["buy_hold"] = {"benchmark": backtest_all_offsets(any_model, "buy_hold", bt)}
    for strategy in STRATEGIES:
        if strategy == "buy_hold":
            continue
        out["strategies"][strategy] = {
            name: backtest_all_offsets(preds, strategy, bt)
            for name, preds in predictions.items()
            if name != "majority"  # a constant score cannot rank
        }
    if candidate is not None:
        for strategy in ("top_k", "long_short"):
            out["cost_sweep"][strategy] = {
                str(c): backtest_all_offsets(
                    predictions[candidate],
                    strategy,
                    BacktestConfig(bt.horizon, c, bt.k, bt.sessions_per_year),
                )
                for c in COST_GRID_BPS
            }
    return out


def run_development(dataset: pd.DataFrame, cfg: ModelsConfig) -> dict[str, Any]:
    h, embargo = cfg.run.labels.horizon, cfg.run.splits.embargo_sessions
    parts = chronological_split(dataset, cfg.run.splits)
    train, validation = parts["train"], parts["validation"]
    development = pd.concat([train, validation])

    tuning_lgb = tune(development, lambda p: LightGBMDirection(p, embargo=embargo), cfg)
    tuning_xgb = tune(development, lambda p: XGBoostDirection(p, embargo=embargo), cfg)
    best_lgb = GbmParams(**tuning_lgb[0]["params"])
    best_xgb = GbmParams(**tuning_xgb[0]["params"])
    models = factories(best_lgb, best_xgb, embargo)

    report: dict[str, Any] = {
        "feature_set_version": FEATURE_SET_VERSION,
        "horizon": h,
        "rows": {name: len(part) for name, part in parts.items()},
        "periods": {
            name: [str(part["session_date"].min().date()), str(part["session_date"].max().date())]
            for name, part in parts.items()
        },
        "tuning": {"years": list(cfg.tuning_years), "lightgbm": tuning_lgb, "xgboost": tuning_xgb},
        "validation": {},
        "walk_forward": {},
    }
    for name, factory in models.items():
        report["validation"][name] = fit_predict(factory, train, validation, h)[0]

    oos: dict[str, list[pd.DataFrame]] = {name: [] for name in models}
    for fold in walk_forward(
        development,
        first_test_year=cfg.run.walk_forward_first_year,
        last_test_year=cfg.run.walk_forward_last_year,
        embargo=embargo,
    ):
        for name, factory in models.items():
            metrics, preds = fit_predict(factory, fold.train, fold.test, h)
            report["walk_forward"].setdefault(name, {})[fold.name] = metrics
            oos[name].append(preds)
        log.info("walk_forward.fold", year=fold.name)

    untuned = [str(y) for y in range(cfg.untuned_years[0], cfg.untuned_years[1] + 1)]
    candidate = min(
        PROBABILISTIC,
        key=lambda n: _mean(report["walk_forward"][n][y]["log_loss"] for y in untuned),
    )
    untuned_ll = {
        n: _mean(report["walk_forward"][n][y]["log_loss"] for y in untuned)
        for n in (*PROBABILISTIC, "majority")
    }
    report["candidate"] = {
        "name": candidate,
        "untuned_log_loss": untuned_ll[candidate],
        "null_log_loss": untuned_ll["majority"],
        "beats_null": untuned_ll[candidate] < untuned_ll["majority"],
        "rule": "lowest mean walk-forward log loss over the untuned folds "
        f"{cfg.untuned_years[0]}-{cfg.untuned_years[1]}",
    }
    predictions = {name: pd.concat(frames) for name, frames in oos.items()}
    report["backtest_walk_forward"] = backtests(predictions, cfg, candidate)

    lgb = LightGBMDirection(best_lgb, embargo=embargo).fit(_labelled(train))
    report["lightgbm_importance"] = dict(
        sorted(lgb.importance().items(), key=lambda kv: -kv[1])[:10]
    )
    return report


def run_final_test(dataset: pd.DataFrame, cfg: ModelsConfig, report: dict[str, Any]) -> None:
    """Refit on all development data and evaluate once on the held-out test period."""
    h, embargo = cfg.run.labels.horizon, cfg.run.splits.embargo_sessions
    dev_end = pd.Timestamp(cfg.run.splits.validation_end)
    development, test = purge_and_embargo(dataset, dev_end, None, embargo)
    tuning = report["tuning"]
    models = factories(
        GbmParams(**tuning["lightgbm"][0]["params"]),
        GbmParams(**tuning["xgboost"][0]["params"]),
        embargo,
    )
    metrics: dict[str, Metrics] = {}
    predictions: dict[str, pd.DataFrame] = {}
    for name, factory in models.items():
        metrics[name], predictions[name] = fit_predict(factory, development, test, h)
    report["test"] = {
        "period": [str(test["session_date"].min().date()), str(test["session_date"].max().date())],
        "rows": len(test),
        "metrics": metrics,
        "backtest": backtests(predictions, cfg, report["candidate"]["name"]),
    }


# --------------------------------------------------------------------------- report


def _fmt(x: float | None, digits: int = 3) -> str:
    return "n/a" if x is None or (isinstance(x, float) and np.isnan(x)) else f"{x:.{digits}f}"


def _pct(x: float) -> str:
    return "n/a" if np.isnan(x) else f"{x * 100:+.1f}%"


def _fold_stats(folds: dict[str, Metrics], key: str, years: list[str] | None = None) -> float:
    return _mean(m.get(key, float("nan")) for y, m in folds.items() if years is None or y in years)


def _direction_table(metrics: dict[str, Metrics]) -> list[str]:
    lines = [
        "| Model | Accuracy (95% CI) | Base rate | Balanced acc. | ROC-AUC | Log loss |",
        "| --- | --- | ---: | ---: | ---: | ---: |",
    ]
    for name, m in metrics.items():
        ci = m["accuracy_ci95"]
        lines.append(
            f"| {name} | {_fmt(m['accuracy'])} ({_fmt(ci[0])}-{_fmt(ci[1])}) "
            f"| {_fmt(m['base_rate'])} | {_fmt(m['balanced_accuracy'])} "
            f"| {_fmt(m.get('roc_auc'))} | {_fmt(m.get('log_loss'))} |"
        )
    return lines


def _backtest_tables(bt: dict[str, Any]) -> list[str]:
    cfg = bt["config"]
    lines = [
        f"Weekly rebalancing, {cfg['cost_bps']:g} bps per unit turnover, k = {cfg['k']}. "
        "Statistics are for offset 0; the Sharpe range covers all "
        f"{cfg['horizon']} possible rebalancing offsets.",
        "",
        "| Strategy | Model | Ann. return | Ann. vol | Sharpe (range) | t-stat | Max DD "
        "| Turnover | Ann. cost |",
        "| --- | --- | ---: | ---: | --- | ---: | ---: | ---: | ---: |",
    ]
    for strategy, by_model in bt["strategies"].items():
        for name, r in by_model.items():
            s = r["offset_0"]
            lines.append(
                f"| {strategy} | {name} | {_pct(s['ann_return'])} | {_pct(s['ann_volatility'])} "
                f"| {_fmt(s['sharpe'], 2)} ({_fmt(r['sharpe_min'], 2)} to "
                f"{_fmt(r['sharpe_max'], 2)}) | {_fmt(s['t_stat'], 2)} "
                f"| {_pct(s['max_drawdown'])} | {_fmt(s['avg_turnover'], 2)} "
                f"| {_pct(s['ann_cost'])} |"
            )
    return lines


def _cost_sweep(bt: dict[str, Any], candidate: str) -> list[str]:
    lines = [
        f"Cost sensitivity for the candidate (`{candidate}`), median over offsets:",
        "",
        "| Strategy | " + " | ".join(f"{c} bps" for c in bt["cost_sweep"]["top_k"]) + " |",
        "| --- |" + " ---: |" * len(bt["cost_sweep"]["top_k"]),
    ]
    for strategy, by_cost in bt["cost_sweep"].items():
        cells = " | ".join(
            f"{_pct(r['ann_return_median'])} / SR {_fmt(r['sharpe_median'], 2)}"
            for r in by_cost.values()
        )
        lines.append(f"| {strategy} | {cells} |")
    return lines


def render_markdown(report: dict[str, Any]) -> str:
    periods, rows = report["periods"], report["rows"]
    wf = report["walk_forward"]
    tuning_years = report["tuning"]["years"]
    all_years = sorted(next(iter(wf.values())))
    untuned = [y for y in all_years if int(y) > tuning_years[1]]
    lines = [
        "# Model Results",
        "",
        "<!-- Generated by `make models` (stockml.training.models). Do not edit by hand. -->",
        "",
        f"Feature set `{report['feature_set_version']}`, horizon {report['horizon']} sessions. "
        f"Train {periods['train'][0]}..{periods['train'][1]} ({rows['train']:,} rows), "
        f"validation {periods['validation'][0]}..{periods['validation'][1]} "
        f"({rows['validation']:,} rows). Protocol and interpretation: "
        "[ML_PIPELINE.md](ML_PIPELINE.md#phase-7-gradient-boosting-and-backtest).",
        "",
        f"## Hyperparameter tuning (walk-forward folds {tuning_years[0]}-{tuning_years[1]})",
        "",
        "| Model | Parameters | Mean log loss | Mean ROC-AUC | Mean trees |",
        "| --- | --- | ---: | ---: | ---: |",
    ]
    for lib in ("lightgbm", "xgboost"):
        for i, r in enumerate(report["tuning"][lib]):
            mark = " **(selected)**" if i == 0 else ""
            lines.append(
                f"| {lib} | {r['label']}{mark} | {_fmt(r['log_loss'], 4)} "
                f"| {_fmt(r['roc_auc'])} | {r['rounds']:.0f} |"
            )
    lines += [
        "",
        "## Direction: validation (train <= 2019, evaluate 2020-2022)",
        "",
        *_direction_table(report["validation"]),
        "",
        "## Direction: walk-forward",
        "",
        f"Folds {tuning_years[0]}-{tuning_years[1]} were used to tune the GBMs, so their GBM "
        f"numbers are optimistic; folds {untuned[0]}-{untuned[-1]} played no part in tuning.",
        "",
        "| Model | Acc. - base (all) | AUC (all) | Log loss (all) | Acc. - base (untuned) "
        "| AUC (untuned) | Log loss (untuned) |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for name, folds in wf.items():
        lines.append(
            f"| {name} | {_fold_stats(folds, 'accuracy_minus_base_rate'):+.3f} "
            f"| {_fmt(_fold_stats(folds, 'roc_auc'))} | {_fmt(_fold_stats(folds, 'log_loss'), 4)} "
            f"| {_fold_stats(folds, 'accuracy_minus_base_rate', untuned):+.3f} "
            f"| {_fmt(_fold_stats(folds, 'roc_auc', untuned))} "
            f"| {_fmt(_fold_stats(folds, 'log_loss', untuned), 4)} |"
        )
    lines += [
        "",
        "Per-fold ROC-AUC:",
        "",
        "| Model | " + " | ".join(all_years) + " |",
        "| --- |" + " ---: |" * len(all_years),
    ]
    for name, folds in wf.items():
        if name == "majority":
            continue
        lines.append(
            f"| {name} | " + " | ".join(_fmt(folds[y].get("roc_auc")) for y in all_years) + " |"
        )
    candidate = report["candidate"]["name"]
    lines += [
        "",
        f"**Candidate:** `{candidate}` ({report['candidate']['rule']}): log loss "
        f"{_fmt(report['candidate']['untuned_log_loss'], 4)} vs "
        f"{_fmt(report['candidate']['null_log_loss'], 4)} for the base-rate forecast "
        f"(majority), so it **{'beats' if report['candidate']['beats_null'] else 'does not beat'}"
        "** the null forecast.",
        "",
        "## Backtest on walk-forward predictions (2015-2022, out of sample)",
        "",
        *_backtest_tables(report["backtest_walk_forward"]),
        "",
        *_cost_sweep(report["backtest_walk_forward"], candidate),
        "",
        "## LightGBM feature importance (share of split gain, fitted on train)",
        "",
        "| Feature | Gain share |",
        "| --- | ---: |",
        *(f"| `{k}` | {v:.3f} |" for k, v in report["lightgbm_importance"].items()),
        "",
    ]
    if "test" in report:
        test = report["test"]
        lines += [
            f"## Held-out test ({test['period'][0]}..{test['period'][1]}, {test['rows']:,} rows)",
            "",
            "Evaluated once, after the protocol and candidate were fixed. Models refitted on "
            "2010-2022.",
            "",
            *_direction_table(test["metrics"]),
            "",
            *_backtest_tables(test["backtest"]),
            "",
            *_cost_sweep(test["backtest"], candidate),
            "",
        ]
    else:
        lines += ["## Held-out test", "", "Not run (`make final-test`).", ""]
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Train GBMs, compare to baselines, backtest")
    parser.add_argument("--dataset", default="us-equities-daily")
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT_MD)
    parser.add_argument(
        "--final-test",
        action="store_true",
        help="also evaluate once on the held-out test period (2023+)",
    )
    args = parser.parse_args(argv)
    configure_logging("models", level=LogSettings().level, fmt="console")

    cfg = ModelsConfig()
    dataset = load_dataset(args.dataset, cfg.run.labels)
    if dataset is None:
        return 1
    report = run_development(dataset, cfg)
    if args.final_test:
        run_final_test(dataset, cfg, report)
    reports_dir = REPO_ROOT / "data" / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    (reports_dir / "models.json").write_text(json.dumps(report, indent=2, default=str) + "\n")
    args.report.write_text(render_markdown(report))
    log.info("models.done", report=str(args.report), candidate=report["candidate"]["name"])
    return 0


if __name__ == "__main__":
    sys.exit(main())

"""Retraining workflow: fresh evaluation + a new **candidate**, never more.

    python -m stockml.registry.retrain [--years 4] [--reason "monitor: drift alert"]

Triggered by a person or a scheduler, typically after the model monitor
writes a ``retrain_recommended`` report. It:

1. rebuilds the labelled dataset from the latest cleaned data;
2. keeps the hyperparameters chosen in Phase 7 (retraining is not re-tuning:
   re-tuning on every retrain would quietly reuse the same recent data for
   selection again and again);
3. evaluates on the most recent complete years with the same purged,
   embargoed walk-forward procedure: all but the last year give the
   ``eval.wf_*`` numbers, the last year gives ``eval.test_*``, each against
   the base-rate forecast fitted on the same training fold;
4. fits on all data and registers the result as a ``candidate`` with those
   numbers as its evaluation tags.

The promotion gates then judge the candidate on this fresh evidence. Nothing
in this module can move a model past ``candidate``.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pandas as pd
from mlflow import MlflowClient

from shared.config import LogSettings
from shared.observability.logs import configure_logging, get_logger
from stockml.data.catalog import DatasetSpec
from stockml.registry.promote import DEFAULT_MODEL_NAME
from stockml.registry.train import MODEL_TYPES, MODELS_REPORT, build_model, train_and_register
from stockml.training.baselines import DirectionModel, Majority, load_dataset
from stockml.training.models import ModelsConfig, fit_predict
from stockml.training.splits import walk_forward

log = get_logger(__name__)


def recent_evaluation(
    dataset: pd.DataFrame,
    make_model: Callable[[], DirectionModel],
    *,
    years: int,
    horizon: int,
    embargo: int,
) -> dict[str, Any]:
    """Walk-forward over the last ``years`` complete years; the last one is the 'test'."""
    last_full = int(dataset["label_end"].max().year) - 1
    first = last_full - years + 1
    folds: dict[str, dict[str, Any]] = {}
    for fold in walk_forward(
        dataset, first_test_year=first, last_test_year=last_full, embargo=embargo
    ):
        model_metrics, _ = fit_predict(make_model, fold.train, fold.test, horizon)
        null_metrics, _ = fit_predict(Majority, fold.train, fold.test, horizon)
        folds[fold.name] = {"model": model_metrics, "null": null_metrics}
    wf_years = [str(y) for y in range(first, last_full)]
    test = folds[str(last_full)]

    def mean(kind: str) -> float:
        return sum(float(folds[y][kind]["log_loss"]) for y in wf_years) / len(wf_years)

    return {
        "protocol": f"walk-forward {first}-{last_full - 1}, test {last_full}, fixed params",
        "folds": folds,
        "tags": {
            "eval.wf_log_loss": f"{mean('model'):.6f}",
            "eval.wf_null_log_loss": f"{mean('null'):.6f}",
            "eval.test_log_loss": f"{test['model']['log_loss']:.6f}",
            "eval.test_null_log_loss": f"{test['null']['log_loss']:.6f}",
            "eval.test_roc_auc": f"{test['model']['roc_auc']:.6f}",
            "eval.test_accuracy": f"{test['model']['accuracy']:.6f}",
            "eval.test_base_rate": f"{test['model']['base_rate']:.6f}",
            "evaluation_protocol": f"retrain: wf {first}-{last_full - 1}, test {last_full}",
        },
    }


def retrain(
    dataset: pd.DataFrame,
    report: dict[str, Any],
    *,
    client: MlflowClient,
    model_type: str,
    model_name: str,
    years: int,
    reason: str,
    audit_file: Path | None,
    dataset_revision: str,
) -> str:
    if years < 2:
        raise ValueError("need at least 2 years: one for walk-forward, one for test")
    cfg = ModelsConfig()
    embargo, horizon = cfg.run.splits.embargo_sessions, cfg.run.labels.horizon
    evaluation = recent_evaluation(
        dataset,
        lambda: build_model(model_type, report, embargo),
        years=years,
        horizon=horizon,
        embargo=embargo,
    )
    log.info("retrain.evaluated", model_type=model_type, **evaluation["tags"])
    return train_and_register(
        dataset,
        report,
        client=client,
        model_type=model_type,
        model_name=model_name,
        audit_file=audit_file,
        dataset_revision=dataset_revision,
        eval_tags=evaluation["tags"],
        reason=f"retraining ({evaluation['protocol']}): {reason}",
        artifacts={"evaluation/retrain.json": evaluation},
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Retrain and register a new candidate")
    parser.add_argument("--dataset", default="us-equities-daily")
    parser.add_argument("--model", choices=MODEL_TYPES, default=None, help="default: candidate")
    parser.add_argument("--model-name", default=DEFAULT_MODEL_NAME)
    parser.add_argument("--years", type=int, default=4)
    parser.add_argument("--reason", default="scheduled retraining")
    parser.add_argument("--audit-file", type=Path, default=None)
    args = parser.parse_args(argv)
    configure_logging("retrain", level=LogSettings().level, fmt="console")

    if not MODELS_REPORT.exists():
        log.error("retrain.missing_report", path=str(MODELS_REPORT), hint="run make final-test")
        return 1
    report = json.loads(MODELS_REPORT.read_text())
    model_type = args.model or report["candidate"]["name"]
    dataset = load_dataset(args.dataset, ModelsConfig().run.labels)
    if dataset is None:
        return 1
    version = retrain(
        dataset,
        report,
        client=MlflowClient(),
        model_type=model_type,
        model_name=args.model_name,
        years=args.years,
        reason=args.reason,
        audit_file=args.audit_file,
        dataset_revision=DatasetSpec.load(args.dataset).revision,
    )
    print(f"registered {args.model_name} version {version} as candidate")
    return 0


if __name__ == "__main__":
    sys.exit(main())

"""Train the production model and register it as a ``candidate``.

    python -m stockml.registry.train [--model xgboost] [--audit-file PATH]

What it does, in order:

1. Reads the Phase 7 report (``data/reports/models.json``, written by
   ``make final-test``) for the candidate model type, its tuned
   hyperparameters and its evaluation numbers. Without that report it refuses:
   a model is never registered without the evidence that describes it.
2. Fits that model on **all** labelled data. The evaluation estimated how the
   procedure generalises; the deployed model uses the most recent data too.
   GBMs keep the same procedure (early stopping on the last year).
3. Logs an MLflow run (params, evaluation metrics, the report as artifacts)
   and the model in its native flavor, with ``feature_set_version`` and the
   feature order in the model metadata so the server can check compatibility.
4. Registers a new version and moves it to ``candidate`` through the same
   audited path as every other transition. It never promotes further.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

import lightgbm as lgb
import mlflow
import pandas as pd
from mlflow import MlflowClient

from shared.config import LogSettings
from shared.features import FEATURE_NAMES, FEATURE_SET_VERSION
from shared.monitoring import ReferenceProfile, profile
from shared.observability.logs import configure_logging, get_logger
from stockml.data.catalog import REPO_ROOT, DatasetSpec
from stockml.registry.promote import DEFAULT_MODEL_NAME, make_audit_sink, promote
from stockml.registry.stages import Stage
from stockml.training.baselines import DirectionModel, Logistic, load_dataset
from stockml.training.gbm import GbmParams, LightGBMDirection, XGBoostDirection
from stockml.training.models import ModelsConfig

log = get_logger(__name__)

EXPERIMENT = "stockintel-direction"
MODELS_REPORT = REPO_ROOT / "data" / "reports" / "models.json"
FEATURES = list(FEATURE_NAMES)
MODEL_TYPES = ("logistic", "lightgbm", "xgboost")


def evaluation_tags(report: dict[str, Any], model_type: str) -> dict[str, str]:
    """``eval.*`` tags from the Phase 7 report (untuned walk-forward + test)."""
    if "test" not in report:
        raise ValueError("models.json has no held-out test section: run `make final-test` first")
    first, last = ModelsConfig().untuned_years
    untuned = [str(y) for y in range(first, last + 1)]
    wf = report["walk_forward"]

    def wf_mean(name: str) -> float:
        return float(sum(wf[name][y]["log_loss"] for y in untuned)) / len(untuned)

    test = report["test"]["metrics"]
    return {
        "eval.wf_log_loss": f"{wf_mean(model_type):.6f}",
        "eval.wf_null_log_loss": f"{wf_mean('majority'):.6f}",
        "eval.test_log_loss": f"{test[model_type]['log_loss']:.6f}",
        "eval.test_null_log_loss": f"{test['majority']['log_loss']:.6f}",
        "eval.test_roc_auc": f"{test[model_type]['roc_auc']:.6f}",
        "eval.test_accuracy": f"{test[model_type]['accuracy']:.6f}",
        "eval.test_base_rate": f"{test[model_type]['base_rate']:.6f}",
    }


def build_model(model_type: str, report: dict[str, Any], embargo: int) -> DirectionModel:
    if model_type == "logistic":
        return Logistic()
    params = GbmParams(**report["tuning"][model_type][0]["params"])
    if model_type == "lightgbm":
        return LightGBMDirection(params, embargo=embargo)
    return XGBoostDirection(params, embargo=embargo)


def log_model(model: DirectionModel, example: pd.DataFrame, metadata: dict[str, Any]) -> str:
    """Log in the native flavor, trimmed to the early-stopped trees. Returns the model URI."""
    if isinstance(model, XGBoostDirection):
        if model.booster is None:
            raise RuntimeError("model is not fitted")
        trimmed = model.booster[: model.best_iteration]
        info = mlflow.xgboost.log_model(
            trimmed, name="model", input_example=example, metadata=metadata
        )
    elif isinstance(model, LightGBMDirection):
        if model.booster is None:
            raise RuntimeError("model is not fitted")
        trimmed_lgb = lgb.Booster(
            model_str=model.booster.model_to_string(num_iteration=model.best_iteration)
        )
        info = mlflow.lightgbm.log_model(
            trimmed_lgb, name="model", input_example=example, metadata=metadata
        )
    elif isinstance(model, Logistic):
        info = mlflow.sklearn.log_model(
            model.pipeline,
            name="model",
            input_example=example,
            metadata=metadata,
            pyfunc_predict_fn="predict_proba",
        )
    else:
        raise TypeError(f"cannot log {type(model).__name__}")
    return str(info.model_uri)


def git_commit() -> str:
    try:
        out = subprocess.run(
            ["git", "rev-parse", "HEAD"],  # noqa: S607 - fixed command, no user input
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            check=True,
            timeout=5,
        )
        return out.stdout.strip()
    except (OSError, subprocess.SubprocessError):
        return "unknown"


def reference_profile(model: DirectionModel, labelled: pd.DataFrame) -> ReferenceProfile:
    """Training distributions for the monitor.

    Inputs: all training rows. Output (P(up)): the most recent year of training
    data, which for the GBMs is their early-stopping set (not fitted on), so
    the reference resembles out-of-sample output more than in-sample output.
    """
    recent = labelled[
        labelled["session_date"] > labelled["session_date"].max() - pd.DateOffset(years=1)
    ]
    _, p_up = model.predict(recent)
    return ReferenceProfile(
        feature_set_version=FEATURE_SET_VERSION,
        features={f: profile(labelled[f].to_numpy(dtype=float)) for f in FEATURES},
        prediction=profile(p_up),
        base_rate=float(labelled["y_up"].mean()),
    )


def train_and_register(
    dataset: pd.DataFrame,
    report: dict[str, Any],
    *,
    client: MlflowClient,
    model_type: str,
    model_name: str,
    audit_file: Path | None,
    dataset_revision: str,
    eval_tags: dict[str, str] | None = None,
    reason: str | None = None,
    artifacts: dict[str, dict[str, Any]] | None = None,
) -> str:
    """Fit on all labelled rows, log, register and move to ``candidate``.

    ``eval_tags`` defaults to the Phase 7 evaluation in ``report``; the
    retraining workflow passes its own, fresher evaluation instead.
    """
    cfg = ModelsConfig()
    labelled = dataset.dropna(subset=["y_up"])
    model = build_model(model_type, report, cfg.run.splits.embargo_sessions)
    model.fit(labelled)

    tags = {
        "model_type": model_type,
        "feature_set_version": FEATURE_SET_VERSION,
        "horizon_bars": str(cfg.run.labels.horizon),
        "task": "direction",
        "train_start": str(labelled["session_date"].min().date()),
        "train_end": str(labelled["label_end"].max().date()),
        "train_rows": str(len(labelled)),
        "dataset_revision": dataset_revision,
        "git_commit": git_commit(),
        **(eval_tags if eval_tags is not None else evaluation_tags(report, model_type)),
    }
    tags.setdefault(
        "evaluation_protocol", "phase 7: untuned walk-forward 2020-2022, held-out test 2023+"
    )
    metadata = {
        "feature_set_version": FEATURE_SET_VERSION,
        "feature_names": FEATURES,
        "horizon_bars": cfg.run.labels.horizon,
        "task": "direction",
        "output": "probability of an up move over the horizon",
        "model_type": model_type,
    }
    mlflow.set_experiment(EXPERIMENT)
    with mlflow.start_run(run_name=f"{model_type}-{tags['train_end']}") as run:
        mlflow.set_tags(tags)
        describe = getattr(model, "describe", None)
        mlflow.log_params(describe() if describe else {"C": 0.1})
        mlflow.log_metrics(
            {k.removeprefix("eval."): float(v) for k, v in tags.items() if k.startswith("eval.")}
        )
        mlflow.log_dict(report, "evaluation/models.json")
        mlflow.log_dict({"features": FEATURES}, "features.json")
        mlflow.log_dict(
            reference_profile(model, labelled).to_dict(), "monitoring/reference_profile.json"
        )
        for path, content in (artifacts or {}).items():
            mlflow.log_dict(content, path)
        model_uri = log_model(model, labelled[FEATURES].head(5).astype(float), metadata)
        version = mlflow.register_model(model_uri, model_name, tags=tags)
        log.info(
            "registry.registered", model=model_name, version=version.version, run=run.info.run_id
        )

    promote(
        client,
        make_audit_sink(audit_file),
        name=model_name,
        version=str(version.version),
        to=Stage.CANDIDATE,
        reason=reason or f"registered by training ({model_type}, data to {tags['train_end']})",
        actor="training-pipeline",
    )
    return str(version.version)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Train the production model and register it")
    parser.add_argument("--dataset", default="us-equities-daily")
    parser.add_argument("--model", choices=MODEL_TYPES, default=None, help="default: candidate")
    parser.add_argument("--model-name", default=DEFAULT_MODEL_NAME)
    parser.add_argument("--audit-file", type=Path, default=None)
    args = parser.parse_args(argv)
    configure_logging("register", level=LogSettings().level, fmt="console")

    if not MODELS_REPORT.exists():
        log.error("registry.missing_report", path=str(MODELS_REPORT), hint="run make final-test")
        return 1
    report = json.loads(MODELS_REPORT.read_text())
    if "test" not in report:
        log.error("registry.no_test_section", hint="run make final-test")
        return 1
    model_type = args.model or report["candidate"]["name"]
    dataset = load_dataset(args.dataset, ModelsConfig().run.labels)
    if dataset is None:
        return 1
    version = train_and_register(
        dataset,
        report,
        client=MlflowClient(),
        model_type=model_type,
        model_name=args.model_name,
        audit_file=args.audit_file,
        dataset_revision=DatasetSpec.load(args.dataset).revision,
    )
    print(f"registered {args.model_name} version {version} as candidate")
    return 0


if __name__ == "__main__":
    sys.exit(main())

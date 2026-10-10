"""Registration, audited promotion, and loading models back for serving.

Runs a real MLflow tracking/registry store on SQLite in a temp directory, so
it needs no server (the first test pays ~10 s for MLflow's schema migrations).
"""

from __future__ import annotations

import os
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import mlflow
import numpy as np
import pandas as pd
import pytest
from ml_fixtures import cleaned_frame
from mlflow import MlflowClient

from inference.model import MlflowModelSource, NativePredictor, PyfuncPredictor
from model_monitor.store import MlflowProfileSource
from shared.features import FEATURE_NAMES, FEATURE_SET_VERSION
from stockml.features.dataset import build_dataset
from stockml.registry.audit import JsonlAuditSink, MemoryAuditSink
from stockml.registry.promote import promote, status
from stockml.registry.retrain import retrain
from stockml.registry.stages import STAGE_TAG, PromotionError, Stage
from stockml.registry.train import evaluation_tags, train_and_register

NAME = "test-direction"


def fake_report(*, gain: float) -> dict[str, Any]:
    """A models.json-shaped report; ``gain`` = model log-loss improvement over the null."""
    folds = {str(y): {"log_loss": 0.69 - gain} for y in range(2020, 2023)}
    null_folds = {str(y): {"log_loss": 0.69} for y in range(2020, 2023)}
    metrics = {"log_loss": 0.68 - gain, "roc_auc": 0.55, "accuracy": 0.56, "base_rate": 0.55}
    params = {"max_rounds": 30, "early_stopping_rounds": 10}
    return {
        "tuning": {"lightgbm": [{"params": params}], "xgboost": [{"params": params}]},
        "walk_forward": {name: folds for name in ("logistic", "lightgbm", "xgboost")}
        | {"majority": null_folds},
        "test": {
            "metrics": {name: metrics for name in ("logistic", "lightgbm", "xgboost")}
            | {"majority": {"log_loss": 0.68}}
        },
    }


@pytest.fixture(scope="module", name="registry")
def registry_fixture(tmp_path_factory: pytest.TempPathFactory) -> Iterator[MlflowClient]:
    root = tmp_path_factory.mktemp("mlflow")
    previous_cwd, previous_uri = Path.cwd(), mlflow.get_tracking_uri()
    os.chdir(root)  # the default artifact root (./mlruns) lands in the temp dir
    mlflow.set_tracking_uri(f"sqlite:///{root / 'mlflow.db'}")
    try:
        yield MlflowClient()
    finally:
        mlflow.set_tracking_uri(previous_uri)
        os.chdir(previous_cwd)


@pytest.fixture(scope="module", name="dataset")
def dataset_fixture() -> pd.DataFrame:
    return build_dataset(cleaned_frame(900, ("AAA", "BBB"), seed=5))


@pytest.fixture(scope="module", name="versions")
def versions_fixture(
    registry: MlflowClient, dataset: pd.DataFrame, tmp_path_factory: pytest.TempPathFactory
) -> dict[str, str]:
    audit = tmp_path_factory.mktemp("audit") / "audit.jsonl"
    versions = {}
    for model_type, gain in (("xgboost", 0.0), ("lightgbm", 0.01), ("logistic", 0.01)):
        versions[model_type] = train_and_register(
            dataset,
            fake_report(gain=gain),
            client=registry,
            model_type=model_type,
            model_name=NAME,
            audit_file=audit,
            dataset_revision="test",
        )
    lines = audit.read_text().splitlines()
    assert len(lines) == 3, "every registration is audited"
    return versions


def test_registration_records_evidence_and_stops_at_candidate(
    registry: MlflowClient, versions: dict[str, str]
) -> None:
    mv = registry.get_model_version(NAME, versions["xgboost"])
    assert mv.tags["feature_set_version"] == FEATURE_SET_VERSION
    assert mv.tags[STAGE_TAG] == "candidate"
    assert float(mv.tags["eval.test_null_log_loss"]) == pytest.approx(0.68)
    # The newest registration holds the candidate alias; nothing became champion.
    assert (
        str(registry.get_model_version_by_alias(NAME, "candidate").version) == versions["logistic"]
    )
    with pytest.raises(Exception, match="champion"):
        registry.get_model_version_by_alias(NAME, "champion")


def test_evaluation_tags_require_a_test_section() -> None:
    report = fake_report(gain=0.0)
    del report["test"]
    with pytest.raises(ValueError, match="final-test"):
        evaluation_tags(report, "xgboost")


class TestPromotion:
    def test_soft_gate_blocks_until_overridden_and_is_recorded(
        self, registry: MlflowClient, versions: dict[str, str]
    ) -> None:
        audit = MemoryAuditSink()
        kwargs: dict[str, Any] = {"name": NAME, "version": versions["xgboost"], "actor": "test"}
        with pytest.raises(PromotionError, match="beats_base_rate"):
            promote(registry, audit, to=Stage.CHAMPION, reason="no", **kwargs)
        assert audit.entries == []
        assert registry.get_model_version(NAME, versions["xgboost"]).tags[STAGE_TAG] == "candidate"

        result = promote(
            registry, audit, to=Stage.CHAMPION, reason="demo", override_gates=True, **kwargs
        )
        (entry,) = result.entries
        assert (entry.from_stage, entry.to_stage) == ("candidate", "champion")
        assert entry.metrics["overridden_gates"] == ["beats_base_rate"]
        assert (
            str(registry.get_model_version_by_alias(NAME, "champion").version)
            == versions["xgboost"]
        )

    def test_new_champion_archives_the_previous_one(
        self, registry: MlflowClient, versions: dict[str, str]
    ) -> None:
        audit = MemoryAuditSink()
        promote(
            registry,
            audit,
            name=NAME,
            version=versions["lightgbm"],
            to=Stage.CHAMPION,
            reason="passes gates",
            actor="test",
        )
        moves = [(e.model_version, e.from_stage, e.to_stage) for e in audit.entries]
        assert moves == [
            (versions["lightgbm"], "candidate", "champion"),
            (versions["xgboost"], "champion", "archived"),
        ]
        old = registry.get_model_version(NAME, versions["xgboost"])
        assert old.tags[STAGE_TAG] == "archived"
        assert "champion" not in old.aliases
        rows = {r["version"]: r for r in status(registry, NAME)}
        assert rows[int(versions["lightgbm"])]["aliases"] == ["champion"]

    def test_refusals(self, registry: MlflowClient, versions: dict[str, str]) -> None:
        audit = MemoryAuditSink()
        archived = versions["xgboost"]
        with pytest.raises(PromotionError, match="cannot move from archived"):
            promote(
                registry, audit, name=NAME, version=archived, to=Stage.CHAMPION,
                reason="revive", actor="t", override_gates=True,
            )  # fmt: skip
        with pytest.raises(PromotionError, match="reason is required"):
            promote(
                registry, audit, name=NAME, version=versions["logistic"],
                to=Stage.CHALLENGER, reason=" ", actor="t",
            )  # fmt: skip
        with pytest.raises(PromotionError, match="not found"):
            promote(
                registry, audit, name=NAME, version="999", to=Stage.CHALLENGER,
                reason="x", actor="t",
            )  # fmt: skip
        assert audit.entries == []

    def test_hard_gate_cannot_be_overridden(
        self, registry: MlflowClient, versions: dict[str, str]
    ) -> None:
        version = versions["logistic"]
        registry.set_model_version_tag(NAME, version, "feature_set_version", "fs-0.0.1")
        try:
            with pytest.raises(PromotionError, match="feature_set_compatible"):
                promote(
                    registry, MemoryAuditSink(), name=NAME, version=version,
                    to=Stage.CHALLENGER, reason="x", actor="t", override_gates=True,
                )  # fmt: skip
        finally:
            registry.set_model_version_tag(
                NAME, version, "feature_set_version", FEATURE_SET_VERSION
            )

    def test_jsonl_sink_writes_nothing_when_the_change_fails(self, tmp_path: Path) -> None:
        sink = JsonlAuditSink(tmp_path / "audit.jsonl")

        def broken() -> None:
            raise RuntimeError("registry unavailable")

        with pytest.raises(RuntimeError):
            sink.record([], broken)
        assert not (tmp_path / "audit.jsonl").exists()


@pytest.mark.parametrize("model_type", ["xgboost", "lightgbm", "logistic"])
def test_served_predictions_match_mlflow_pyfunc(
    registry: MlflowClient, versions: dict[str, str], dataset: pd.DataFrame, model_type: str
) -> None:
    """The fast native path must give the same probabilities as MLflow's generic one."""
    loaded = MlflowModelSource().load(NAME, versions[model_type], "champion")
    assert loaded.feature_names == tuple(FEATURE_NAMES)
    assert loaded.horizon_bars == 5
    if model_type != "logistic":
        assert isinstance(loaded.predictor, NativePredictor)
    rows = dataset[list(FEATURE_NAMES)].tail(20).to_numpy(dtype=np.float64)
    served = loaded.predict_up(rows)
    reference = PyfuncPredictor(
        mlflow.pyfunc.load_model(f"models:/{NAME}/{versions[model_type]}")
    ).predict_matrix(rows, loaded.feature_names)
    reference = np.asarray(reference, dtype=float)
    if reference.ndim == 2:
        reference = reference[:, -1]
    np.testing.assert_allclose(served, reference, rtol=1e-6)
    assert np.all((served > 0) & (served < 1))


def test_reference_profile_is_stored_for_the_monitor(
    registry: MlflowClient, versions: dict[str, str]
) -> None:
    reference = MlflowProfileSource().get(NAME, versions["xgboost"])
    assert reference is not None
    assert set(reference.features) == set(FEATURE_NAMES)
    assert 0 < reference.base_rate < 1
    assert reference.prediction.n > 0


def test_retraining_produces_only_a_candidate_with_fresh_evidence(
    registry: MlflowClient, versions: dict[str, str], tmp_path: Path
) -> None:
    champion_before = str(registry.get_model_version_by_alias(NAME, "champion").version)
    dataset = build_dataset(cleaned_frame(1_400, ("AAA", "BBB"), seed=9))
    version = retrain(
        dataset,
        fake_report(gain=0.0),
        client=registry,
        model_type="logistic",
        model_name=NAME,
        years=2,
        reason="test",
        audit_file=tmp_path / "audit.jsonl",
        dataset_revision="test",
    )
    mv = registry.get_model_version(NAME, version)
    assert mv.tags[STAGE_TAG] == "candidate"
    assert mv.tags["evaluation_protocol"] == "retrain: wf 2019-2019, test 2020"
    assert float(mv.tags["eval.test_null_log_loss"]) != pytest.approx(0.68), "not the old report"
    assert str(registry.get_model_version_by_alias(NAME, "champion").version) == champion_before

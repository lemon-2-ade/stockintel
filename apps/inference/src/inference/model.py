"""Loading the served model from the registry, and swapping it on promotion.

``ModelHolder`` owns the current ``LoadedModel``. A background task re-resolves
the alias periodically; when it points at a new version, the new model is
loaded off the event loop, checked for compatibility, and swapped in with a
single reference assignment. Requests already running keep the model object
they started with, so a swap never mixes two models within one response.

If a load fails or the new version is incompatible, the previous model keeps
serving and the failure is logged and counted. If no model has ever loaded,
``/ready`` reports 503 and ``/predict`` returns 503: the prediction pipeline
then degrades to "no predictions" while the rest of the platform keeps working.
"""

from __future__ import annotations

import asyncio
import contextlib
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Protocol

import numpy as np
import pandas as pd

from inference.metrics import InferenceMetrics
from shared.observability.logs import get_logger
from shared.schemas.inference import ModelInfo

log = get_logger(__name__)


class Predictor(Protocol):
    def predict(self, data: pd.DataFrame) -> Any: ...


class MatrixPredictor(Protocol):
    def predict_matrix(self, matrix: np.ndarray, columns: tuple[str, ...]) -> Any:
        """Scores for rows of ``matrix`` whose columns are ``columns`` (training order)."""
        ...


class PyfuncPredictor:
    """Generic path: MLflow's pyfunc wrapper (DataFrame in, schema-checked)."""

    def __init__(self, model: Predictor) -> None:
        self.model = model

    def predict_matrix(self, matrix: np.ndarray, columns: tuple[str, ...]) -> Any:
        return self.model.predict(pd.DataFrame(matrix, columns=list(columns)))


class NativePredictor:
    """Calls the underlying library directly with a float array.

    MLflow's generic ``pyfunc.predict`` converts and schema-checks a DataFrame
    on every call; with XGBoost's DataFrame path that costs several
    milliseconds per request even for one row, while the model itself needs a
    fraction of a millisecond on a NumPy array. The service already enforces
    the input contract (exact feature names, order and finite values), so the
    native call is safe. Unknown flavors fall back to the pyfunc wrapper.
    """

    def __init__(self, raw: Any) -> None:
        self.raw = raw
        kind = type(raw).__module__.split(".")[0]
        if kind not in {"xgboost", "lightgbm", "sklearn"}:
            raise TypeError(f"no native path for {type(raw).__name__}")
        self.kind = kind
        if kind == "xgboost":
            raw.set_param({"nthread": 1})  # tiny batches: threading costs more than it saves

    def predict_matrix(self, matrix: np.ndarray, columns: tuple[str, ...]) -> Any:
        if self.kind == "xgboost":
            return self.raw.inplace_predict(matrix)
        if self.kind == "lightgbm":
            return self.raw.predict(matrix, num_threads=1)
        # sklearn pipelines were fitted on named columns: keep the names.
        return self.raw.predict_proba(pd.DataFrame(matrix, columns=list(columns)))


class IncompatibleModelError(Exception):
    """The model cannot be served by this build of the service."""


@dataclass(frozen=True, slots=True)
class LoadedModel:
    name: str
    version: str
    alias: str
    feature_set_version: str
    feature_names: tuple[str, ...]
    horizon_bars: int
    predictor: MatrixPredictor
    run_id: str | None = None
    loaded_at: datetime = field(default_factory=lambda: datetime.now(UTC))

    def predict_up(self, matrix: np.ndarray) -> np.ndarray:
        """P(up) per row; ``matrix`` columns must be in ``feature_names`` order."""
        raw = np.asarray(self.predictor.predict_matrix(matrix, self.feature_names), dtype=float)
        if raw.ndim == 2:  # classifiers logged with predict_proba: [P(down), P(up)]
            raw = raw[:, -1]
        p_up = raw.reshape(-1)
        if p_up.shape[0] != matrix.shape[0] or not np.all((p_up >= 0) & (p_up <= 1)):
            raise ValueError("model output is not one probability per row")
        return p_up

    def info(self) -> ModelInfo:
        return ModelInfo(
            name=self.name,
            version=self.version,
            alias=self.alias,
            feature_set_version=self.feature_set_version,
            horizon_bars=self.horizon_bars,
            run_id=self.run_id,
            loaded_at=self.loaded_at,
        )


class ModelSource(Protocol):
    def resolve(self, name: str, alias: str) -> str | None:
        """Version the alias points at, or None if it is not set."""
        ...

    def load(self, name: str, version: str, alias: str) -> LoadedModel: ...


def from_metadata(
    name: str,
    version: str,
    alias: str,
    predictor: MatrixPredictor,
    *,
    metadata: dict[str, Any],
    run_id: str | None,
) -> LoadedModel:
    try:
        return LoadedModel(
            name=name,
            version=version,
            alias=alias,
            feature_set_version=str(metadata["feature_set_version"]),
            feature_names=tuple(metadata["feature_names"]),
            horizon_bars=int(metadata["horizon_bars"]),
            predictor=predictor,
            run_id=run_id,
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise IncompatibleModelError(f"model metadata incomplete: {exc}") from exc


class MlflowModelSource:
    """Reads ``models:/<name>@<alias>`` from the MLflow registry."""

    def resolve(self, name: str, alias: str) -> str | None:
        from mlflow import MlflowClient  # noqa: PLC0415 - heavy import, only when serving
        from mlflow.exceptions import MlflowException  # noqa: PLC0415

        try:
            return str(MlflowClient().get_model_version_by_alias(name, alias).version)
        except MlflowException as exc:
            if "RESOURCE_DOES_NOT_EXIST" in str(exc.error_code) or "not found" in str(exc):
                return None
            raise

    def load(self, name: str, version: str, alias: str) -> LoadedModel:
        import mlflow  # noqa: PLC0415

        model = mlflow.pyfunc.load_model(f"models:/{name}/{version}")
        meta = model.metadata
        predictor: MatrixPredictor
        try:
            predictor = NativePredictor(model.get_raw_model())
        except (TypeError, ValueError, AttributeError):
            predictor = PyfuncPredictor(model)
        return from_metadata(
            name, version, alias, predictor, metadata=dict(meta.metadata or {}), run_id=meta.run_id
        )


class ModelHolder:
    def __init__(
        self,
        source: ModelSource,
        *,
        name: str,
        alias: str,
        feature_set_version: str,
        metrics: InferenceMetrics,
    ) -> None:
        self.source = source
        self.name = name
        self.alias = alias
        self.feature_set_version = feature_set_version
        self.metrics = metrics
        self.current: LoadedModel | None = None
        self._task: asyncio.Task[None] | None = None

    def _check(self, model: LoadedModel) -> None:
        if model.feature_set_version != self.feature_set_version:
            raise IncompatibleModelError(
                f"model uses features {model.feature_set_version}, "
                f"service computes {self.feature_set_version}"
            )

    async def refresh(self) -> bool:
        """Load the alias target if it changed. Returns True if a new model was swapped in."""
        try:
            version = await asyncio.to_thread(self.source.resolve, self.name, self.alias)
            if version is None:
                log.warning("model.alias_unset", model=self.name, alias=self.alias)
                return False
            if self.current is not None and self.current.version == version:
                return False
            model = await asyncio.to_thread(self.source.load, self.name, version, self.alias)
            self._check(model)
        except Exception as exc:
            self.metrics.load_failures.inc()
            log.error("model.load_failed", model=self.name, alias=self.alias, error=str(exc))
            return False
        previous = self.current
        self.current = model
        self.metrics.set_model(model.name, model.version, previous)
        log.info(
            "model.loaded",
            model=model.name,
            version=model.version,
            previous=previous.version if previous else None,
        )
        return True

    def start(self, interval_s: float) -> None:
        async def loop() -> None:
            while True:
                await self.refresh()
                await asyncio.sleep(interval_s)

        self._task = asyncio.create_task(loop(), name="model-refresh")

    async def stop(self) -> None:
        if self._task is not None:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task

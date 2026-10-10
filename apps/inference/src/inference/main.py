"""Inference service (``python -m inference.main``).

Endpoints
    POST /predict   score up to 256 feature vectors with the @champion model
    GET  /model     which model is served
    GET  /health    liveness
    GET  /ready     503 until a compatible model is loaded
    GET  /metrics   Prometheus
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from inference.config import InferenceSettings
from inference.metrics import InferenceMetrics
from inference.model import LoadedModel, MlflowModelSource, ModelHolder, ModelSource
from inference.predictor import InvalidRequestError, predict
from shared.config import LogSettings
from shared.observability.logs import configure_logging, get_logger
from shared.schemas.inference import ModelInfo, PredictRequest, PredictResponse

log = get_logger(__name__)

DESCRIPTION = """
Scores precomputed feature vectors with the model currently aliased
`@champion` in the MLflow registry. Outputs are statistical estimates of the
direction of the next 5 bars, not investment advice; see docs/MODELS.md for
how (little) skill the current model has.
"""


def create_app(
    settings: InferenceSettings | None = None,
    *,
    source: ModelSource | None = None,
    metrics: InferenceMetrics | None = None,
    refresh: bool = True,
) -> FastAPI:
    """``source`` defaults to the MLflow registry; tests pass a fake.
    ``refresh=False`` loads once at startup without the background task."""
    settings = settings or InferenceSettings()
    metrics = metrics or InferenceMetrics()
    holder = ModelHolder(
        source or MlflowModelSource(),
        name=settings.model_name,
        alias=settings.model_alias,
        feature_set_version=settings.feature_set_version,
        metrics=metrics,
    )

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        if refresh:
            holder.start(settings.refresh_interval_s)
        else:
            await holder.refresh()
        log.info("inference.started", model=settings.model_name, alias=settings.model_alias)
        try:
            yield
        finally:
            await holder.stop()
            log.info("inference.stopped")

    app = FastAPI(
        title="Stock Intelligence Platform: inference",
        version="1.0.0",
        description=DESCRIPTION,
        lifespan=lifespan,
    )
    app.state.holder = holder
    app.state.metrics = metrics

    @app.exception_handler(RequestValidationError)
    async def invalid_request(request: Request, exc: RequestValidationError) -> JSONResponse:
        # FastAPI's default echoes the offending input, which fails to serialise
        # for NaN/Infinity (turning a 422 into a 500) and can be a 256-row batch.
        metrics.requests.labels("invalid").inc()
        errors = [
            {"loc": list(e.get("loc", ())), "msg": e.get("msg", ""), "type": e.get("type", "")}
            for e in exc.errors()
        ]
        return JSONResponse({"detail": errors[:20]}, status_code=422)

    def served() -> LoadedModel:
        model = holder.current
        if model is None:
            metrics.requests.labels("unavailable").inc()
            raise HTTPException(503, detail="no model loaded")
        return model

    @app.post("/predict", response_model=PredictResponse, tags=["inference"])
    def predict_endpoint(request: PredictRequest) -> PredictResponse:
        # A sync handler runs in the threadpool: model calls never block the event loop.
        model = served()
        try:
            events, elapsed = predict(request.instances, model, trace_id=request.trace_id)
        except InvalidRequestError as exc:
            metrics.requests.labels("invalid").inc()
            raise HTTPException(422, detail=str(exc)) from exc
        metrics.requests.labels("ok").inc()
        metrics.model_latency.observe(elapsed)
        metrics.batch_size.observe(len(events))
        metrics.predictions.labels(model.version).inc(len(events))
        return PredictResponse(model=model.info(), predictions=events)

    @app.get("/model", response_model=ModelInfo, tags=["inference"])
    def model_endpoint() -> ModelInfo:
        return served().info()

    @app.get("/health", tags=["operations"])
    def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/ready", tags=["operations"])
    def ready() -> JSONResponse:
        model = holder.current
        if model is None:
            return JSONResponse({"status": "unavailable", "model": None}, status_code=503)
        return JSONResponse({"status": "ready", "model": f"{model.name}/{model.version}"})

    @app.get("/metrics", include_in_schema=False)
    def metrics_endpoint(request: Request) -> Response:
        return Response(generate_latest(metrics.registry), media_type=CONTENT_TYPE_LATEST)

    return app


def main() -> None:
    log_settings = LogSettings()
    configure_logging("inference", level=log_settings.level, fmt=log_settings.format)
    settings = InferenceSettings()
    uvicorn.run(
        create_app(settings),
        host=settings.host,
        port=settings.port,
        log_config=None,
        access_log=False,
        timeout_graceful_shutdown=10,
    )


if __name__ == "__main__":
    main()

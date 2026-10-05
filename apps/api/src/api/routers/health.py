"""Liveness, readiness and Prometheus metrics."""

from __future__ import annotations

from fastapi import APIRouter, Request, Response
from fastapi.responses import JSONResponse
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from api.deps import Service
from api.schemas import Health, Readiness

router = APIRouter(tags=["operations"])


@router.get("/health", response_model=Health, summary="Liveness: the process is serving")
async def health() -> Health:
    return Health()


@router.get(
    "/ready",
    response_model=Readiness,
    summary="Readiness: dependencies reachable",
    responses={503: {"model": Readiness}},
)
async def ready(service: Service) -> JSONResponse:
    checks = await service.readiness()
    healthy = [v == "ok" for v in checks.values()]
    if all(healthy):
        status, code = "ready", 200
    elif any(healthy):
        # One store down: the API still serves (degraded), keep it in rotation.
        status, code = "degraded", 200
    else:
        status, code = "unavailable", 503
    body = Readiness(status=status, checks=checks)  # type: ignore[arg-type]
    return JSONResponse(body.model_dump(), status_code=code)


@router.get("/metrics", include_in_schema=False)
async def metrics(request: Request) -> Response:
    registry = request.app.state.metrics.registry
    return Response(generate_latest(registry), media_type=CONTENT_TYPE_LATEST)

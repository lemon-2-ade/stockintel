"""Model and pipeline status: which model is served, how it is doing, what is fresh."""

from __future__ import annotations

from fastapi import APIRouter

from api.deps import Service, Settings
from api.schemas import ModelStatus, MonitoringSummary, PipelineStatus

router = APIRouter(prefix="/api/v1", tags=["ml"])


@router.get(
    "/model",
    response_model=ModelStatus | None,
    summary="Current champion model (null when none is promoted)",
)
async def model(service: Service) -> ModelStatus | None:
    return await service.model_status()


@router.get(
    "/monitoring",
    response_model=MonitoringSummary | None,
    summary="Latest model-monitor cycle: drift, performance, retraining signal",
)
async def monitoring(service: Service) -> MonitoringSummary | None:
    return await service.monitoring()


@router.get(
    "/status",
    response_model=PipelineStatus,
    summary="End-to-end pipeline status: stores, data and prediction freshness, monitor",
)
async def status(service: Service, settings: Settings) -> PipelineStatus:
    return await service.pipeline_status(monitor_stale_after_s=settings.monitor_stale_after_s)

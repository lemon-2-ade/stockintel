"""Market data, analytics, anomalies and predictions."""

from __future__ import annotations

from datetime import datetime
from typing import Annotated

from fastapi import APIRouter, Query

from api.deps import Service, Settings, SymbolPath
from api.errors import ApiError
from api.schemas import (
    Anomaly,
    History,
    IndicatorPoint,
    Page,
    Prediction,
    StockDetail,
    StockSummary,
)
from shared.schemas import BarInterval

router = APIRouter(prefix="/api/v1", tags=["market"])

Before = Annotated[
    datetime | None, Query(description="Keyset cursor: only items strictly older than this")
]
Limit = Annotated[int, Query(ge=1, le=1_000)]


@router.get("/stocks", response_model=list[StockSummary], summary="All symbols with latest quote")
async def list_stocks(service: Service) -> list[StockSummary]:
    return await service.list_stocks()


@router.get(
    "/stocks/{symbol}",
    response_model=StockDetail,
    summary="Latest observation, analytics, session stats and prediction",
)
async def stock_detail(symbol: SymbolPath, service: Service) -> StockDetail:
    return await service.stock_detail(symbol)


@router.get("/stocks/{symbol}/history", response_model=History, summary="OHLCV bars")
async def history(
    symbol: SymbolPath,
    *,
    service: Service,
    settings: Settings,
    interval: BarInterval = BarInterval.S1,
    start: datetime | None = None,
    end: datetime | None = None,
    before: Before = None,
    limit: Annotated[int, Query(ge=1)] = 500,
) -> History:
    if limit > settings.history_max_limit:
        raise ApiError(f"limit must be <= {settings.history_max_limit}")
    if start and end and start >= end:
        raise ApiError("start must be before end")
    return await service.history(symbol, interval, start=start, end=end, before=before, limit=limit)


@router.get(
    "/stocks/{symbol}/indicators",
    response_model=Page[IndicatorPoint],
    summary="Indicator history (newest first)",
)
async def indicators(
    symbol: SymbolPath,
    *,
    service: Service,
    interval: BarInterval = BarInterval.S1,
    before: Before = None,
    limit: Limit = 200,
) -> Page[IndicatorPoint]:
    return await service.indicators(symbol, interval, before=before, limit=limit)


@router.get(
    "/stocks/{symbol}/anomalies", response_model=Page[Anomaly], summary="Anomalies for a symbol"
)
async def symbol_anomalies(
    symbol: SymbolPath, service: Service, before: Before = None, limit: Limit = 50
) -> Page[Anomaly]:
    return await service.anomalies(symbol, before=before, limit=limit)


@router.get(
    "/stocks/{symbol}/predictions",
    response_model=Page[Prediction],
    summary="Model predictions (estimates, not advice)",
)
async def predictions(
    symbol: SymbolPath, service: Service, before: Before = None, limit: Limit = 50
) -> Page[Prediction]:
    return await service.predictions(symbol, before=before, limit=limit)


@router.get("/anomalies", response_model=Page[Anomaly], summary="Recent anomalies, all symbols")
async def anomalies(service: Service, before: Before = None, limit: Limit = 50) -> Page[Anomaly]:
    return await service.anomalies(None, before=before, limit=limit)

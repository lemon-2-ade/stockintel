"""WebSocket endpoints: ``/ws/market?symbols=AAPL,MSFT`` and ``/ws/market/{symbol}``."""

from __future__ import annotations

import asyncio
import contextlib
from typing import Any

from fastapi import APIRouter, WebSocket, WebSocketDisconnect, status

from api.config import ApiSettings
from api.live import ClientConnection, LiveHub, parse_symbols
from api.metrics import ApiMetrics
from api.service import MarketService
from shared.observability.logs import get_logger

log = get_logger(__name__)
router = APIRouter()


@router.websocket("/ws/market")
async def market(websocket: WebSocket, symbols: str = "") -> None:
    await serve(websocket, symbols.split(","))


@router.websocket("/ws/market/{symbol}")
async def market_symbol(websocket: WebSocket, symbol: str) -> None:
    await serve(websocket, [symbol])


async def serve(websocket: WebSocket, requested: list[str]) -> None:
    state = websocket.app.state
    settings: ApiSettings = state.settings
    metrics: ApiMetrics = state.metrics
    hub: LiveHub | None = getattr(state, "hub", None)
    service: MarketService = state.service

    # Browsers do not apply CORS to WebSockets: enforce the origin allow-list here.
    origin = websocket.headers.get("origin")
    if origin and origin not in settings.cors_origins:
        await websocket.close(code=status.WS_1008_POLICY_VIOLATION, reason="origin not allowed")
        return
    if hub is None:
        await websocket.close(code=status.WS_1013_TRY_AGAIN_LATER, reason="live feed unavailable")
        return

    await websocket.accept()
    conn = ClientConnection(
        websocket,
        flush_interval_s=settings.ws_flush_interval_ms / 1000,
        heartbeat_s=settings.ws_heartbeat_s,
        max_pending_anomalies=settings.ws_max_pending_anomalies,
        metrics=metrics,
    )
    try:
        symbols = parse_symbols(requested, settings.ws_max_symbols)
    except ValueError as exc:
        await conn.send({"type": "error", "message": str(exc)})
        await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
        return

    metrics.ws_connections.inc()
    try:
        await _subscribe(conn, hub, service, symbols)
        sender = asyncio.create_task(conn.run_sender())
        receiver = asyncio.create_task(_receive(websocket, conn, hub, service, settings))
        done, pending = await asyncio.wait({sender, receiver}, return_when=asyncio.FIRST_COMPLETED)
        for task in pending:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await task
        for task in done:
            error = task.exception()
            if error is not None and not isinstance(error, WebSocketDisconnect):
                log.warning("ws.connection_error", error=str(error))
    finally:
        hub.unsubscribe(conn)
        metrics.ws_connections.dec()
        if conn.dropped_events:
            log.warning("ws.slow_client_dropped_events", dropped=conn.dropped_events)


async def _subscribe(
    conn: ClientConnection, hub: LiveHub, service: MarketService, symbols: list[str]
) -> None:
    hub.subscribe(conn, symbols)
    await conn.send({"type": "subscribed", "symbols": sorted(conn.symbols)})
    if symbols:
        snapshot = await service.live_snapshot(symbols)
        await conn.send({"type": "snapshot", "items": snapshot})


async def _receive(
    websocket: WebSocket,
    conn: ClientConnection,
    hub: LiveHub,
    service: MarketService,
    settings: ApiSettings,
) -> None:
    while True:
        message: Any = await websocket.receive_json()
        action = message.get("action") if isinstance(message, dict) else None
        if action == "ping":
            await conn.send({"type": "pong"})
            continue
        if action not in ("subscribe", "unsubscribe"):
            await conn.send({"type": "error", "message": "unknown action"})
            continue
        try:
            symbols = parse_symbols(message.get("symbols") or [], settings.ws_max_symbols)
        except (TypeError, ValueError) as exc:
            await conn.send({"type": "error", "message": str(exc)})
            continue
        if action == "unsubscribe":
            hub.unsubscribe(conn, symbols)
            await conn.send({"type": "subscribed", "symbols": sorted(conn.symbols)})
        elif len(conn.symbols | set(symbols)) > settings.ws_max_symbols:
            await conn.send(
                {"type": "error", "message": f"at most {settings.ws_max_symbols} symbols"}
            )
        else:
            await _subscribe(conn, hub, service, symbols)

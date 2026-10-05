"""Throttling, coalescing and fan-out of live updates (no Redis needed)."""

from __future__ import annotations

import asyncio
import json
from typing import Any

import pytest
from prometheus_client import CollectorRegistry

from api.live import ClientConnection, LiveHub, parse_symbols
from api.metrics import ApiMetrics

pytestmark = pytest.mark.anyio


class RecordingSocket:
    def __init__(self) -> None:
        self.frames: list[dict[str, Any]] = []

    async def send_text(self, data: str) -> None:
        self.frames.append(json.loads(data))


def bar(symbol: str, close: float) -> dict[str, Any]:
    return {"type": "bar", "symbol": symbol, "data": {"close": close}}


def anomaly(symbol: str, n: int) -> dict[str, Any]:
    return {"type": "anomaly", "symbol": symbol, "data": {"n": n}}


def connection(socket: RecordingSocket, metrics: ApiMetrics, **kw: Any) -> ClientConnection:
    options: dict[str, Any] = {
        "flush_interval_s": 0.05,
        "heartbeat_s": 10.0,
        "max_pending_anomalies": 3,
    } | kw
    return ClientConnection(socket, metrics=metrics, **options)


async def test_bars_are_coalesced_but_anomalies_are_not() -> None:
    metrics = ApiMetrics(CollectorRegistry())
    socket = RecordingSocket()
    conn = connection(socket, metrics)
    for i in range(100):
        conn.offer(bar("AAPL", 100 + i))
    conn.offer(anomaly("AAPL", 1))
    conn.offer(anomaly("AAPL", 2))
    conn.offer(bar("MSFT", 50))

    items = conn.drain()
    bars = {i["symbol"]: i["data"]["close"] for i in items if i["type"] == "bar"}
    assert bars == {"AAPL": 199, "MSFT": 50}, "only the latest bar per symbol survives"
    assert [i["data"]["n"] for i in items if i["type"] == "anomaly"] == [1, 2]
    assert metrics.registry.get_sample_value("sip_api_websocket_coalesced_total") == 99


async def test_anomaly_queue_is_bounded_for_slow_clients() -> None:
    conn = connection(RecordingSocket(), ApiMetrics(CollectorRegistry()))
    for n in range(10):
        conn.offer(anomaly("AAPL", n))
    assert [i["data"]["n"] for i in conn.drain()] == [7, 8, 9]
    assert conn.dropped_events == 7


async def test_sender_flushes_at_most_once_per_interval() -> None:
    socket = RecordingSocket()
    conn = connection(socket, ApiMetrics(CollectorRegistry()), flush_interval_s=0.2)
    sender = asyncio.create_task(conn.run_sender())
    try:
        for i in range(20):  # 20 updates over ~0.4 s
            conn.offer(bar("AAPL", i))
            await asyncio.sleep(0.02)
        await asyncio.sleep(0.25)
    finally:
        sender.cancel()
    updates = [f for f in socket.frames if f["type"] == "updates"]
    assert 2 <= len(updates) <= 4, f"expected ~3 frames, got {len(updates)}"
    assert updates[-1]["items"][-1]["data"]["close"] == 19, "the newest value is never lost"


async def test_heartbeat_when_idle() -> None:
    socket = RecordingSocket()
    conn = connection(socket, ApiMetrics(CollectorRegistry()), heartbeat_s=0.05)
    sender = asyncio.create_task(conn.run_sender())
    await asyncio.sleep(0.12)
    sender.cancel()
    assert {f["type"] for f in socket.frames} == {"heartbeat"}


async def test_hub_routes_by_symbol() -> None:
    metrics = ApiMetrics(CollectorRegistry())
    hub = LiveHub(client=None, metrics=metrics)  # type: ignore[arg-type]
    a, b = connection(RecordingSocket(), metrics), connection(RecordingSocket(), metrics)
    hub.subscribe(a, ["AAPL"])
    hub.subscribe(b, ["AAPL", "MSFT"])
    assert hub.dispatch("market:AAPL", json.dumps(bar("AAPL", 1))) == 2
    assert hub.dispatch("market:MSFT", json.dumps(bar("MSFT", 1))) == 1
    assert hub.dispatch("market:TSLA", json.dumps(bar("TSLA", 1))) == 0
    assert hub.dispatch("market:AAPL", "not json") == 0
    assert len(a.drain()) == 1
    assert len(b.drain()) == 2
    hub.unsubscribe(b)
    assert hub.connections == 1
    assert b.symbols == set()


def test_parse_symbols() -> None:
    assert parse_symbols(["AAPL", " MSFT", "AAPL", ""], 5) == ["AAPL", "MSFT"]
    with pytest.raises(ValueError, match="invalid"):
        parse_symbols(["aapl"], 5)
    with pytest.raises(ValueError, match="at most"):
        parse_symbols(["A", "B", "C"], 2)

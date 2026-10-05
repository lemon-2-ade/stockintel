"""API against real PostgreSQL + Redis, seeded through the real sinks (no Kafka needed)."""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta

import pytest
import redis
from fastapi.testclient import TestClient
from sqlalchemy import Engine

from api.config import ApiSettings
from api.main import create_app
from shared.schemas import (
    AnomalyEvent,
    AnomalyType,
    BarInterval,
    Direction,
    EnrichedBarEvent,
    IndicatorSnapshot,
    MarketBarEvent,
    PredictionEvent,
    PredictionTask,
    Severity,
    deterministic_prediction_id,
)
from sinks.postgres_sink import PostgresSink
from sinks.redis_sink import RedisCacheSink

pytestmark = pytest.mark.integration
N_BARS = 30


@pytest.fixture
def seeded(migrated_engine: Engine, redis_client: redis.Redis) -> str:
    symbol = "Y" + uuid.uuid4().hex[:7].upper()
    now = datetime.now(UTC).replace(microsecond=0)
    start = now - timedelta(minutes=N_BARS)
    raw = [
        MarketBarEvent(
            source="test",
            symbol=symbol,
            timestamp=start + timedelta(minutes=i),
            interval=BarInterval.M1,
            open=100 + i,
            high=101 + i,
            low=99 + i,
            close=100.5 + i,
            volume=1_000 + i,
        )
        for i in range(N_BARS)
    ]
    enriched = [
        EnrichedBarEvent.from_bar(
            b,
            indicators=IndicatorSnapshot(sma_20=float(i), return_1=0.01),
            indicator_version="1.0.0",
            source="test",
        )
        for i, b in enumerate(raw)
    ]
    last = raw[-1]
    anomaly = AnomalyEvent(
        source="test",
        symbol=symbol,
        timestamp=last.timestamp,
        interval=BarInterval.M1,
        anomaly_type=AnomalyType.PRICE_SPIKE,
        severity=Severity.MEDIUM,
        observed_value=0.02,
        expected_value=0.0,
        score=0.02,
        threshold=0.01,
        detector="return_threshold",
        detector_version="1.0.0",
        source_event_id=last.event_id,
    )
    prediction = PredictionEvent(
        source="test",
        prediction_id=deterministic_prediction_id(last.event_id, "m", "1", 5),
        symbol=symbol,
        timestamp=last.timestamp,
        interval=BarInterval.M1,
        horizon_bars=5,
        target_timestamp=last.timestamp + timedelta(minutes=10),
        task=PredictionTask.DIRECTION,
        predicted_direction=Direction.UP,
        class_probabilities={Direction.UP: 0.55, Direction.DOWN: 0.45},
        confidence=0.55,
        model_name="m",
        model_version="1",
        feature_set_version="fs1",
        source_event_id=last.event_id,
    )
    events = [*raw, *enriched, anomaly, prediction]
    PostgresSink(migrated_engine).write(events)
    RedisCacheSink(redis_client).write(events)
    return symbol


@pytest.fixture
def client() -> Iterator[TestClient]:
    with TestClient(create_app(ApiSettings(rate_limit_burst=10_000))) as test_client:
        yield test_client


def test_health_and_readiness(client: TestClient) -> None:
    assert client.get("/health").json() == {"status": "ok"}
    ready = client.get("/ready")
    assert ready.status_code == 200
    assert ready.json() == {"status": "ready", "checks": {"postgres": "ok", "redis": "ok"}}


def test_stock_list_and_detail(client: TestClient, seeded: str) -> None:
    listing = {s["symbol"]: s for s in client.get("/api/v1/stocks").json()}
    assert listing[seeded]["last_price"] == 100.5 + N_BARS - 1
    assert listing[seeded]["change_pct"] == pytest.approx(1.0)

    detail = client.get(f"/api/v1/stocks/{seeded}").json()
    assert detail["observation"]["close"] == 100.5 + N_BARS - 1
    assert detail["analytics"]["sma_20"] == N_BARS - 1
    assert detail["freshness"]["source"] == "cache"
    assert detail["session"]["bars"] >= 1
    prediction = detail["prediction"]
    assert prediction["kind"] == "model_estimate"
    assert prediction["model_version"] == "1"
    assert "Not investment advice" in prediction["disclaimer"]


def test_history_keyset_pagination(client: TestClient, seeded: str) -> None:
    seen: list[str] = []
    before = None
    for _ in range(10):
        params: dict[str, str | int] = {"interval": "1m", "limit": 12}
        if before:
            params["before"] = before
        page = client.get(f"/api/v1/stocks/{seeded}/history", params=params).json()
        stamps = [b["timestamp"] for b in page["bars"]]
        assert stamps == sorted(stamps), "each page is ascending"
        seen = stamps + seen
        before = page["next_before"]
        if before is None:
            break
    assert len(seen) == N_BARS
    assert len(set(seen)) == N_BARS, "no overlaps or gaps between pages"


def test_indicators_anomalies_predictions(client: TestClient, seeded: str) -> None:
    ind = client.get(f"/api/v1/stocks/{seeded}/indicators", params={"interval": "1m"}).json()
    assert len(ind["items"]) == N_BARS
    assert ind["items"][0]["values"]["sma_20"] == N_BARS - 1  # newest first

    anomalies = client.get(f"/api/v1/stocks/{seeded}/anomalies").json()["items"]
    assert [a["anomaly_type"] for a in anomalies] == ["price_spike"]
    feed = client.get("/api/v1/anomalies", params={"limit": 500}).json()["items"]
    assert any(a["symbol"] == seeded for a in feed)

    predictions = client.get(f"/api/v1/stocks/{seeded}/predictions").json()["items"]
    assert predictions[0]["class_probabilities"] == {"up": 0.55, "down": 0.45}


def test_errors_are_structured(client: TestClient) -> None:
    missing = client.get("/api/v1/stocks/NOPE123", headers={"X-Request-ID": "req-42"})
    assert missing.status_code == 404
    assert missing.headers["x-request-id"] == "req-42"
    assert missing.json()["error"] == {
        "code": "not_found",
        "message": "unknown symbol 'NOPE123'",
        "request_id": "req-42",
    }
    invalid = client.get("/api/v1/stocks/not-a-ticker")
    assert invalid.status_code == 422
    assert invalid.json()["error"]["code"] == "validation_error"
    too_big = client.get("/api/v1/stocks/AAPL/history", params={"limit": 999_999})
    assert too_big.status_code in (400, 404)


def test_watchlist_lifecycle(client: TestClient, seeded: str) -> None:
    headers = {"X-User-Id": f"user-{uuid.uuid4().hex[:8]}"}
    assert client.get("/api/v1/watchlist", headers=headers).json() == []
    assert (
        client.post("/api/v1/watchlist", json={"symbol": seeded}, headers=headers).status_code
        == 201
    )
    assert (
        client.post("/api/v1/watchlist", json={"symbol": seeded}, headers=headers).status_code
        == 200
    )
    items = client.get("/api/v1/watchlist", headers=headers).json()
    assert [i["symbol"] for i in items] == [seeded]
    assert items[0]["quote"]["last_price"] is not None
    assert (
        client.post("/api/v1/watchlist", json={"symbol": "NOPE999"}, headers=headers).status_code
        == 404
    )
    assert client.delete(f"/api/v1/watchlist/{seeded}", headers=headers).status_code == 204
    assert client.delete(f"/api/v1/watchlist/{seeded}", headers=headers).status_code == 404


def test_cache_outage_degrades_to_database(seeded: str, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("REDIS_PORT", "1")  # nothing listens there
    with TestClient(create_app(ApiSettings())) as degraded:
        detail = degraded.get(f"/api/v1/stocks/{seeded}")
        assert detail.status_code == 200
        body = detail.json()
        assert body["freshness"]["source"] == "database"
        assert body["analytics"]["sma_20"] == N_BARS - 1
        ready = degraded.get("/ready").json()
        assert ready["status"] == "degraded"
        assert ready["checks"]["postgres"] == "ok"


def test_metrics_use_route_templates(client: TestClient, seeded: str) -> None:
    client.get(f"/api/v1/stocks/{seeded}")
    text = client.get("/metrics").text
    assert 'route="/api/v1/stocks/{symbol}"' in text
    assert seeded not in text, "raw paths must never become label values"


def test_websocket_streams_throttled_updates(
    client: TestClient, seeded: str, redis_client: redis.Redis
) -> None:
    with client.websocket_connect(f"/ws/market?symbols={seeded}") as ws:
        assert ws.receive_json() == {"type": "subscribed", "symbols": [seeded]}
        snapshot = ws.receive_json()
        assert snapshot["type"] == "snapshot"
        assert snapshot["items"][0]["data"]["close"] == 100.5 + N_BARS - 1

        # Five newer bars arrive in a burst: the client gets them coalesced.
        sink = RedisCacheSink(redis_client)
        base = datetime.now(UTC).replace(microsecond=0) + timedelta(minutes=1)
        for i in range(5):
            bar = MarketBarEvent(
                source="test",
                symbol=seeded,
                timestamp=base + timedelta(minutes=i),
                interval=BarInterval.M1,
                open=200,
                high=210,
                low=199,
                close=200 + i,
                volume=1,
            )
            sink.write(
                [
                    EnrichedBarEvent.from_bar(
                        bar, indicators=IndicatorSnapshot(), indicator_version="1", source="t"
                    )
                ]
            )
        received: list[float] = []
        while not received or received[-1] != 204:
            frame = ws.receive_json()
            if frame["type"] == "updates":
                received += [i["data"]["close"] for i in frame["items"]]
        assert len(received) < 5, "burst was coalesced"
        assert received[-1] == 204

        ws.send_json({"action": "ping"})
        while (frame := ws.receive_json())["type"] != "pong":
            pass


def test_websocket_rejects_bad_input_and_foreign_origins(client: TestClient) -> None:
    with client.websocket_connect("/ws/market?symbols=bad-symbol") as ws:
        assert ws.receive_json()["type"] == "error"
    from starlette.websockets import WebSocketDisconnect  # noqa: PLC0415

    with (
        pytest.raises(WebSocketDisconnect),
        client.websocket_connect("/ws/market/AAPL", headers={"Origin": "http://evil.example"}),
    ):
        pass

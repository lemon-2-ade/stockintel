"""HTTP behaviour that needs no storage: errors, request ids, rate limiting, CORS."""

from __future__ import annotations

from typing import Any

from fastapi.testclient import TestClient

from api.config import ApiSettings
from api.errors import NotFoundError
from api.main import create_app
from api.middleware import TokenBucketLimiter


class StubService:
    async def list_stocks(self) -> list[Any]:
        return []

    async def stock_detail(self, symbol: str) -> Any:
        raise NotFoundError(f"unknown symbol {symbol!r}")

    async def readiness(self) -> dict[str, str]:
        return {"postgres": "error: OperationalError", "redis": "error: ConnectionError"}


def client(**settings: Any) -> TestClient:
    app = create_app(ApiSettings(**settings), service=StubService())  # type: ignore[arg-type]
    return TestClient(app)


def test_request_id_is_generated_and_echoed() -> None:
    with client() as c:
        generated = c.get("/api/v1/stocks").headers["x-request-id"]
        assert len(generated) == 32
        assert c.get("/health", headers={"X-Request-ID": "abc"}).headers["x-request-id"] == "abc"


def test_not_found_shape() -> None:
    with client() as c:
        body = c.get("/api/v1/stocks/AAPL").json()
        assert body["error"]["code"] == "not_found"
        assert body["error"]["request_id"]


def test_readiness_reports_unavailable_with_503() -> None:
    with client() as c:
        response = c.get("/ready")
        assert response.status_code == 503
        assert response.json()["status"] == "unavailable"


def test_rate_limit_returns_429_with_retry_after() -> None:
    with client(rate_limit_per_s=1, rate_limit_burst=3) as c:
        codes = [c.get("/api/v1/stocks").status_code for _ in range(5)]
        assert codes[:3] == [200, 200, 200]
        assert codes[3:] == [429, 429]
        limited = c.get("/api/v1/stocks")
        assert limited.headers["retry-after"] == "1"
        assert limited.json()["error"]["code"] == "rate_limited"
        assert c.get("/health").status_code == 200, "probes are never rate limited"


def test_cors_allows_configured_origin_only() -> None:
    with client(cors_origins="http://dash.local") as c:
        ok = c.get("/api/v1/stocks", headers={"Origin": "http://dash.local"})
        assert ok.headers["access-control-allow-origin"] == "http://dash.local"
        other = c.get("/api/v1/stocks", headers={"Origin": "http://evil.example"})
        assert "access-control-allow-origin" not in other.headers


def test_openapi_documents_versioned_routes() -> None:
    with client() as c:
        paths = c.get("/openapi.json").json()["paths"]
        assert "/api/v1/stocks/{symbol}/history" in paths
        assert "/api/v1/watchlist/{symbol}" in paths


def test_token_bucket_refills() -> None:
    now = [0.0]
    limiter = TokenBucketLimiter(rate_per_s=2, burst=2, clock=lambda: now[0])
    assert limiter.acquire("a") is None
    assert limiter.acquire("a") is None
    assert limiter.acquire("a") == 0.5
    assert limiter.acquire("b") is None, "keys are independent"
    now[0] = 0.5
    assert limiter.acquire("a") is None

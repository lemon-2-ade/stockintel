"""Request context, metrics and rate limiting as pure ASGI middleware."""

from __future__ import annotations

import time
import uuid
from collections import OrderedDict
from collections.abc import Callable
from dataclasses import dataclass

import structlog
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from api.metrics import ApiMetrics
from shared.observability.logs import get_logger

log = get_logger("api.access")

REQUEST_ID_HEADER = "x-request-id"
UNLIMITED_PATHS = ("/health", "/ready", "/metrics")


def _route_template(scope: Scope) -> str:
    route = scope.get("route")
    path = getattr(route, "path", None)
    return path if isinstance(path, str) else "unmatched"


class RequestContextMiddleware:
    """Request id (accepted from the client or generated), access log, metrics."""

    def __init__(self, app: ASGIApp, metrics: ApiMetrics) -> None:
        self.app = app
        self.metrics = metrics

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        headers = dict(scope.get("headers") or [])
        incoming = headers.get(REQUEST_ID_HEADER.encode(), b"").decode()[:64]
        request_id = incoming or uuid.uuid4().hex
        scope.setdefault("state", {})["request_id"] = request_id
        structlog.contextvars.bind_contextvars(request_id=request_id)

        status = 500
        started = time.perf_counter()

        async def send_wrapper(message: Message) -> None:
            nonlocal status
            if message["type"] == "http.response.start":
                status = message["status"]
                message.setdefault("headers", []).append(
                    (REQUEST_ID_HEADER.encode(), request_id.encode())
                )
            await send(message)

        self.metrics.in_flight.inc()
        try:
            await self.app(scope, receive, send_wrapper)
        finally:
            elapsed = time.perf_counter() - started
            route = _route_template(scope)
            method = scope["method"]
            self.metrics.in_flight.dec()
            self.metrics.requests.labels(method, route, str(status)).inc()
            self.metrics.latency.labels(method, route).observe(elapsed)
            if route not in UNLIMITED_PATHS:
                log.info(
                    "http.request",
                    method=method,
                    route=route,
                    status=status,
                    duration_ms=round(elapsed * 1000, 2),
                )
            structlog.contextvars.unbind_contextvars("request_id")


@dataclass(slots=True)
class _Bucket:
    tokens: float
    updated: float


class TokenBucketLimiter:
    """Per-key token bucket. In-process: with N API replicas the effective limit is
    N x rate; a shared limiter (Redis) would be needed for a global guarantee."""

    def __init__(
        self,
        rate_per_s: float,
        burst: int,
        *,
        max_keys: int = 10_000,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.rate = rate_per_s
        self.burst = burst
        self._buckets: OrderedDict[str, _Bucket] = OrderedDict()
        self._max_keys = max_keys
        self._clock = clock

    def acquire(self, key: str) -> float | None:
        """Take a token; return None if allowed, else seconds until the next token."""
        now = self._clock()
        bucket = self._buckets.get(key)
        if bucket is None:
            bucket = _Bucket(tokens=float(self.burst), updated=now)
            self._buckets[key] = bucket
            if len(self._buckets) > self._max_keys:
                self._buckets.popitem(last=False)
        else:
            bucket.tokens = min(self.burst, bucket.tokens + (now - bucket.updated) * self.rate)
            bucket.updated = now
            self._buckets.move_to_end(key)
        if bucket.tokens >= 1:
            bucket.tokens -= 1
            return None
        return (1 - bucket.tokens) / self.rate


class RateLimitMiddleware:
    def __init__(self, app: ASGIApp, limiter: TokenBucketLimiter, metrics: ApiMetrics) -> None:
        self.app = app
        self.limiter = limiter
        self.metrics = metrics

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http" or scope["path"].startswith(UNLIMITED_PATHS):
            await self.app(scope, receive, send)
            return
        request = Request(scope)
        client = request.client.host if request.client else "unknown"
        retry_after = self.limiter.acquire(client)
        if retry_after is None:
            await self.app(scope, receive, send)
            return
        self.metrics.rate_limited.inc()
        response: Response = JSONResponse(
            {"error": {"code": "rate_limited", "message": "too many requests"}},
            status_code=429,
            headers={"Retry-After": str(max(1, round(retry_after)))},
        )
        await response(scope, receive, send)

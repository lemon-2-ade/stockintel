"""Application factory and entry point (``python -m api.main``)."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api.config import ApiSettings
from api.errors import install_error_handlers
from api.live import LiveHub
from api.metrics import ApiMetrics
from api.middleware import RateLimitMiddleware, RequestContextMiddleware, TokenBucketLimiter
from api.routers import health, stocks, watchlist, ws
from api.service import MarketService
from shared.config import LogSettings, PostgresSettings, RedisSettings
from shared.observability.logs import configure_logging, get_logger

log = get_logger(__name__)

DESCRIPTION = """
Real-time market data, deterministic analytics, anomaly detections and model
predictions for the Stock Intelligence Platform.

Responses keep **observations**, **analytics**, **anomalies** and
**predictions** in separate fields. Predictions are statistical estimates,
not investment advice.
"""


def create_app(
    settings: ApiSettings | None = None,
    *,
    service: MarketService | None = None,
    metrics: ApiMetrics | None = None,
    hub: LiveHub | None = None,
) -> FastAPI:
    """Build the app. Passing ``service`` (and optionally ``hub``) skips connecting
    to Postgres/Redis, which is how unit tests run without infrastructure."""
    settings = settings or ApiSettings()
    metrics = metrics or ApiMetrics()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        if service is not None:
            app.state.service = service
            app.state.hub = hub
            yield
            return

        import redis.asyncio as aioredis  # noqa: PLC0415

        from api.repositories.cache import CacheRepository  # noqa: PLC0415
        from api.repositories.postgres import MarketRepository  # noqa: PLC0415
        from shared.db.session import create_async_db_engine  # noqa: PLC0415

        engine = create_async_db_engine(PostgresSettings(), application_name="api")
        redis_client = aioredis.Redis(**RedisSettings().client_kwargs())  # type: ignore[arg-type]
        app.state.redis = redis_client
        app.state.service = MarketService(
            MarketRepository(engine),
            CacheRepository(redis_client),
            stale_after_bars=settings.stale_after_bars,
        )
        live_hub = LiveHub(redis_client, metrics)
        live_hub.start()
        app.state.hub = live_hub
        log.info("api.started")
        try:
            yield
        finally:
            await live_hub.stop()
            await redis_client.aclose()
            await engine.dispose()
            log.info("api.stopped")

    app = FastAPI(
        title="Stock Intelligence Platform API",
        version="1.0.0",
        description=DESCRIPTION,
        lifespan=lifespan,
    )
    app.state.settings = settings
    app.state.metrics = metrics

    install_error_handlers(app)
    app.include_router(health.router)
    app.include_router(stocks.router)
    app.include_router(watchlist.router)
    app.include_router(ws.router)

    # Order: the last added runs first. Request context wraps everything.
    app.add_middleware(
        RateLimitMiddleware,
        limiter=TokenBucketLimiter(settings.rate_limit_per_s, settings.rate_limit_burst),
        metrics=metrics,
    )
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_origins,
        allow_methods=["GET", "POST", "DELETE"],
        allow_headers=["Content-Type", "X-User-Id", "X-Request-ID"],
        expose_headers=["X-Request-ID", "Retry-After"],
        max_age=600,
    )
    app.add_middleware(RequestContextMiddleware, metrics=metrics)
    return app


def main() -> None:
    log_settings = LogSettings()
    configure_logging("api", level=log_settings.level, fmt=log_settings.format)
    settings = ApiSettings()
    uvicorn.run(
        create_app(settings),
        host=settings.host,
        port=settings.port,
        log_config=None,  # structlog handles logging
        access_log=False,  # RequestContextMiddleware emits structured access logs
        proxy_headers=True,
        timeout_graceful_shutdown=10,
    )


if __name__ == "__main__":
    main()

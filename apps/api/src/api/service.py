"""Use cases and degradation rules.

* **Latest state** comes from Redis; if Redis is down or cold, from Postgres.
* **History** comes from Postgres only; if Postgres is down, those endpoints
  answer 503 while cache-backed endpoints keep working.
* **Predictions** are optional: no fresh prediction (or the ML path being
  down) yields ``prediction: null``, never an error, so the dashboard keeps
  showing deterministic analytics.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from datetime import UTC, datetime, timedelta
from typing import Any, TypeVar

import redis
from sqlalchemy.exc import DBAPIError, SQLAlchemyError

from api.errors import NotFoundError, ServiceUnavailableError
from api.repositories.cache import CacheRepository
from api.repositories.postgres import MarketRepository, Row
from api.schemas import (
    Anomaly,
    Bar,
    Freshness,
    History,
    IndicatorPoint,
    ModelStatus,
    MonitoringItem,
    MonitoringSummary,
    Page,
    PipelineStatus,
    Prediction,
    PredictionOutcome,
    SessionStats,
    StageStatus,
    StockDetail,
    StockSummary,
    WatchlistItem,
)
from shared.observability.logs import get_logger
from shared.schemas import BarInterval, IndicatorSnapshot, utcnow

log = get_logger(__name__)
T = TypeVar("T")

CACHE_ERRORS: tuple[type[BaseException], ...] = (redis.RedisError, OSError, TimeoutError)
DB_ERRORS: tuple[type[BaseException], ...] = (SQLAlchemyError, DBAPIError, OSError, TimeoutError)


def _bar_from_payload(p: Mapping[str, Any]) -> Bar:
    return Bar(
        timestamp=p["timestamp"],
        interval=p["interval"],
        open=p["open"],
        high=p["high"],
        low=p["low"],
        close=p["close"],
        volume=p["volume"],
    )


def _bar_from_row(r: Row) -> Bar:
    return Bar(
        timestamp=r["ts"],
        interval=r["bar_interval"],
        open=r["open"],
        high=r["high"],
        low=r["low"],
        close=r["close"],
        volume=r["volume"],
    )


def _anomaly(source: Mapping[str, Any]) -> Anomaly:
    return Anomaly(
        anomaly_id=source["anomaly_id"],
        symbol=source["symbol"],
        timestamp=source.get("timestamp", source.get("ts")),
        interval=source.get("interval", source.get("bar_interval")),
        anomaly_type=source["anomaly_type"],
        severity=source["severity"],
        observed_value=source["observed_value"],
        expected_value=source.get("expected_value"),
        score=source["score"],
        threshold=source.get("threshold"),
        detector=source["detector"],
        detector_version=source["detector_version"],
    )


def _outcome(source: Mapping[str, Any]) -> PredictionOutcome | None:
    if source.get("actual_direction") is None:
        return None
    return PredictionOutcome(
        actual_direction=source["actual_direction"],
        actual_return=source["actual_return"],
        is_correct=source.get("is_correct"),
        resolved_at=source["resolved_at"],
    )


def _prediction(source: Mapping[str, Any]) -> Prediction:
    return Prediction(
        prediction_id=source["prediction_id"],
        timestamp=source.get("timestamp", source.get("ts")),
        target_timestamp=source.get("target_timestamp", source.get("target_ts")),
        horizon_bars=source["horizon_bars"],
        task=source["task"],
        predicted_direction=source.get("predicted_direction"),
        predicted_return=source.get("predicted_return"),
        class_probabilities=source.get("class_probabilities"),
        confidence=source.get("confidence"),
        model_name=source["model_name"],
        model_version=source["model_version"],
        outcome=_outcome(source),
    )


def _model_status(row: Mapping[str, Any]) -> ModelStatus:
    metrics: Mapping[str, Any] = row.get("metrics") or {}
    evaluation = {}
    for key, value in (metrics.get("evaluation") or {}).items():
        try:
            evaluation[str(key).removeprefix("eval.")] = float(value)
        except (TypeError, ValueError):
            continue
    return ModelStatus(
        name=row["model_name"],
        version=row["model_version"],
        promoted_at=row["created_at"],
        promoted_by=row["actor"],
        reason=row["reason"],
        overridden_gates=list(metrics.get("overridden_gates") or []),
        evaluation=evaluation,
    )


class MarketService:
    def __init__(
        self,
        db: MarketRepository,
        cache: CacheRepository,
        *,
        stale_after_bars: int = 5,
        clock: Callable[[], datetime] = utcnow,
    ) -> None:
        self._db = db
        self._cache = cache
        self._stale_after_bars = stale_after_bars
        self._clock = clock

    # ------------------------------------------------------------- helpers
    async def _from_cache(self, op: Callable[[], Awaitable[T]], what: str) -> T | None:
        try:
            return await op()
        except CACHE_ERRORS as exc:
            log.warning("cache.unavailable", operation=what, error=str(exc))
            return None

    async def _from_db(self, op: Callable[[], Awaitable[T]], what: str) -> T:
        try:
            return await op()
        except DB_ERRORS as exc:
            log.error("db.unavailable", operation=what, error=str(exc))
            raise ServiceUnavailableError("historical data store is unavailable") from exc

    async def _optional_db(self, op: Callable[[], Awaitable[T]], what: str) -> T | None:
        try:
            return await self._from_db(op, what)
        except ServiceUnavailableError:
            return None

    def _freshness(self, bar: Bar, source: str) -> Freshness:
        interval = bar.interval.duration
        as_of = bar.timestamp + interval
        age = (self._clock() - as_of).total_seconds()
        stale_after = max(self._stale_after_bars * interval.total_seconds(), 10.0)
        return Freshness(
            as_of=as_of,
            age_seconds=round(age, 3),
            stale=age > stale_after,
            source=source,  # type: ignore[arg-type]
        )

    async def require_symbol(self, symbol: str) -> str | None:
        """Return the symbol's display name; 404 for symbols the platform has never seen."""
        try:
            row = await self._from_db(lambda: self._db.symbol(symbol), "symbol")
        except ServiceUnavailableError:
            # Degraded mode: trust the cache's view of active symbols.
            active = await self._from_cache(self._cache.active_symbols, "active_symbols")
            if active is None:
                raise
            if symbol in active:
                return None
            raise NotFoundError(f"unknown symbol {symbol!r}") from None
        if row is None:
            raise NotFoundError(f"unknown symbol {symbol!r}")
        name: str | None = row["name"]
        return name

    # ------------------------------------------------------------- use cases
    async def list_stocks(self) -> list[StockSummary]:
        rows = await self._optional_db(self._db.symbols, "symbols")
        names: dict[str, str | None] = {r["symbol"]: r["name"] for r in rows or []}
        active = await self._from_cache(self._cache.active_symbols, "active_symbols")
        if rows is None and active is None:
            raise ServiceUnavailableError("neither the database nor the cache is reachable")
        symbols = sorted(set(names) | (active or set()))

        latest = await self._from_cache(lambda: self._cache.latest_bars(symbols), "latest_bars")
        if latest is None:
            db_rows = await self._optional_db(lambda: self._db.latest_bars(symbols), "latest")
            fallback = {r["symbol"]: r for r in db_rows or []}
            return [self._summary_from_row(s, names.get(s), fallback.get(s)) for s in symbols]
        return [self._summary_from_payload(s, names.get(s), latest.get(s)) for s in symbols]

    def _summary_from_payload(
        self, symbol: str, name: str | None, payload: Mapping[str, Any] | None
    ) -> StockSummary:
        if payload is None:
            return StockSummary(symbol=symbol, name=name)
        bar = _bar_from_payload(payload)
        indicators = payload.get("indicators") or {}
        return StockSummary(
            symbol=symbol,
            name=name,
            last_price=bar.close,
            change=indicators.get("price_change"),
            change_pct=(
                indicators["return_1"] * 100 if indicators.get("return_1") is not None else None
            ),
            volume=bar.volume,
            timestamp=bar.timestamp,
            stale=self._freshness(bar, "cache").stale,
        )

    def _summary_from_row(self, symbol: str, name: str | None, row: Row | None) -> StockSummary:
        if row is None:
            return StockSummary(symbol=symbol, name=name)
        bar = _bar_from_row(row)
        return StockSummary(
            symbol=symbol,
            name=name,
            last_price=bar.close,
            volume=bar.volume,
            timestamp=bar.timestamp,
            stale=self._freshness(bar, "database").stale,
        )

    async def stock_detail(self, symbol: str) -> StockDetail:
        name = await self.require_symbol(symbol)
        payload = await self._from_cache(lambda: self._cache.latest_bar(symbol), "latest_bar")
        analytics: IndicatorSnapshot | None
        if payload is not None:
            bar, source = _bar_from_payload(payload), "cache"
            analytics = IndicatorSnapshot.model_validate(payload.get("indicators") or {})
        else:
            row = await self._from_db(lambda: self._db.latest_bar(symbol), "latest_bar")
            if row is None:
                raise NotFoundError(f"no market data for {symbol!r} yet")
            bar, source = _bar_from_row(row), "database"
            ind = await self._optional_db(
                lambda: self._db.latest_indicators(symbol, bar.interval.value), "indicators"
            )
            analytics = (
                IndicatorSnapshot.model_validate(ind["indicators"])
                if ind and ind["ts"] == bar.timestamp
                else None
            )

        return StockDetail(
            symbol=symbol,
            name=name,
            observation=bar,
            analytics=analytics,
            session=await self._session(symbol, bar.interval),
            prediction=await self._latest_prediction(symbol),
            freshness=self._freshness(bar, source),
        )

    async def _session(self, symbol: str, interval: BarInterval) -> SessionStats | None:
        now = self._clock()
        midnight = datetime(now.year, now.month, now.day, tzinfo=UTC)
        since = midnight - timedelta(days=365) if interval is BarInterval.D1 else midnight
        stats = await self._optional_db(
            lambda: self._db.session_stats(symbol, interval.value, since), "session"
        )
        if stats is None:
            return None
        change = stats["close"] - stats["open"]
        return SessionStats(
            open=stats["open"],
            high=stats["high"],
            low=stats["low"],
            volume=int(stats["volume"]),
            change=change,
            change_pct=change / stats["open"] * 100,
            bars=stats["bars"],
        )

    async def _latest_prediction(self, symbol: str) -> Prediction | None:
        payload = await self._from_cache(
            lambda: self._cache.latest_prediction(symbol), "latest_prediction"
        )
        if payload is not None:
            return _prediction(payload)
        row = await self._optional_db(
            lambda: self._db.latest_open_prediction(symbol, self._clock()), "prediction"
        )
        return _prediction(row) if row else None

    async def history(
        self,
        symbol: str,
        interval: BarInterval,
        *,
        start: datetime | None,
        end: datetime | None,
        before: datetime | None,
        limit: int,
    ) -> History:
        await self.require_symbol(symbol)
        rows = await self._from_db(
            lambda: self._db.bars(
                symbol, interval.value, start=start, end=end, before=before, limit=limit
            ),
            "history",
        )
        bars = [_bar_from_row(r) for r in reversed(rows)]
        return History(
            symbol=symbol,
            interval=interval,
            bars=bars,
            next_before=bars[0].timestamp if len(rows) == limit else None,
        )

    async def indicators(
        self, symbol: str, interval: BarInterval, *, before: datetime | None, limit: int
    ) -> Page[IndicatorPoint]:
        await self.require_symbol(symbol)
        rows = await self._from_db(
            lambda: self._db.indicators(symbol, interval.value, before=before, limit=limit),
            "indicators",
        )
        items = [
            IndicatorPoint(
                timestamp=r["ts"],
                indicator_version=r["indicator_version"],
                values=IndicatorSnapshot.model_validate(r["indicators"]),
            )
            for r in rows
        ]
        return Page(items=items, next_before=items[-1].timestamp if len(rows) == limit else None)

    async def anomalies(
        self, symbol: str | None, *, before: datetime | None, limit: int
    ) -> Page[Anomaly]:
        if symbol is not None:
            await self.require_symbol(symbol)
        if before is None:  # live feed: newest first, straight from the cache
            cached = await self._from_cache(
                lambda: self._cache.recent_anomalies(symbol, limit), "recent_anomalies"
            )
            if cached:
                items = [_anomaly(a) for a in cached]
                return Page(
                    items=items, next_before=items[-1].timestamp if len(items) == limit else None
                )
        rows = await self._from_db(
            lambda: self._db.anomalies(symbol, before=before, limit=limit), "anomalies"
        )
        items = [_anomaly(r) for r in rows]
        return Page(items=items, next_before=items[-1].timestamp if len(rows) == limit else None)

    async def predictions(
        self, symbol: str, *, before: datetime | None, limit: int
    ) -> Page[Prediction]:
        await self.require_symbol(symbol)
        rows = await self._from_db(
            lambda: self._db.predictions(symbol, before=before, limit=limit), "predictions"
        )
        items = [_prediction(r) for r in rows]
        return Page(items=items, next_before=items[-1].timestamp if len(rows) == limit else None)

    # ------------------------------------------------------------- watchlist
    async def watchlist(self, user_id: str) -> list[WatchlistItem]:
        rows = await self._from_db(lambda: self._db.watchlist(user_id), "watchlist")
        quotes = {s.symbol: s for s in await self.list_stocks()} if rows else {}
        return [
            WatchlistItem(
                symbol=r["symbol"],
                position=r["position"],
                added_at=r["added_at"],
                quote=quotes.get(r["symbol"]),
            )
            for r in rows
        ]

    async def add_to_watchlist(self, user_id: str, symbol: str) -> bool:
        row = await self._from_db(lambda: self._db.symbol(symbol), "symbol")
        if row is None:
            raise NotFoundError(f"unknown symbol {symbol!r}")
        return await self._from_db(
            lambda: self._db.add_to_watchlist(user_id, symbol), "watchlist_add"
        )

    async def remove_from_watchlist(self, user_id: str, symbol: str) -> None:
        removed = await self._from_db(
            lambda: self._db.remove_from_watchlist(user_id, symbol), "watchlist_remove"
        )
        if not removed:
            raise NotFoundError(f"{symbol!r} is not on the watchlist")

    async def live_snapshot(self, symbols: list[str]) -> list[dict[str, Any]]:
        """Current cached bar per symbol, in the live-update message format."""
        latest = await self._from_cache(lambda: self._cache.latest_bars(symbols), "snapshot")
        return [
            {"type": "bar", "symbol": symbol, "data": payload}
            for symbol, payload in (latest or {}).items()
        ]

    # ------------------------------------------------------------- ML + pipeline status
    async def model_status(self) -> ModelStatus | None:
        row = await self._from_db(self._db.champion, "champion")
        return _model_status(row) if row else None

    async def monitoring(self) -> MonitoringSummary | None:
        rows = await self._from_db(self._db.latest_monitoring, "monitoring")
        if not rows:
            return None
        items = [
            MonitoringItem(
                report_type=r["report_type"],
                metric=r["metric_name"],
                feature=r["feature_name"],
                value=r["value"],
                threshold=r["threshold"],
                status=r["status"],
                details=r["details"] or {},
            )
            for r in rows
        ]
        counts = {
            status: sum(i.status == status for i in items) for status in ("ok", "warning", "alert")
        }
        first = rows[0]
        return MonitoringSummary(
            model_name=first["model_name"],
            model_version=first["model_version"],
            window_start=first["window_start"],
            window_end=first["window_end"],
            computed_at=first["created_at"],
            status_counts=counts,
            retrain_recommended=any(i.metric == "retrain_recommended" for i in items),
            items=items,
        )

    def _stage(self, latest: datetime | None, stale_after_s: float, what: str) -> StageStatus:
        if latest is None:
            return StageStatus(status="missing", detail=f"no {what} yet")
        age = (self._clock() - latest).total_seconds()
        return StageStatus(
            status="stale" if age > stale_after_s else "ok",
            latest_at=latest,
            age_seconds=round(age, 3),
        )

    async def pipeline_status(self, *, monitor_stale_after_s: float) -> PipelineStatus:
        """Freshness of each stage: live data from the cache, ML records from the database.

        A stage is ``stale`` when its newest record is older than a few bar
        intervals (market data, predictions) or than the monitor's schedule.
        """
        stores = await self.readiness()
        try:
            stocks = await self.list_stocks()
        except ServiceUnavailableError:
            stocks = []
        live = [s for s in stocks if s.timestamp is not None]
        interval = BarInterval.S1
        if live:
            interval = await self._latest_interval(live[0].symbol) or interval
        bar_limit = max(self._stale_after_bars * interval.duration.total_seconds(), 10.0)
        latest_bar = max(
            (s.timestamp + interval.duration for s in live if s.timestamp), default=None
        )

        cached = await self._from_cache(
            lambda: self._cache.latest_predictions([s.symbol for s in live]), "latest_predictions"
        )
        prediction_times = [
            datetime.fromisoformat(p["timestamp"]) + interval.duration
            for p in (cached or {}).values()
            if p.get("timestamp")
        ]
        latest_prediction = max(prediction_times, default=None)

        model: ModelStatus | None = None
        summary: MonitoringSummary | None = None
        try:
            model = await self.model_status()
            summary = await self.monitoring()
        except ServiceUnavailableError:
            pass
        return PipelineStatus(
            checked_at=self._clock(),
            stores=stores,
            market_data=self._stage(latest_bar, bar_limit, "market data"),
            # Predictions need a 61-bar warm-up and a served model; allow more slack.
            predictions=self._stage(
                latest_prediction, 2 * bar_limit, "recent predictions in the live cache"
            ),
            monitoring=self._stage(
                summary.computed_at if summary else None, monitor_stale_after_s, "monitor run"
            ),
            model=model,
            active_symbols=len(live),
            stale_symbols=sorted(s.symbol for s in live if s.stale),
        )

    async def _latest_interval(self, symbol: str) -> BarInterval | None:
        payload = await self._from_cache(lambda: self._cache.latest_bar(symbol), "latest_bar")
        return BarInterval(payload["interval"]) if payload else None

    # ------------------------------------------------------------- readiness
    async def readiness(self) -> dict[str, str]:
        checks: dict[str, str] = {}
        for name, probe in (("postgres", self._db.ping), ("redis", self._cache.ping)):
            try:
                await probe()
                checks[name] = "ok"
            except (*DB_ERRORS, *CACHE_ERRORS) as exc:
                checks[name] = f"error: {type(exc).__name__}"
        return checks

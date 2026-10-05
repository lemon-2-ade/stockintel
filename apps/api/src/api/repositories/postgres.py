"""Read/write access to PostgreSQL (async). Returns plain mappings, no HTTP types."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime
from typing import Any

from sqlalchemy import Select, delete, func, select, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncEngine

from shared.db.models import (
    Anomaly,
    BarIndicators,
    MarketBar,
    Prediction,
    Symbol,
    WatchlistItem,
)

Row = Mapping[str, Any]
AnySelect = Select[*tuple[Any, ...]]


class MarketRepository:
    def __init__(self, engine: AsyncEngine) -> None:
        self._engine = engine

    async def _all(self, statement: AnySelect) -> list[Row]:
        async with self._engine.connect() as conn:
            result = await conn.execute(statement)
            return [dict(r) for r in result.mappings().all()]

    async def _one(self, statement: AnySelect) -> Row | None:
        rows = await self._all(statement.limit(1))
        return rows[0] if rows else None

    async def ping(self) -> None:
        async with self._engine.connect() as conn:
            await conn.execute(text("SELECT 1"))

    # ------------------------------------------------------------- symbols
    async def symbols(self) -> list[Row]:
        return await self._all(
            select(Symbol.symbol, Symbol.name).where(Symbol.is_active).order_by(Symbol.symbol)
        )

    async def symbol(self, symbol: str) -> Row | None:
        return await self._one(select(Symbol.symbol, Symbol.name).where(Symbol.symbol == symbol))

    # ------------------------------------------------------------- bars
    async def latest_bar(self, symbol: str) -> Row | None:
        return await self._one(
            select(MarketBar).where(MarketBar.symbol == symbol).order_by(MarketBar.ts.desc())
        )

    async def latest_bars(self, symbols: Sequence[str]) -> list[Row]:
        """Newest bar per symbol (``DISTINCT ON``); used when the cache is unavailable."""
        return await self._all(
            select(MarketBar)
            .distinct(MarketBar.symbol)
            .where(MarketBar.symbol.in_(symbols))
            .order_by(MarketBar.symbol, MarketBar.ts.desc())
        )

    async def bars(
        self,
        symbol: str,
        interval: str,
        *,
        start: datetime | None,
        end: datetime | None,
        before: datetime | None,
        limit: int,
    ) -> list[Row]:
        """Newest-first page of bars; keyset pagination on ``ts`` via ``before``."""
        query = select(MarketBar).where(
            MarketBar.symbol == symbol, MarketBar.bar_interval == interval
        )
        if start is not None:
            query = query.where(MarketBar.ts >= start)
        if end is not None:
            query = query.where(MarketBar.ts < end)
        if before is not None:
            query = query.where(MarketBar.ts < before)
        return await self._all(query.order_by(MarketBar.ts.desc()).limit(limit))

    async def session_stats(self, symbol: str, interval: str, since: datetime) -> Row | None:
        scope = (
            MarketBar.symbol == symbol,
            MarketBar.bar_interval == interval,
            MarketBar.ts >= since,
        )
        aggregates = await self._one(
            select(
                func.max(MarketBar.high).label("high"),
                func.min(MarketBar.low).label("low"),
                func.coalesce(func.sum(MarketBar.volume), 0).label("volume"),
                func.count().label("bars"),
            ).where(*scope)
        )
        if not aggregates or not aggregates["bars"]:
            return None
        first = await self._one(select(MarketBar.open).where(*scope).order_by(MarketBar.ts))
        last = await self._one(select(MarketBar.close).where(*scope).order_by(MarketBar.ts.desc()))
        if first is None or last is None:
            return None
        return {**aggregates, "open": first["open"], "close": last["close"]}

    # ------------------------------------------------------------- analytics
    async def latest_indicators(self, symbol: str, interval: str) -> Row | None:
        return await self._one(
            select(BarIndicators)
            .where(BarIndicators.symbol == symbol, BarIndicators.bar_interval == interval)
            .order_by(BarIndicators.ts.desc())
        )

    async def indicators(
        self, symbol: str, interval: str, *, before: datetime | None, limit: int
    ) -> list[Row]:
        query = select(BarIndicators).where(
            BarIndicators.symbol == symbol, BarIndicators.bar_interval == interval
        )
        if before is not None:
            query = query.where(BarIndicators.ts < before)
        return await self._all(query.order_by(BarIndicators.ts.desc()).limit(limit))

    async def anomalies(
        self, symbol: str | None, *, before: datetime | None, limit: int
    ) -> list[Row]:
        query = select(Anomaly)
        if symbol is not None:
            query = query.where(Anomaly.symbol == symbol)
        if before is not None:
            query = query.where(Anomaly.ts < before)
        return await self._all(query.order_by(Anomaly.ts.desc()).limit(limit))

    async def predictions(self, symbol: str, *, before: datetime | None, limit: int) -> list[Row]:
        query = select(Prediction).where(Prediction.symbol == symbol)
        if before is not None:
            query = query.where(Prediction.ts < before)
        return await self._all(query.order_by(Prediction.ts.desc()).limit(limit))

    async def latest_open_prediction(self, symbol: str, now: datetime) -> Row | None:
        """Newest prediction whose target time has not passed yet."""
        return await self._one(
            select(Prediction)
            .where(Prediction.symbol == symbol, Prediction.target_ts > now)
            .order_by(Prediction.ts.desc())
        )

    # ------------------------------------------------------------- watchlist
    async def watchlist(self, user_id: str) -> list[Row]:
        return await self._all(
            select(WatchlistItem)
            .where(WatchlistItem.user_id == user_id)
            .order_by(WatchlistItem.position, WatchlistItem.added_at)
        )

    async def add_to_watchlist(self, user_id: str, symbol: str) -> bool:
        async with self._engine.begin() as conn:
            position = await conn.scalar(
                select(func.coalesce(func.max(WatchlistItem.position) + 1, 0)).where(
                    WatchlistItem.user_id == user_id
                )
            )
            result = await conn.execute(
                insert(WatchlistItem.__table__)  # type: ignore[arg-type]
                .values(user_id=user_id, symbol=symbol, position=position)
                .on_conflict_do_nothing()
                .returning(WatchlistItem.symbol)
            )
            return result.first() is not None

    async def remove_from_watchlist(self, user_id: str, symbol: str) -> bool:
        async with self._engine.begin() as conn:
            result = await conn.execute(
                delete(WatchlistItem)
                .where(WatchlistItem.user_id == user_id, WatchlistItem.symbol == symbol)
                .returning(WatchlistItem.symbol)
            )
            return result.first() is not None

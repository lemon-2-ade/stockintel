"""Degradation rules of MarketService, with in-memory repositories."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
import redis
from sqlalchemy.exc import OperationalError

from api.errors import NotFoundError, ServiceUnavailableError
from api.service import MarketService

pytestmark = pytest.mark.anyio
NOW = datetime(2026, 1, 5, 15, 0, tzinfo=UTC)
BAR_TS = NOW - timedelta(seconds=2)


def bar_payload(close: float = 101.0) -> dict[str, Any]:
    return {
        "symbol": "AAPL",
        "timestamp": BAR_TS.isoformat(),
        "interval": "1s",
        "open": 100.0,
        "high": 102.0,
        "low": 99.0,
        "close": close,
        "volume": 10,
        "indicators": {"sma_20": 100.0, "return_1": 0.002, "price_change": 0.2},
    }


def bar_row() -> dict[str, Any]:
    return {
        "symbol": "AAPL",
        "ts": BAR_TS,
        "bar_interval": "1s",
        "open": 100.0,
        "high": 102.0,
        "low": 99.0,
        "close": 100.5,
        "volume": 10,
    }


def down(*_: Any, **__: Any) -> Any:
    raise OperationalError("SELECT 1", {}, Exception("connection refused"))


class FakeDb:
    def __init__(self, *, available: bool = True) -> None:
        self.available = available

    def _check(self) -> None:
        if not self.available:
            down()

    async def symbol(self, symbol: str) -> dict[str, Any] | None:
        self._check()
        return {"symbol": "AAPL", "name": "Apple"} if symbol == "AAPL" else None

    async def symbols(self) -> list[dict[str, Any]]:
        self._check()
        return [{"symbol": "AAPL", "name": "Apple"}]

    async def latest_bar(self, symbol: str) -> dict[str, Any] | None:
        self._check()
        return bar_row()

    async def latest_bars(self, symbols: list[str]) -> list[dict[str, Any]]:
        self._check()
        return [bar_row()]

    async def latest_indicators(self, symbol: str, interval: str) -> dict[str, Any] | None:
        self._check()
        return {"ts": BAR_TS, "indicators": {"sma_20": 99.0}}

    async def session_stats(self, *a: Any) -> dict[str, Any] | None:
        self._check()
        return {"open": 100.0, "close": 101.0, "high": 102.0, "low": 99.0, "volume": 50, "bars": 5}

    async def latest_open_prediction(self, *a: Any) -> None:
        self._check()

    async def bars(self, *a: Any, **k: Any) -> list[dict[str, Any]]:
        self._check()
        return [bar_row()]


class FakeCache:
    def __init__(self, *, available: bool = True, warm: bool = True) -> None:
        self.available = available
        self.warm = warm

    def _check(self) -> None:
        if not self.available:
            raise redis.ConnectionError("redis down")

    async def active_symbols(self) -> set[str]:
        self._check()
        return {"AAPL", "MSFT"}

    async def latest_bar(self, symbol: str) -> dict[str, Any] | None:
        self._check()
        return bar_payload() if self.warm else None

    async def latest_bars(self, symbols: list[str]) -> dict[str, Any]:
        self._check()
        return {"AAPL": bar_payload()} if self.warm else {}

    async def latest_prediction(self, symbol: str) -> None:
        self._check()


def service(db: FakeDb, cache: FakeCache) -> MarketService:
    return MarketService(db, cache, clock=lambda: NOW)  # type: ignore[arg-type]


async def test_detail_prefers_cache() -> None:
    detail = await service(FakeDb(), FakeCache()).stock_detail("AAPL")
    assert detail.freshness.source == "cache"
    assert detail.observation.close == 101.0
    assert detail.analytics is not None
    assert detail.analytics.sma_20 == 100.0
    assert detail.session is not None
    assert detail.session.change_pct == pytest.approx(1.0)
    assert detail.prediction is None
    assert not detail.freshness.stale


async def test_cache_outage_falls_back_to_database() -> None:
    detail = await service(FakeDb(), FakeCache(available=False)).stock_detail("AAPL")
    assert detail.freshness.source == "database"
    assert detail.analytics is not None
    assert detail.analytics.sma_20 == 99.0


async def test_database_outage_keeps_live_data_but_drops_session() -> None:
    detail = await service(FakeDb(available=False), FakeCache()).stock_detail("AAPL")
    assert detail.freshness.source == "cache"
    assert detail.session is None


async def test_history_needs_the_database() -> None:
    from shared.schemas import BarInterval  # noqa: PLC0415

    svc = service(FakeDb(available=False), FakeCache())
    with pytest.raises(ServiceUnavailableError):
        await svc.history("AAPL", BarInterval.S1, start=None, end=None, before=None, limit=10)


async def test_both_stores_down_is_unavailable() -> None:
    svc = service(FakeDb(available=False), FakeCache(available=False))
    with pytest.raises(ServiceUnavailableError):
        await svc.list_stocks()
    with pytest.raises(ServiceUnavailableError):
        await svc.stock_detail("AAPL")


async def test_unknown_symbol_is_404_even_in_degraded_mode() -> None:
    with pytest.raises(NotFoundError):
        await service(FakeDb(), FakeCache()).stock_detail("ZZZ")
    with pytest.raises(NotFoundError):
        await service(FakeDb(available=False), FakeCache()).stock_detail("ZZZ")


async def test_listing_merges_db_and_cache_symbols() -> None:
    listing = await service(FakeDb(), FakeCache()).list_stocks()
    assert [s.symbol for s in listing] == ["AAPL", "MSFT"]
    aapl, msft = listing
    assert aapl.name == "Apple"
    assert aapl.change_pct == pytest.approx(0.2)
    assert msft.last_price is None


async def test_stale_data_is_flagged() -> None:
    svc = MarketService(FakeDb(), FakeCache(), clock=lambda: NOW + timedelta(minutes=5))  # type: ignore[arg-type]
    assert (await svc.stock_detail("AAPL")).freshness.stale

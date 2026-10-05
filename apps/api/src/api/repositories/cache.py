"""Read access to the Redis latest-state cache (written by the cache sink)."""

from __future__ import annotations

import json
from collections.abc import Sequence
from typing import Any

import redis.asyncio as aioredis

from shared import cache

Payload = dict[str, Any]


class CacheRepository:
    def __init__(self, client: aioredis.Redis) -> None:
        self._client = client

    @property
    def client(self) -> aioredis.Redis:
        return self._client

    async def ping(self) -> None:
        await self._client.ping()

    async def active_symbols(self) -> set[str]:
        members = await self._client.smembers(cache.ACTIVE_SYMBOLS)
        return {str(m) for m in members}

    async def latest_bar(self, symbol: str) -> Payload | None:
        raw = await self._client.hget(cache.latest_bar_key(symbol), "data")
        return json.loads(raw) if raw else None

    async def latest_bars(self, symbols: Sequence[str]) -> dict[str, Payload]:
        if not symbols:
            return {}
        pipe = self._client.pipeline(transaction=False)
        for symbol in symbols:
            pipe.hget(cache.latest_bar_key(symbol), "data")
        values = await pipe.execute()
        return {s: json.loads(v) for s, v in zip(symbols, values, strict=True) if v}

    async def latest_prediction(self, symbol: str) -> Payload | None:
        raw = await self._client.hget(cache.latest_prediction_key(symbol), "data")
        return json.loads(raw) if raw else None

    async def recent_anomalies(self, symbol: str | None, limit: int) -> list[Payload]:
        key = cache.RECENT_ANOMALIES if symbol is None else cache.symbol_anomalies_key(symbol)
        members = await self._client.zrevrange(key, 0, limit - 1)
        return [json.loads(str(m)) for m in members]

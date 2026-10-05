"""Live market updates over WebSocket, fed by Redis pub/sub.

Fan-out design
--------------
Each API process holds **one** pattern subscription (``market:*``) and fans
messages out to its local connections. Redis therefore sees one subscriber
per replica, not one per browser tab, and any replica can serve any client.

Throttling (per connection)
---------------------------
The stream can produce many bars per second per symbol; a browser needs a
few. Each connection keeps:

* the **latest** bar and prediction per symbol (newer updates *replace*
  older ones that were not sent yet: coalescing, counted in a metric);
* a bounded queue of anomalies (each one matters, so they are not coalesced;
  if a client falls so far behind that the queue overflows, the oldest are
  dropped and counted).

A sender task flushes everything pending as one ``updates`` frame at most
once per ``flush_interval``. A slow client therefore costs bounded memory
and never slows the hub or other clients. When idle, a ``heartbeat`` frame
is sent so clients (and proxies) can detect dead connections.

Protocol
--------
server -> client: ``snapshot`` (on connect), ``updates``, ``heartbeat``,
``subscribed``, ``error``, ``pong``.
client -> server: ``{"action": "subscribe" | "unsubscribe", "symbols": [...]}``,
``{"action": "ping"}``.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import re
from collections import deque
from collections.abc import Callable, Iterable
from typing import Any, Protocol

import redis.asyncio as aioredis

from api.metrics import ApiMetrics
from shared import cache
from shared.observability.logs import get_logger
from shared.schemas import utcnow
from shared.schemas.base import SYMBOL_PATTERN
from shared.utils.retry import BackoffPolicy

log = get_logger(__name__)
_SYMBOL_RE = re.compile(SYMBOL_PATTERN)
COALESCED_TYPES = frozenset({"bar", "prediction"})


class SocketLike(Protocol):
    async def send_text(self, data: str) -> None: ...


def parse_symbols(raw: Iterable[str], limit: int) -> list[str]:
    symbols = list(dict.fromkeys(s.strip() for s in raw if s.strip()))
    bad = [s for s in symbols if not _SYMBOL_RE.match(s)]
    if bad:
        raise ValueError(f"invalid symbols: {bad}")
    if len(symbols) > limit:
        raise ValueError(f"at most {limit} symbols per connection")
    return symbols


class ClientConnection:
    def __init__(
        self,
        socket: SocketLike,
        *,
        flush_interval_s: float,
        heartbeat_s: float,
        max_pending_anomalies: int,
        metrics: ApiMetrics,
        clock: Callable[[], float] | None = None,
    ) -> None:
        self.socket = socket
        self.symbols: set[str] = set()
        self._flush_interval_s = flush_interval_s
        self._heartbeat_s = heartbeat_s
        self._metrics = metrics
        self._latest: dict[tuple[str, str], dict[str, Any]] = {}
        self._events: deque[dict[str, Any]] = deque(maxlen=max_pending_anomalies)
        self._pending = asyncio.Event()
        self._clock = clock or asyncio.get_running_loop().time
        self.dropped_events = 0

    def offer(self, update: dict[str, Any]) -> None:
        """Called by the hub for every update on a subscribed symbol (never blocks)."""
        kind = update.get("type", "")
        if kind in COALESCED_TYPES:
            key = (kind, update.get("symbol", ""))
            if key in self._latest:
                self._metrics.ws_coalesced.inc()
            self._latest[key] = update
        else:
            if len(self._events) == self._events.maxlen:
                self.dropped_events += 1
            self._events.append(update)
        self._pending.set()

    def drain(self) -> list[dict[str, Any]]:
        items = [*self._events, *self._latest.values()]
        self._events.clear()
        self._latest.clear()
        self._pending.clear()
        return items

    async def send(self, message: dict[str, Any]) -> None:
        await self.socket.send_text(json.dumps(message, separators=(",", ":")))
        self._metrics.ws_messages.labels(type=message["type"]).inc()

    async def run_sender(self) -> None:
        """Flush at most once per interval; heartbeat when idle. Ends when the socket fails."""
        last_flush = -self._flush_interval_s
        while True:
            try:
                await asyncio.wait_for(self._pending.wait(), timeout=self._heartbeat_s)
            except TimeoutError:
                await self.send({"type": "heartbeat", "server_time": utcnow().isoformat()})
                continue
            wait = last_flush + self._flush_interval_s - self._clock()
            if wait > 0:
                await asyncio.sleep(wait)  # let more updates coalesce
            items = self.drain()
            if items:
                await self.send(
                    {"type": "updates", "server_time": utcnow().isoformat(), "items": items}
                )
            last_flush = self._clock()


class LiveHub:
    """Single Redis pattern subscription per process, fanned out to local clients."""

    def __init__(self, client: aioredis.Redis, metrics: ApiMetrics) -> None:
        self._client = client
        self._metrics = metrics
        self._by_symbol: dict[str, set[ClientConnection]] = {}
        self._task: asyncio.Task[None] | None = None
        self._backoff = BackoffPolicy(max_attempts=10_000, base_delay_s=0.5, max_delay_s=10.0)

    @property
    def connections(self) -> int:
        return len({c for conns in self._by_symbol.values() for c in conns})

    def start(self) -> None:
        self._task = asyncio.create_task(self._listen_forever(), name="live-hub")

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task

    def subscribe(self, conn: ClientConnection, symbols: Iterable[str]) -> None:
        for symbol in symbols:
            self._by_symbol.setdefault(symbol, set()).add(conn)
            conn.symbols.add(symbol)

    def unsubscribe(self, conn: ClientConnection, symbols: Iterable[str] | None = None) -> None:
        for symbol in list(conn.symbols if symbols is None else symbols):
            subscribers = self._by_symbol.get(symbol)
            if subscribers is not None:
                subscribers.discard(conn)
                if not subscribers:
                    del self._by_symbol[symbol]
            conn.symbols.discard(symbol)

    def dispatch(self, channel_name: str, data: str) -> int:
        """Deliver one pub/sub message to subscribers; returns how many received it."""
        subscribers = self._by_symbol.get(cache.symbol_from_channel(channel_name))
        if not subscribers:
            return 0
        try:
            update = json.loads(data)
        except json.JSONDecodeError:
            log.warning("live.bad_message", channel=channel_name)
            return 0
        for conn in subscribers:
            conn.offer(update)
        return len(subscribers)

    async def _listen_forever(self) -> None:
        attempt = 0
        while True:
            try:
                pubsub = self._client.pubsub()
                await pubsub.psubscribe(f"{cache.CHANNEL_PREFIX}*")
                log.info("live.subscribed")
                attempt = 0
                async for message in pubsub.listen():
                    if message.get("type") == "pmessage":
                        self.dispatch(str(message["channel"]), str(message["data"]))
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # Redis restart/failover: reconnect with backoff
                attempt += 1
                delay = self._backoff.delay_for(min(attempt, 10))
                log.warning("live.redis_disconnected", error=str(exc), retry_in_s=round(delay, 2))
                await asyncio.sleep(delay)

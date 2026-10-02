"""Latest-state cache and live fan-out in Redis.

All writes are small Lua scripts so that "check, write, publish" is atomic
and idempotent:

* ``latest:*`` hashes only move **forward in event time**: an older or
  duplicate event (redelivery, replay, late bar) never overwrites newer state,
  and nothing is published for it.
* recent anomalies are sorted-set members whose payload is canonical
  (``produced_at`` excluded), so a re-delivered anomaly is the same member and
  is not re-published; sets are trimmed to a fixed size.

A whole Kafka batch is sent in one pipeline round trip. Redis errors raise
:class:`TransientSinkError`; the runner retries and, if Redis stays down,
restarts without committing, so the cache is rebuilt from Kafka afterwards.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Sequence
from typing import Any

import redis

from shared import cache
from shared.kafka.topics import Topic
from shared.schemas import AnomalyEvent, BaseEvent, EnrichedBarEvent, PredictionEvent
from sinks.runner import TransientSinkError, WriteResult

SET_IF_NEWER = """
local current = redis.call('HGET', KEYS[1], 'ts')
if current and tonumber(current) >= tonumber(ARGV[1]) then
  return 0
end
redis.call('HSET', KEYS[1], 'ts', ARGV[1], 'data', ARGV[2])
local ttl = tonumber(ARGV[3])
if ttl > 0 then
  redis.call('PEXPIRE', KEYS[1], ttl)
end
redis.call('SADD', KEYS[2], ARGV[6])
redis.call('PUBLISH', ARGV[4], ARGV[5])
return 1
"""

ADD_ANOMALY = """
local added = redis.call('ZADD', KEYS[1], ARGV[1], ARGV[2])
redis.call('ZADD', KEYS[2], ARGV[1], ARGV[2])
redis.call('ZREMRANGEBYRANK', KEYS[1], 0, -(tonumber(ARGV[3]) + 1))
redis.call('ZREMRANGEBYRANK', KEYS[2], 0, -(tonumber(ARGV[4]) + 1))
if added == 1 then
  redis.call('PUBLISH', ARGV[5], ARGV[6])
end
return added
"""


def _epoch_ms(event: EnrichedBarEvent | AnomalyEvent | PredictionEvent) -> int:
    return int(event.timestamp.timestamp() * 1000)


class RedisCacheSink:
    name = "redis"
    topics: Sequence[str] = (
        Topic.MARKET_ENRICHED,
        Topic.MARKET_ANOMALIES,
        Topic.MARKET_PREDICTIONS,
    )

    def __init__(self, client: redis.Redis) -> None:
        self._client = client
        self._set_if_newer = client.register_script(SET_IF_NEWER)
        self._add_anomaly = client.register_script(ADD_ANOMALY)

    def write(self, events: Sequence[BaseEvent]) -> WriteResult:
        pipe = self._client.pipeline(transaction=False)
        targets: list[str] = []
        for event in events:
            if isinstance(event, EnrichedBarEvent):
                self._latest(pipe, cache.latest_bar_key(event.symbol), "bar", event, ttl_ms=0)
                targets.append("latest_bar")
            elif isinstance(event, PredictionEvent):
                ttl_ms = max(
                    1, int((event.target_timestamp - event.timestamp).total_seconds() * 1000)
                )
                key = cache.latest_prediction_key(event.symbol)
                self._latest(pipe, key, "prediction", event, ttl_ms=ttl_ms)
                targets.append("latest_prediction")
            elif isinstance(event, AnomalyEvent):
                self._add_anomaly(
                    keys=[cache.RECENT_ANOMALIES, cache.symbol_anomalies_key(event.symbol)],
                    args=[
                        _epoch_ms(event),
                        cache.dumps(cache.event_payload(event)),
                        cache.RECENT_ANOMALIES_GLOBAL,
                        cache.RECENT_ANOMALIES_PER_SYMBOL,
                        cache.channel(event.symbol),
                        cache.update_message("anomaly", event),
                    ],
                    client=pipe,
                )
                targets.append("anomalies")
        if not targets:
            return WriteResult()
        try:
            outcomes: list[Any] = pipe.execute()
        except redis.RedisError as exc:
            raise TransientSinkError(f"redis unavailable: {exc}") from exc

        written: Counter[str] = Counter()
        skipped: Counter[str] = Counter()
        for target, changed in zip(targets, outcomes, strict=True):
            (written if changed else skipped)[target] += 1
        return WriteResult(written=dict(written), skipped=dict(skipped))

    def _latest(
        self,
        pipe: Any,
        key: str,
        kind: cache.UpdateType,
        event: EnrichedBarEvent | PredictionEvent,
        *,
        ttl_ms: int,
    ) -> None:
        self._set_if_newer(
            keys=[key, cache.ACTIVE_SYMBOLS],
            args=[
                _epoch_ms(event),
                cache.dumps(cache.event_payload(event)),
                ttl_ms,
                cache.channel(event.symbol),
                cache.update_message(kind, event),
                event.symbol,
            ],
            client=pipe,
        )

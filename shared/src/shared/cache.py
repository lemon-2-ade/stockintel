"""Redis key layout and payload format shared by the cache writer and the API.

Redis only holds *derivable, latest* state (rebuildable from Kafka/Postgres)
and fans out live updates; it is never the system of record.

======================================  ======  =====================================
key                                     type    content
======================================  ======  =====================================
``latest:bar:{symbol}``                 hash    ``ts`` (event-time epoch ms), ``data``
                                                (enriched bar JSON); event-time guarded
``latest:prediction:{symbol}``          hash    same shape; expires after its horizon
``anomalies:recent``                    zset    canonical anomaly JSON scored by ts,
                                                capped at ``RECENT_ANOMALIES_GLOBAL``
``anomalies:recent:{symbol}``           zset    same, capped per symbol
``symbols:active``                      set     symbols seen in the stream
``market:{symbol}`` (pub/sub channel)   -       live update messages (see ``Update``)
======================================  ======  =====================================
"""

from __future__ import annotations

import json
from typing import Any, Literal, TypedDict

from shared.schemas import AnomalyEvent, BaseEvent, EnrichedBarEvent, PredictionEvent

RECENT_ANOMALIES_GLOBAL = 500
RECENT_ANOMALIES_PER_SYMBOL = 100
CHANNEL_PREFIX = "market:"
ACTIVE_SYMBOLS = "symbols:active"
RECENT_ANOMALIES = "anomalies:recent"

UpdateType = Literal["bar", "anomaly", "prediction"]


class Update(TypedDict):
    """Message published on ``market:{symbol}`` and forwarded to WebSocket clients."""

    type: UpdateType
    symbol: str
    data: dict[str, Any]


def latest_bar_key(symbol: str) -> str:
    return f"latest:bar:{symbol}"


def latest_prediction_key(symbol: str) -> str:
    return f"latest:prediction:{symbol}"


def symbol_anomalies_key(symbol: str) -> str:
    return f"anomalies:recent:{symbol}"


def channel(symbol: str) -> str:
    return f"{CHANNEL_PREFIX}{symbol}"


def symbol_from_channel(name: str) -> str:
    return name.removeprefix(CHANNEL_PREFIX)


def event_payload(event: BaseEvent) -> dict[str, Any]:
    """Canonical JSON-able payload.

    ``produced_at`` is dropped: it is the only field that differs when the same
    input is re-processed, so excluding it makes the payload of a redelivered
    event byte-identical (sorted-set members de-duplicate naturally).
    """
    payload: dict[str, Any] = event.model_dump(mode="json", exclude={"produced_at"})
    return payload


def dumps(payload: dict[str, Any]) -> str:
    return json.dumps(payload, separators=(",", ":"), sort_keys=True)


def update_message(
    kind: UpdateType, event: EnrichedBarEvent | AnomalyEvent | PredictionEvent
) -> str:
    message: Update = {"type": kind, "symbol": event.symbol, "data": event_payload(event)}
    return json.dumps(message, separators=(",", ":"))

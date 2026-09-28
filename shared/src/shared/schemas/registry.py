"""Maps ``(event_type, schema_version)`` to the model class that decodes it.

This is the in-process equivalent of a schema registry subject lookup. Adding
a ``schema_version=2`` model means registering it here next to v1 so both can
be decoded while producers roll forward.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from types import MappingProxyType

from shared.schemas.anomaly import AnomalyEvent
from shared.schemas.base import BaseEvent
from shared.schemas.dead_letter import DeadLetterEvent
from shared.schemas.market import EnrichedBarEvent, MarketBarEvent
from shared.schemas.prediction import PredictionEvent

EventKey = tuple[str, int]


class UnknownEventTypeError(LookupError):
    """No model is registered for the given ``(event_type, schema_version)``."""


def _build(models: Iterable[type[BaseEvent]]) -> Mapping[EventKey, type[BaseEvent]]:
    registry: dict[EventKey, type[BaseEvent]] = {}
    for model in models:
        key = model.type_key()
        if key in registry:
            raise ValueError(f"duplicate event registration for {key}")
        registry[key] = model
    return MappingProxyType(registry)


EVENT_REGISTRY: Mapping[EventKey, type[BaseEvent]] = _build(
    [MarketBarEvent, EnrichedBarEvent, AnomalyEvent, PredictionEvent, DeadLetterEvent]
)


def model_for(event_type: str, schema_version: int) -> type[BaseEvent]:
    try:
        return EVENT_REGISTRY[(event_type, schema_version)]
    except KeyError:
        raise UnknownEventTypeError(
            f"no schema registered for event_type={event_type!r} schema_version={schema_version}"
        ) from None

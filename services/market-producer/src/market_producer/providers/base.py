"""The provider abstraction.

A provider yields :class:`Batch` objects: bars that should be published at a
given wall-clock time. The producer runtime owns pacing, publishing, metrics
and shutdown, so a new source (a vendor websocket, a Kafka mirror, a CSV
replay) only has to implement :meth:`MarketDataProvider.batches`. Swapping the
simulator for a live feed therefore does not touch the rest of the pipeline.
"""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from typing import Protocol

from shared.schemas import MarketBarEvent


@dataclass(frozen=True, slots=True)
class ProducedBar:
    event: MarketBarEvent
    injected: tuple[str, ...] = ()
    """Ground-truth anomaly labels (simulator only). Travels as a Kafka header,
    never inside the event, because a real feed cannot know them."""


@dataclass(frozen=True, slots=True)
class Batch:
    due_at: float
    """Wall-clock epoch seconds at which to publish; in the past means now."""
    bars: Sequence[ProducedBar]


class MarketDataProvider(Protocol):
    @property
    def name(self) -> str: ...

    def batches(self) -> Iterator[Batch]:
        """Yield batches in publish order. Finite for replays, endless for live sources."""
        ...

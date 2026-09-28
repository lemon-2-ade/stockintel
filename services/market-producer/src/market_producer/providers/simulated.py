"""Simulator-backed provider emitting closed bars on the wall-clock grid."""

from __future__ import annotations

import itertools
import math
from collections.abc import Iterator
from datetime import UTC, datetime

from market_producer.providers.base import Batch
from market_producer.simulator import MarketSimulator
from shared.schemas import BarInterval


def aligned_start(now: datetime, interval: BarInterval, backfill_bars: int) -> datetime:
    """Open time of the first bar so that ``backfill_bars`` bars are already closed.

    Bars are aligned to the interval grid (e.g. whole minutes for ``1m``). The
    backfilled bars are due immediately, giving charts some history on start,
    and the stream then continues in real time. Event times therefore never lie
    in the future, which downstream freshness checks rely on.
    """
    if backfill_bars < 0:
        raise ValueError("backfill_bars must be >= 0")
    seconds = interval.duration.total_seconds()
    current_open = math.floor(now.timestamp() / seconds) * seconds
    return datetime.fromtimestamp(current_open - backfill_bars * seconds, tz=UTC)


class SimulatorProvider:
    def __init__(
        self,
        simulator: MarketSimulator,
        *,
        start: datetime,
        max_bars: int | None = None,
    ) -> None:
        if start.tzinfo is None:
            raise ValueError("start must be timezone-aware")
        self._simulator = simulator
        self._start = start
        self._max_bars = max_bars

    @property
    def name(self) -> str:
        return "simulator"

    def batches(self) -> Iterator[Batch]:
        step = self._simulator.interval.duration
        counter = itertools.count() if self._max_bars is None else range(self._max_bars)
        for k in counter:
            open_time = self._start + k * step
            yield Batch(due_at=(open_time + step).timestamp(), bars=self._simulator.step(open_time))

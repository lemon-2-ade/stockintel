"""Replay a raw historical snapshot (``data/raw/...``) onto Kafka.

Useful for exercising the pipeline with real price dynamics. Events keep their
original (historical) event time; pacing is controlled by
``bars_per_second`` (one "bar" = all symbols' bars sharing a timestamp).

Rows that violate the event schema are skipped and counted, never published:
the replay must not inject invalid data. Formal validation and cleaning of
the snapshot is a separate offline stage (docs/DATA_PIPELINE.md).
"""

from __future__ import annotations

import csv
import heapq
import itertools
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path

from pydantic import ValidationError

from market_producer.providers.base import Batch, ProducedBar
from shared.observability.logs import get_logger
from shared.schemas import BarInterval, MarketBarEvent

log = get_logger(__name__)


class HistoricalReplayProvider:
    def __init__(
        self,
        snapshot_dir: Path,
        symbols: list[str],
        *,
        bars_per_second: float,
        start_at: float,
        interval: BarInterval = BarInterval.D1,
        source: str = "replay",
        limit: int | None = None,
    ) -> None:
        if bars_per_second <= 0:
            raise ValueError("bars_per_second must be > 0")
        missing = [s for s in symbols if not (snapshot_dir / f"{s}.csv").exists()]
        if missing:
            raise FileNotFoundError(f"{snapshot_dir}: no data for {missing}; run `make data`")
        self._dir = snapshot_dir
        self._symbols = symbols
        self._period = 1.0 / bars_per_second
        self._start_at = start_at
        self._interval = interval
        self._source = source
        self._limit = limit
        self.skipped_rows = 0

    @property
    def name(self) -> str:
        return "replay"

    def _read_symbol(self, symbol: str) -> Iterator[MarketBarEvent]:
        last: datetime | None = None
        with (self._dir / f"{symbol}.csv").open(newline="") as fh:
            for line, row in enumerate(csv.DictReader(fh), start=2):
                try:
                    event = MarketBarEvent(
                        source=self._source,
                        symbol=symbol,
                        timestamp=datetime.fromtimestamp(int(row["timestamp"]), tz=UTC),
                        interval=self._interval,
                        open=float(row["open"]),
                        high=float(row["high"]),
                        low=float(row["low"]),
                        close=float(row["close"]),
                        volume=int(float(row["volume"])),
                    )
                except (KeyError, ValueError, ValidationError) as exc:
                    self.skipped_rows += 1
                    log.warning(
                        "replay.row_skipped", symbol=symbol, line=line, error=str(exc)[:200]
                    )
                    continue
                if last is not None and event.timestamp <= last:
                    self.skipped_rows += 1
                    log.warning("replay.row_out_of_order", symbol=symbol, line=line)
                    continue
                last = event.timestamp
                yield event

    def batches(self) -> Iterator[Batch]:
        # k-way merge of per-symbol streams: memory is O(symbols), not O(rows).
        merged = heapq.merge(
            *(self._read_symbol(s) for s in self._symbols), key=lambda e: e.timestamp
        )
        groups = itertools.groupby(merged, key=lambda e: e.timestamp)
        for index, (_, events) in enumerate(itertools.islice(groups, self._limit)):
            yield Batch(
                due_at=self._start_at + index * self._period,
                bars=[ProducedBar(event=e) for e in events],
            )

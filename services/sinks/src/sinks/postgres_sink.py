"""Idempotent, batched persistence of market events into PostgreSQL.

* One transaction per Kafka batch; rows are inserted with multi-row
  ``INSERT ... ON CONFLICT DO NOTHING`` (chunked below Postgres's bind
  parameter limit), so a re-delivered event is a no-op and the returned row
  count separates new rows from duplicates.
* Symbols seen for the first time are registered in ``symbols`` (needed by
  the watchlist foreign key), cached in memory to avoid re-checking.
* Connection-level failures raise :class:`TransientSinkError` (the runner
  retries, then restarts). A row the database refuses (a constraint the
  schema somehow missed) must not block the batch: the batch is retried row by
  row inside savepoints and only the offending rows are rejected to the DLQ.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable, Iterable, Sequence
from typing import Any

from sqlalchemy import Connection, Engine, Table
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import DBAPIError, IntegrityError, InterfaceError, OperationalError

from shared.db.models import Anomaly, BarIndicators, MarketBar, Prediction, Symbol
from shared.kafka.topics import Topic
from shared.observability.logs import get_logger
from shared.schemas import (
    AnomalyEvent,
    BaseEvent,
    EnrichedBarEvent,
    MarketBarEvent,
    PredictionEvent,
)
from sinks.runner import TransientSinkError, WriteResult

log = get_logger(__name__)

ROWS_PER_STATEMENT = 1_000  # x <= 18 columns stays far below the 65,535-parameter limit

Row = dict[str, Any]


def bar_row(e: MarketBarEvent) -> Row:
    return {
        "symbol": e.symbol,
        "bar_interval": e.interval.value,
        "ts": e.timestamp,
        "open": e.open,
        "high": e.high,
        "low": e.low,
        "close": e.close,
        "volume": e.volume,
        "source": e.source,
        "event_id": e.event_id,
    }


def indicator_row(e: EnrichedBarEvent) -> Row:
    return {
        "symbol": e.symbol,
        "bar_interval": e.interval.value,
        "ts": e.timestamp,
        "indicators": e.indicators.model_dump(mode="json"),
        "indicator_version": e.indicator_version,
        "source_event_id": e.source_event_id,
    }


def anomaly_row(e: AnomalyEvent) -> Row:
    return {
        "anomaly_id": e.anomaly_id,
        "symbol": e.symbol,
        "bar_interval": e.interval.value,
        "ts": e.timestamp,
        "anomaly_type": e.anomaly_type.value,
        "severity": e.severity.value,
        "observed_value": e.observed_value,
        "expected_value": e.expected_value,
        "score": e.score,
        "threshold": e.threshold,
        "detector": e.detector,
        "detector_version": e.detector_version,
        "source_event_id": e.source_event_id,
        "details": e.details,
    }


def prediction_row(e: PredictionEvent) -> Row:
    return {
        "prediction_id": e.prediction_id,
        "symbol": e.symbol,
        "bar_interval": e.interval.value,
        "ts": e.timestamp,
        "target_ts": e.target_timestamp,
        "horizon_bars": e.horizon_bars,
        "task": e.task.value,
        "predicted_direction": e.predicted_direction.value if e.predicted_direction else None,
        "predicted_return": e.predicted_return,
        "class_probabilities": (
            {k.value: v for k, v in e.class_probabilities.items()}
            if e.class_probabilities
            else None
        ),
        "confidence": e.confidence,
        "model_name": e.model_name,
        "model_version": e.model_version,
        "feature_set_version": e.feature_set_version,
        "features": e.features,
        "inference_latency_ms": e.inference_latency_ms,
        "source_event_id": e.source_event_id,
    }


# event class -> (table, row mapper)
ROUTES: dict[type[BaseEvent], tuple[Table, Callable[[Any], Row]]] = {
    MarketBarEvent: (MarketBar.__table__, bar_row),  # type: ignore[dict-item]
    EnrichedBarEvent: (BarIndicators.__table__, indicator_row),  # type: ignore[dict-item]
    AnomalyEvent: (Anomaly.__table__, anomaly_row),  # type: ignore[dict-item]
    PredictionEvent: (Prediction.__table__, prediction_row),  # type: ignore[dict-item]
}


def _chunks(rows: list[Row], size: int) -> Iterable[list[Row]]:
    for start in range(0, len(rows), size):
        yield rows[start : start + size]


class PostgresSink:
    name = "postgres"
    topics: Sequence[str] = (
        Topic.MARKET_RAW,
        Topic.MARKET_ENRICHED,
        Topic.MARKET_ANOMALIES,
        Topic.MARKET_PREDICTIONS,
    )

    def __init__(self, engine: Engine) -> None:
        self._engine = engine
        self._known_symbols: set[str] = set()

    def write(self, events: Sequence[BaseEvent]) -> WriteResult:
        grouped: dict[Table, list[tuple[BaseEvent, Row]]] = defaultdict(list)
        for event in events:
            route = ROUTES.get(type(event))
            if route is None:
                continue  # e.g. a dead-letter record on a market topic: not ours to store
            table, to_row = route
            grouped[table].append((event, to_row(event)))

        result = WriteResult()
        try:
            with self._engine.begin() as conn:
                new_symbols = self._register_symbols(conn, events)
                for table, items in grouped.items():
                    try:
                        with conn.begin_nested():
                            inserted = self._insert(conn, table, [row for _, row in items])
                    except IntegrityError:
                        inserted = self._insert_one_by_one(conn, table, items, result)
                    result.written[table.name] = inserted
                    result.skipped[table.name] = (
                        len(items)
                        - inserted
                        - sum(1 for e, _ in result.rejected if ROUTES[type(e)][0] is table)
                    )
            # Only remember symbols once the transaction has committed.
            self._known_symbols |= new_symbols
        except (OperationalError, InterfaceError) as exc:
            raise TransientSinkError(f"database unavailable: {exc}") from exc
        except DBAPIError as exc:
            if exc.connection_invalidated:
                raise TransientSinkError(f"connection lost: {exc}") from exc
            raise
        return result

    @staticmethod
    def _insert(conn: Connection, table: Table, rows: list[Row]) -> int:
        # RETURNING yields exactly the rows that were inserted (conflicts return
        # nothing); rowcount is not reliable for multi-row inserts across drivers.
        key = next(iter(table.primary_key.columns))
        inserted = 0
        for chunk in _chunks(rows, ROWS_PER_STATEMENT):
            statement = insert(table).values(chunk).on_conflict_do_nothing().returning(key)
            inserted += len(conn.execute(statement).fetchall())
        return inserted

    def _insert_one_by_one(
        self,
        conn: Connection,
        table: Table,
        items: list[tuple[BaseEvent, Row]],
        result: WriteResult,
    ) -> int:
        log.warning("postgres.batch_rejected_retrying_rows", table=table.name, rows=len(items))
        inserted = 0
        for event, row in items:
            try:
                with conn.begin_nested():
                    inserted += self._insert(conn, table, [row])
            except IntegrityError as exc:
                result.rejected.append((event, f"{table.name}: {exc.orig}"))
        return inserted

    def _register_symbols(self, conn: Connection, events: Sequence[BaseEvent]) -> set[str]:
        new = {
            symbol
            for e in events
            if (symbol := getattr(e, "symbol", None)) and symbol not in self._known_symbols
        }
        if new:
            conn.execute(
                insert(Symbol.__table__)  # type: ignore[arg-type]
                .values([{"symbol": s} for s in sorted(new)])
                .on_conflict_do_nothing()
            )
        return new

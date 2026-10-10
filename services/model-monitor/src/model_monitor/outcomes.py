"""Joining realised truth to predictions once their target time has passed.

One set-based statement per run, idempotent (``ON CONFLICT DO NOTHING`` on the
outcome's primary key), so overlapping runs or restarts cannot double-count.

For a prediction made on the bar at ``ts`` about ``target_ts``:

* base price: close of the bar at ``ts`` (the as-of bar);
* realised price: close of the **first bar at or after** ``target_ts``, but
  only if it arrived within one horizon of the target. A prediction whose
  target bar never came (stream stopped, symbol delisted) stays unresolved
  rather than being scored against a much later price;
* actual return: ``ln(close_target / close_asof)``, the training label;
* ``is_correct``: predicted direction equals the realised one; NULL when the
  realised return is exactly zero (no direction, as in training).

Only predictions whose target falls inside ``lookback`` are considered, which
bounds the scan as the table grows.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from sqlalchemy import text
from sqlalchemy.engine import Engine

RESOLVE_SQL = text(
    """
    INSERT INTO prediction_outcomes (prediction_id, actual_return, actual_direction,
                                     is_correct, abs_error)
    SELECT p.prediction_id,
           r.actual_return,
           r.actual_direction,
           CASE WHEN r.actual_direction = 'neutral' OR p.predicted_direction IS NULL THEN NULL
                ELSE p.predicted_direction = r.actual_direction END,
           CASE WHEN p.predicted_return IS NULL THEN NULL
                ELSE abs(p.predicted_return - r.actual_return) END
    FROM predictions p
    JOIN market_bars b
      ON b.symbol = p.symbol AND b.bar_interval = p.bar_interval AND b.ts = p.ts
    JOIN LATERAL (
        SELECT t.close
        FROM market_bars t
        WHERE t.symbol = p.symbol AND t.bar_interval = p.bar_interval
          AND t.ts >= p.target_ts
          AND t.ts <= p.target_ts + (p.target_ts - p.ts)
        ORDER BY t.ts
        LIMIT 1
    ) t ON true
    CROSS JOIN LATERAL (
        SELECT ln(t.close / b.close) AS actual_return,
               CASE WHEN t.close > b.close THEN 'up'
                    WHEN t.close < b.close THEN 'down'
                    ELSE 'neutral' END AS actual_direction
    ) r
    WHERE p.target_ts <= :now
      AND p.target_ts > :since
      AND NOT EXISTS (SELECT 1 FROM prediction_outcomes o WHERE o.prediction_id = p.prediction_id)
    ORDER BY p.target_ts
    LIMIT :limit
    ON CONFLICT (prediction_id) DO NOTHING
    RETURNING prediction_id
    """
)


def resolve_outcomes(
    engine: Engine, *, now: datetime, lookback: timedelta, limit: int = 50_000
) -> int:
    """Insert outcomes for due predictions; returns how many were resolved."""
    with engine.begin() as conn:
        params = {"now": now, "since": now - lookback, "limit": limit}
        return len(conn.execute(RESOLVE_SQL, params).fetchall())

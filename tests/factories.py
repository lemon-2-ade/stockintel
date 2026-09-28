"""Test data builders shared across the suite (importable as ``factories``)."""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

from shared.schemas import BarInterval, MarketBarEvent

BarFactory = Callable[..., MarketBarEvent]
KwargsFactory = Callable[..., dict[str, Any]]


def bar_kwargs(**overrides: Any) -> dict[str, Any]:
    """Keyword arguments for a valid 1-minute AAPL MarketBarEvent, with overrides."""
    base: dict[str, Any] = {
        "source": "test",
        "symbol": "AAPL",
        "timestamp": datetime(2026, 1, 5, 14, 30, tzinfo=UTC),
        "interval": BarInterval.M1,
        "open": 100.0,
        "high": 101.5,
        "low": 99.5,
        "close": 101.0,
        "volume": 12_345,
    }
    return base | overrides


def make_bar(**overrides: Any) -> MarketBarEvent:
    return MarketBarEvent(**bar_kwargs(**overrides))

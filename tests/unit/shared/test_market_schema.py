from __future__ import annotations

import math
from datetime import UTC, datetime, timedelta, timezone
from typing import Any

import pytest
from factories import BarFactory, KwargsFactory
from pydantic import ValidationError

from shared.schemas import BarInterval, EnrichedBarEvent, IndicatorSnapshot, MarketBarEvent


def test_valid_bar_has_envelope_defaults(make_bar: BarFactory) -> None:
    bar = make_bar()
    assert bar.event_type == "market.bar"
    assert bar.schema_version == 1
    assert bar.event_id.version == 4
    assert bar.produced_at.tzinfo is UTC
    assert bar.partition_key() == "AAPL"
    assert bar.bar_end == bar.timestamp + timedelta(minutes=1)


def test_timestamps_are_normalised_to_utc(make_bar: BarFactory) -> None:
    ist = timezone(timedelta(hours=5, minutes=30))
    bar = make_bar(timestamp=datetime(2026, 1, 5, 20, 0, tzinfo=ist))
    assert bar.timestamp == datetime(2026, 1, 5, 14, 30, tzinfo=UTC)
    assert bar.timestamp.utcoffset() == timedelta(0)


def test_naive_timestamp_is_rejected(bar_kwargs: KwargsFactory) -> None:
    with pytest.raises(ValidationError, match="timezone"):
        MarketBarEvent(**bar_kwargs(timestamp=datetime(2026, 1, 5, 14, 30)))  # noqa: DTZ001


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"high": 100.5, "close": 101.0}, "high"),  # high below close
        ({"low": 100.5}, "low"),  # low above open
        ({"high": 99.0, "low": 99.5, "open": 99.2, "close": 99.3}, "high"),  # high < low
        ({"open": 0.0}, "greater than 0"),
        ({"close": -1.0}, "greater than 0"),
        ({"volume": -1}, "greater than or equal to 0"),
        ({"open": math.nan}, "finite"),
        ({"high": math.inf}, "finite"),
        ({"symbol": "aapl"}, "pattern"),
        ({"symbol": ""}, "pattern"),
        ({"symbol": "TOO-LONG-SYMBOL-X"}, "pattern"),
        ({"interval": "7m"}, "interval"),
        ({"source": ""}, "at least 1"),
    ],
)
def test_invalid_bars_are_rejected(
    bar_kwargs: KwargsFactory, overrides: dict[str, Any], message: str
) -> None:
    with pytest.raises(ValidationError, match=message):
        MarketBarEvent(**bar_kwargs(**overrides))


def test_flat_bar_is_valid(make_bar: BarFactory) -> None:
    """A zero-range bar (no trades moved the price) is legitimate, not an error."""
    bar = make_bar(open=50.0, high=50.0, low=50.0, close=50.0, volume=0)
    assert bar.high == bar.low


def test_symbols_with_class_suffix_are_accepted(make_bar: BarFactory) -> None:
    assert make_bar(symbol="BRK.B").symbol == "BRK.B"


def test_events_are_immutable(make_bar: BarFactory) -> None:
    bar = make_bar()
    with pytest.raises(ValidationError):
        bar.close = 1.0  # type: ignore[misc]


def test_unknown_fields_are_ignored_for_forward_compatibility(bar_kwargs: KwargsFactory) -> None:
    """Tolerant reader: an additive, optional field from a newer producer must not break us."""
    payload = bar_kwargs() | {"exchange_mic": "XNAS"}
    bar = MarketBarEvent.model_validate(payload)
    assert "exchange_mic" not in bar.model_dump()


@pytest.mark.parametrize(("interval", "seconds"), [("1s", 1), ("1m", 60), ("1h", 3600)])
def test_interval_durations(interval: str, seconds: int) -> None:
    assert BarInterval(interval).duration.total_seconds() == seconds


def test_every_interval_has_a_duration() -> None:
    for interval in BarInterval:
        assert interval.duration > timedelta(0)


def test_enriched_event_allows_unwarmed_indicators(make_bar: BarFactory) -> None:
    raw = make_bar(trace_id="trace-1")
    enriched = EnrichedBarEvent.from_bar(
        raw,
        indicators=IndicatorSnapshot(return_1=0.01),
        indicator_version="1.0.0",
        source="stream-processor",
    )
    assert enriched.indicators.sma_20 is None
    assert enriched.partition_key() == "AAPL"
    assert enriched.event_type == "market.enriched"
    assert enriched.source_event_id == raw.event_id
    assert enriched.event_id != raw.event_id
    again = EnrichedBarEvent.from_bar(
        raw, indicators=IndicatorSnapshot(), indicator_version="1.0.0", source="other"
    )
    assert again.event_id == enriched.event_id, "re-processing yields the same event id"
    assert enriched.trace_id == "trace-1"
    assert (enriched.open, enriched.close, enriched.volume) == (raw.open, raw.close, raw.volume)


def test_rsi_bounds_are_enforced() -> None:
    with pytest.raises(ValidationError):
        IndicatorSnapshot(rsi_14=101.0)

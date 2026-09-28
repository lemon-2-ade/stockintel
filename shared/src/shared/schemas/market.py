"""Raw market observations (topic ``market.raw``) and enriched bars (``market.enriched``)."""

from __future__ import annotations

from datetime import datetime, timedelta
from enum import StrEnum
from typing import Literal, Self
from uuid import UUID

from pydantic import Field, model_validator

from shared.schemas.base import (
    BaseEvent,
    EventModel,
    FiniteFloat,
    Identifier,
    NonNegativeInt,
    Price,
    Symbol,
    UtcDatetime,
)


class BarInterval(StrEnum):
    """Supported OHLCV bar durations."""

    S1 = "1s"
    S5 = "5s"
    M1 = "1m"
    M5 = "5m"
    M15 = "15m"
    H1 = "1h"
    D1 = "1d"

    @property
    def duration(self) -> timedelta:
        return _INTERVAL_DURATIONS[self]


_INTERVAL_DURATIONS: dict[BarInterval, timedelta] = {
    BarInterval.S1: timedelta(seconds=1),
    BarInterval.S5: timedelta(seconds=5),
    BarInterval.M1: timedelta(minutes=1),
    BarInterval.M5: timedelta(minutes=5),
    BarInterval.M15: timedelta(minutes=15),
    BarInterval.H1: timedelta(hours=1),
    BarInterval.D1: timedelta(days=1),
}


class OHLCVFields(EventModel):
    """OHLCV columns plus the invariants every bar must satisfy.

    ``timestamp`` is the **bar open time** (event time). A bar covers the
    half-open interval ``[timestamp, timestamp + interval)`` and is only
    published once it has closed, so ``close`` is final.
    """

    symbol: Symbol
    timestamp: UtcDatetime = Field(description="Bar open time (event time), UTC.")
    interval: BarInterval
    open: Price
    high: Price
    low: Price
    close: Price
    volume: NonNegativeInt

    @model_validator(mode="after")
    def _check_ohlc_consistency(self) -> Self:
        if self.high < max(self.open, self.close, self.low):
            raise ValueError(
                f"high ({self.high}) must be >= open ({self.open}), close ({self.close}) "
                f"and low ({self.low})"
            )
        if self.low > min(self.open, self.close):
            raise ValueError(
                f"low ({self.low}) must be <= open ({self.open}) and close ({self.close})"
            )
        return self

    @property
    def bar_end(self) -> datetime:
        return self.timestamp + self.interval.duration


class MarketBarEvent(BaseEvent, OHLCVFields):
    """A closed OHLCV bar as observed from a market-data provider or the simulator."""

    event_type: Literal["market.bar"] = "market.bar"
    schema_version: Literal[1] = 1

    def partition_key(self) -> str:
        """All events for one symbol land on one partition, preserving their order."""
        return self.symbol


class IndicatorSnapshot(EventModel):
    """Deterministic technical indicators computed over bounded rolling windows.

    Every field is optional: an indicator is ``None`` until its window has
    warmed up (e.g. ``sma_20`` needs 20 bars). Consumers must treat ``None`` as
    "not yet available", never as zero.
    """

    return_1: FiniteFloat | None = Field(
        default=None, description="Simple return vs previous close."
    )
    log_return_1: FiniteFloat | None = None
    price_change: FiniteFloat | None = Field(default=None, description="close - previous close.")
    volume_change_pct: FiniteFloat | None = None
    sma_20: FiniteFloat | None = None
    ema_12: FiniteFloat | None = None
    ema_26: FiniteFloat | None = None
    volatility_20: FiniteFloat | None = Field(
        default=None, description="Rolling std-dev of 1-bar log returns (not annualised)."
    )
    volume_sma_20: FiniteFloat | None = None
    volume_ratio: FiniteFloat | None = Field(default=None, description="volume / volume_sma_20.")
    rsi_14: FiniteFloat | None = Field(default=None, ge=0, le=100)
    macd: FiniteFloat | None = None
    macd_signal: FiniteFloat | None = None
    macd_hist: FiniteFloat | None = None
    bb_upper: FiniteFloat | None = None
    bb_middle: FiniteFloat | None = None
    bb_lower: FiniteFloat | None = None
    bb_width: FiniteFloat | None = None


class EnrichedBarEvent(BaseEvent, OHLCVFields):
    """A raw bar plus the indicators the stream processor derived from it.

    Carries the full OHLCV so downstream consumers (feature pipeline, API
    cache writer) can work from this topic alone.
    """

    event_type: Literal["market.enriched"] = "market.enriched"
    schema_version: Literal[1] = 1
    source_event_id: UUID = Field(description="event_id of the market.bar this was derived from.")
    indicators: IndicatorSnapshot
    indicator_version: Identifier = Field(description="Version of the indicator implementation.")
    processing_latency_ms: float | None = Field(
        default=None,
        ge=0,
        description="Wall-clock time from raw event production to enrichment.",
    )

    @classmethod
    def from_bar(
        cls,
        bar: MarketBarEvent,
        *,
        indicators: IndicatorSnapshot,
        indicator_version: str,
        source: str,
        processing_latency_ms: float | None = None,
    ) -> EnrichedBarEvent:
        """Derive an enriched event, carrying over OHLCV and the trace id."""
        return cls(
            source=source,
            trace_id=bar.trace_id,
            **bar.model_dump(include=set(OHLCVFields.model_fields)),
            source_event_id=bar.event_id,
            indicators=indicators,
            indicator_version=indicator_version,
            processing_latency_ms=processing_latency_ms,
        )

    def partition_key(self) -> str:
        return self.symbol

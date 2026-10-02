"""Primitive types and the base class shared by every Kafka event.

Design notes
------------
* **Tolerant reader** - models use ``extra="ignore"``. Adding an *optional*
  field is a backwards-compatible change that does not bump
  ``schema_version``; old consumers simply ignore it. Producers are still
  protected against typos because the pydantic mypy plugin forbids unknown
  ``__init__`` kwargs at type-check time.
* **Breaking changes** (removing/renaming a field, changing a type or its
  meaning) require a new ``schema_version`` and a new model class registered
  alongside the old one, so both can be decoded during a rollout.
* **Immutability** - events are facts; models are frozen.
* **Timezones** - all timestamps must be timezone-aware and are normalised to
  UTC. Naive datetimes are rejected rather than guessed.
"""

from __future__ import annotations

import math
from datetime import UTC, datetime
from typing import Annotated
from uuid import NAMESPACE_URL, UUID, uuid4, uuid5

from pydantic import (
    AfterValidator,
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    StringConstraints,
)

SYMBOL_PATTERN = r"^[A-Z][A-Z0-9.\-]{0,14}$"


_DERIVED_NAMESPACE = uuid5(NAMESPACE_URL, "https://stockintel.local/derived-events")


def derived_event_id(source_event_id: UUID, *parts: str) -> UUID:
    """Deterministic id for an event derived from another one.

    Re-processing the same input (Kafka redelivery, replay after a crash)
    yields the same id, so downstream consumers can de-duplicate by
    ``event_id`` instead of seeing a "new" event each time.
    """
    return uuid5(_DERIVED_NAMESPACE, ":".join([str(source_event_id), *parts]))


def utcnow() -> datetime:
    """Current time as a timezone-aware UTC datetime."""
    return datetime.now(UTC)


def _to_utc(value: datetime) -> datetime:
    return value.astimezone(UTC)


def _finite(value: float) -> float:
    if not math.isfinite(value):
        raise ValueError("value must be a finite number")
    return value


Symbol = Annotated[
    str,
    StringConstraints(pattern=SYMBOL_PATTERN),
    Field(description="Upper-case ticker, e.g. AAPL or BRK.B. Producers must normalise case."),
]
"""Ticker symbol. Deliberately strict: ``aapl`` is rejected, not upper-cased,
so that two producers can never disagree on the partition key."""

UtcDatetime = Annotated[AwareDatetime, AfterValidator(_to_utc)]
"""Timezone-aware datetime, normalised to UTC. Naive values are rejected."""

Price = Annotated[float, Field(gt=0, allow_inf_nan=False)]
"""Strictly positive, finite price.

``float`` (not ``Decimal``) is intentional: this platform does analytics, not
ledger accounting, and every downstream consumer (NumPy, pandas, the ML model)
works in IEEE-754 doubles anyway."""

FiniteFloat = Annotated[float, AfterValidator(_finite)]
Probability = Annotated[float, Field(ge=0.0, le=1.0, allow_inf_nan=False)]
NonNegativeInt = Annotated[int, Field(ge=0)]
Identifier = Annotated[str, StringConstraints(min_length=1, max_length=128)]


class EventModel(BaseModel):
    """Configuration shared by events and the value objects nested inside them."""

    model_config = ConfigDict(
        frozen=True,
        extra="ignore",
        validate_default=True,
        use_enum_values=False,
    )


class BaseEvent(EventModel):
    """Envelope fields present on every event.

    Subclasses pin ``event_type`` and ``schema_version`` with ``Literal`` types
    so that ``(event_type, schema_version)`` uniquely identifies a model class.
    """

    event_id: UUID = Field(default_factory=uuid4, description="Globally unique, used for dedup.")
    event_type: str
    schema_version: int
    produced_at: UtcDatetime = Field(
        default_factory=utcnow,
        description="Processing time at which the producer created the event.",
    )
    source: Identifier = Field(
        description="Producer identity, e.g. 'simulator' or 'stream-processor'."
    )
    trace_id: str | None = Field(
        default=None,
        max_length=64,
        description="Correlation id propagated across services for log stitching.",
    )

    @classmethod
    def type_key(cls) -> tuple[str, int]:
        """``(event_type, schema_version)`` read from the subclass's Literal defaults."""
        event_type = cls.model_fields["event_type"].default
        version = cls.model_fields["schema_version"].default
        if not isinstance(event_type, str) or not isinstance(version, int):
            raise TypeError(f"{cls.__name__} must pin event_type and schema_version defaults")
        return event_type, version

    def partition_key(self) -> str:
        """Kafka message key. Subclasses keyed by symbol override this."""
        return str(self.event_id)

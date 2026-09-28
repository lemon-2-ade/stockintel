"""Dead-letter envelope (topic ``market.dead-letter``).

A poison message must never block a partition, and must never be silently
dropped. The consumer that cannot process it wraps the *original bytes*
(base64, so non-UTF-8 garbage survives) together with enough coordinates to
find, replay or discard it later.
"""

from __future__ import annotations

import base64
from enum import StrEnum
from typing import Literal

from pydantic import Field

from shared.schemas.base import BaseEvent, Identifier, UtcDatetime, utcnow

MAX_ERROR_MESSAGE_LENGTH = 2_000


class FailureReason(StrEnum):
    DESERIALIZATION = "deserialization"
    """Bytes are not valid JSON / not decodable."""
    UNKNOWN_EVENT_TYPE = "unknown_event_type"
    """Decodable, but ``(event_type, schema_version)`` is not registered."""
    VALIDATION = "validation"
    """Decodable and known, but violates the schema's invariants."""
    PROCESSING = "processing"
    """Valid event whose processing kept failing after bounded retries."""


class DeadLetterEvent(BaseEvent):
    event_type: Literal["dead_letter"] = "dead_letter"
    schema_version: Literal[1] = 1

    original_topic: Identifier
    original_partition: int = Field(ge=0)
    original_offset: int = Field(ge=0)
    original_key: str | None = None
    original_value_b64: str = Field(description="Original message value, base64-encoded.")
    reason: FailureReason
    error_message: str = Field(max_length=MAX_ERROR_MESSAGE_LENGTH)
    consumer_group: Identifier
    attempts: int = Field(ge=1)
    failed_at: UtcDatetime = Field(default_factory=utcnow)

    @classmethod
    def from_failure(
        cls,
        *,
        topic: str,
        partition: int,
        offset: int,
        key: bytes | None,
        value: bytes | None,
        reason: FailureReason,
        error: BaseException | str,
        consumer_group: str,
        source: str,
        attempts: int = 1,
    ) -> DeadLetterEvent:
        message = str(error)
        if len(message) > MAX_ERROR_MESSAGE_LENGTH:
            message = message[: MAX_ERROR_MESSAGE_LENGTH - 3] + "..."
        return cls(
            source=source,
            original_topic=topic,
            original_partition=partition,
            original_offset=offset,
            original_key=key.decode("utf-8", errors="replace") if key is not None else None,
            original_value_b64=base64.b64encode(value or b"").decode("ascii"),
            reason=reason,
            error_message=message,
            consumer_group=consumer_group,
            attempts=attempts,
        )

    def original_value(self) -> bytes:
        return base64.b64decode(self.original_value_b64)

    def partition_key(self) -> str:
        # Keep a poisoned symbol's failures together; fall back to the topic.
        return self.original_key or self.original_topic

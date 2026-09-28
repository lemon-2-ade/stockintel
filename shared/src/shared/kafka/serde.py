"""Event (de)serialisation for Kafka message values.

JSON is the wire format for now (see ``docs/adr/0002-json-events-first.md``).
Everything wire-format specific sits behind the :class:`EventSerde` protocol,
so moving to Avro/Protobuf + Schema Registry means adding one implementation,
not touching producers or consumers.

This module deliberately does **not** import ``confluent_kafka``: it works on
plain ``bytes`` and can be unit-tested without a broker.
"""

from __future__ import annotations

import json
from typing import Any, Protocol, TypeVar

from pydantic import ValidationError

from shared.schemas.base import BaseEvent
from shared.schemas.dead_letter import FailureReason
from shared.schemas.registry import UnknownEventTypeError, model_for

E = TypeVar("E", bound=BaseEvent)

HEADER_EVENT_TYPE = "event_type"
HEADER_SCHEMA_VERSION = "schema_version"
HEADER_CONTENT_TYPE = "content-type"
HEADER_TRACE_ID = "trace_id"

Headers = list[tuple[str, bytes]]


class EventDeserializationError(ValueError):
    """The bytes could not be turned into a valid, known event.

    ``reason`` tells the consumer which dead-letter category applies.
    """

    def __init__(self, reason: FailureReason, message: str) -> None:
        super().__init__(message)
        self.reason = reason


class EventSerde(Protocol):
    content_type: str

    def serialize(self, event: BaseEvent) -> bytes: ...

    def headers(self, event: BaseEvent) -> Headers: ...

    def deserialize(self, value: bytes | None, expected: type[E]) -> E: ...

    def deserialize_any(self, value: bytes | None) -> BaseEvent: ...


class JsonEventSerde:
    """UTF-8 JSON values; type/version also mirrored into Kafka headers.

    Headers let infrastructure (routing, metrics, DLQ tooling) identify an
    event without parsing the payload; the payload remains self-describing so
    it is still decodable if headers are stripped (e.g. by a bridge/mirror).
    """

    content_type = "application/json"

    def serialize(self, event: BaseEvent) -> bytes:
        return event.model_dump_json().encode("utf-8")

    def headers(self, event: BaseEvent) -> Headers:
        headers: Headers = [
            (HEADER_EVENT_TYPE, event.event_type.encode()),
            (HEADER_SCHEMA_VERSION, str(event.schema_version).encode()),
            (HEADER_CONTENT_TYPE, self.content_type.encode()),
        ]
        if event.trace_id:
            headers.append((HEADER_TRACE_ID, event.trace_id.encode()))
        return headers

    def deserialize_any(self, value: bytes | None) -> BaseEvent:
        payload = self._decode(value)
        event_type = payload.get("event_type")
        schema_version = payload.get("schema_version")
        if not isinstance(event_type, str) or not isinstance(schema_version, int):
            raise EventDeserializationError(
                FailureReason.VALIDATION,
                "payload must contain string 'event_type' and integer 'schema_version'",
            )
        try:
            model = model_for(event_type, schema_version)
        except UnknownEventTypeError as exc:
            raise EventDeserializationError(FailureReason.UNKNOWN_EVENT_TYPE, str(exc)) from exc
        return self._validate(model, payload)

    def deserialize(self, value: bytes | None, expected: type[E]) -> E:
        event = self.deserialize_any(value)
        if not isinstance(event, expected):
            raise EventDeserializationError(
                FailureReason.UNKNOWN_EVENT_TYPE,
                f"expected {expected.__name__}, got {type(event).__name__} "
                f"({event.event_type} v{event.schema_version})",
            )
        return event

    @staticmethod
    def _decode(value: bytes | None) -> dict[str, Any]:
        if not value:
            raise EventDeserializationError(
                FailureReason.DESERIALIZATION, "empty message value (unexpected tombstone?)"
            )
        try:
            payload = json.loads(value)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise EventDeserializationError(
                FailureReason.DESERIALIZATION, f"invalid JSON: {exc}"
            ) from exc
        if not isinstance(payload, dict):
            raise EventDeserializationError(
                FailureReason.DESERIALIZATION,
                f"expected a JSON object, got {type(payload).__name__}",
            )
        return payload

    @staticmethod
    def _validate(model: type[BaseEvent], payload: dict[str, Any]) -> BaseEvent:
        try:
            return model.model_validate(payload)
        except ValidationError as exc:
            # include_url/input=False keeps potentially large payloads out of logs & DLQ.
            summary = "; ".join(
                f"{'.'.join(str(p) for p in err['loc']) or '<root>'}: {err['msg']}"
                for err in exc.errors(include_url=False, include_input=False)
            )
            raise EventDeserializationError(FailureReason.VALIDATION, summary) from exc

from __future__ import annotations

import json

import pytest
from factories import BarFactory

from shared.kafka.serde import (
    HEADER_CONTENT_TYPE,
    HEADER_EVENT_TYPE,
    HEADER_SCHEMA_VERSION,
    HEADER_TRACE_ID,
    EventDeserializationError,
    EventSerde,
    JsonEventSerde,
)
from shared.schemas import AnomalyEvent, FailureReason, MarketBarEvent

serde = JsonEventSerde()


def test_json_serde_satisfies_protocol() -> None:
    impl: EventSerde = serde
    assert impl.content_type == "application/json"


def test_round_trip_is_lossless(make_bar: BarFactory) -> None:
    bar = make_bar(trace_id="abc")
    decoded = serde.deserialize(serde.serialize(bar), MarketBarEvent)
    assert decoded == bar


def test_wire_format_is_self_describing(make_bar: BarFactory) -> None:
    payload = json.loads(serde.serialize(make_bar()))
    assert payload["event_type"] == "market.bar"
    assert payload["schema_version"] == 1
    assert payload["timestamp"] == "2026-01-05T14:30:00Z"
    assert payload["interval"] == "1m"


def test_headers_mirror_type_and_version(make_bar: BarFactory) -> None:
    headers = dict(serde.headers(make_bar(trace_id="t-1")))
    assert headers[HEADER_EVENT_TYPE] == b"market.bar"
    assert headers[HEADER_SCHEMA_VERSION] == b"1"
    assert headers[HEADER_CONTENT_TYPE] == b"application/json"
    assert headers[HEADER_TRACE_ID] == b"t-1"
    assert HEADER_TRACE_ID not in dict(serde.headers(make_bar()))


def test_deserialize_any_dispatches_on_type(make_bar: BarFactory) -> None:
    assert isinstance(serde.deserialize_any(serde.serialize(make_bar())), MarketBarEvent)


@pytest.mark.parametrize(
    ("value", "reason"),
    [
        (None, FailureReason.DESERIALIZATION),
        (b"", FailureReason.DESERIALIZATION),
        (b"\xff\xfe", FailureReason.DESERIALIZATION),
        (b"{not json", FailureReason.DESERIALIZATION),
        (b"[1, 2, 3]", FailureReason.DESERIALIZATION),
        (b'{"symbol": "AAPL"}', FailureReason.VALIDATION),
        (b'{"event_type": "market.bar", "schema_version": "1"}', FailureReason.VALIDATION),
        (b'{"event_type": "market.bar", "schema_version": 42}', FailureReason.UNKNOWN_EVENT_TYPE),
        (b'{"event_type": "nope", "schema_version": 1}', FailureReason.UNKNOWN_EVENT_TYPE),
    ],
)
def test_bad_payloads_are_classified(value: bytes | None, reason: FailureReason) -> None:
    with pytest.raises(EventDeserializationError) as info:
        serde.deserialize_any(value)
    assert info.value.reason is reason


def test_schema_violations_are_reported_compactly(make_bar: BarFactory) -> None:
    payload = json.loads(serde.serialize(make_bar()))
    payload["high"] = 1.0  # below open/close -> OHLC invariant broken
    payload["volume"] = -5
    with pytest.raises(EventDeserializationError) as info:
        serde.deserialize_any(json.dumps(payload).encode())
    assert info.value.reason is FailureReason.VALIDATION
    assert "volume" in str(info.value)
    assert "https://" not in str(info.value)  # no pydantic doc URLs in DLQ records


def test_expected_type_mismatch_is_rejected(make_bar: BarFactory) -> None:
    with pytest.raises(EventDeserializationError, match="expected AnomalyEvent"):
        serde.deserialize(serde.serialize(make_bar()), AnomalyEvent)

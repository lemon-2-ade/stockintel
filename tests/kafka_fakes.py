"""In-memory stand-in for confluent_kafka.Producer (async delivery via poll/flush)."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from market_producer.publisher import Headers


@dataclass
class FakeError:
    code: str

    def name(self) -> str:
        return self.code

    def __str__(self) -> str:
        return self.code


@dataclass
class Sent:
    topic: str
    key: bytes
    value: bytes
    headers: Headers


class FakeProducer:
    def __init__(self, *, capacity: int = 10_000, fail_keys: set[bytes] | None = None) -> None:
        self.capacity = capacity
        self.fail_keys = fail_keys or set()
        self.sent: list[Sent] = []
        self._pending: list[tuple[bytes, Callable[[Any, Any], None]]] = []
        self.buffer_errors_to_raise = 0

    def produce(
        self,
        topic: str,
        *,
        key: bytes,
        value: bytes,
        headers: Headers,
        on_delivery: Callable[[Any, Any], None],
    ) -> None:
        if self.buffer_errors_to_raise > 0 or len(self._pending) >= self.capacity:
            self.buffer_errors_to_raise = max(0, self.buffer_errors_to_raise - 1)
            raise BufferError("Local: Queue full")
        self.sent.append(Sent(topic, key, value, headers))
        self._pending.append((key, on_delivery))

    def poll(self, timeout: float = 0) -> int:
        served = len(self._pending)
        for key, callback in self._pending:
            callback(FakeError("_MSG_TIMED_OUT") if key in self.fail_keys else None, None)
        self._pending.clear()
        return served

    def flush(self, timeout: float = 0) -> int:
        self.poll()
        return 0

    def __len__(self) -> int:
        return len(self._pending)


@dataclass
class Msg:
    _value: bytes | None
    _offset: int
    _partition: int = 0
    _topic: str = "market.raw"
    _key: bytes | None = b"AAPL"
    _headers: list[tuple[str, bytes]] = field(default_factory=list)
    _error: Any = None

    def error(self) -> Any:
        return self._error

    def topic(self) -> str:
        return self._topic

    def partition(self) -> int:
        return self._partition

    def offset(self) -> int:
        return self._offset

    def key(self) -> bytes | None:
        return self._key

    def value(self) -> bytes | None:
        return self._value

    def headers(self) -> list[tuple[str, bytes]]:
        return self._headers


@dataclass
class TP:
    topic: str
    partition: int
    offset: int = -1001


class FakeConsumer:
    def __init__(self, committed: dict[int, int] | None = None, low: int = 0) -> None:
        self.stored: list[tuple[int, int]] = []
        self.assigned: list[TP] = []
        self._committed = committed or {}
        self._low = low

    def store_offsets(self, message: Msg) -> None:
        self.stored.append((message.partition(), message.offset()))

    def committed(self, partitions: list[TP], timeout: float) -> list[TP]:
        return [
            TP(p.topic, p.partition, self._committed.get(p.partition, -1001)) for p in partitions
        ]

    def get_watermark_offsets(
        self, tp: TP, timeout: float = 0, cached: bool = False
    ) -> tuple[int, int]:
        return self._low, 10_000

    def incremental_assign(self, partitions: list[TP]) -> None:
        self.assigned = partitions

    def commit(self, asynchronous: bool = False) -> None:
        pass

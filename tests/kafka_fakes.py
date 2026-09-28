"""In-memory stand-in for confluent_kafka.Producer (async delivery via poll/flush)."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
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

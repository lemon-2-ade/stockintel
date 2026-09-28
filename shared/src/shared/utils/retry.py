"""Bounded retries with exponential backoff and full jitter.

Used wherever a dependency may be temporarily unavailable (Kafka at startup,
Postgres fail-over, the inference service). Retries are always *bounded*: an
unbounded retry loop just converts an outage into a hang.
"""

from __future__ import annotations

import random
import time
from collections.abc import Callable
from dataclasses import dataclass

import structlog

_log = structlog.get_logger(__name__)


@dataclass(frozen=True, slots=True)
class BackoffPolicy:
    max_attempts: int = 5
    base_delay_s: float = 0.2
    max_delay_s: float = 10.0
    multiplier: float = 2.0

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")
        if self.base_delay_s < 0 or self.max_delay_s < self.base_delay_s:
            raise ValueError("require 0 <= base_delay_s <= max_delay_s")

    def delay_for(self, attempt: int, rng: random.Random | None = None) -> float:
        """Full-jitter delay before retry number ``attempt`` (1-based).

        Full jitter (uniform in ``[0, cap]``) avoids synchronized retry storms
        when many replicas lose the same dependency at the same moment.
        """
        cap = min(self.max_delay_s, self.base_delay_s * self.multiplier ** (attempt - 1))
        return (rng or random).uniform(0.0, cap)  # jitter only; not cryptographic


def retry_call[T](
    fn: Callable[[], T],
    *,
    policy: BackoffPolicy,
    retry_on: tuple[type[BaseException], ...] = (Exception,),
    operation: str = "operation",
    sleep: Callable[[float], None] = time.sleep,
    rng: random.Random | None = None,
) -> T:
    """Call ``fn`` until it succeeds or ``policy.max_attempts`` is exhausted.

    The last exception is re-raised unchanged so callers can decide whether to
    dead-letter, degrade or crash.
    """
    for attempt in range(1, policy.max_attempts + 1):
        try:
            return fn()
        except retry_on as exc:
            if attempt == policy.max_attempts:
                _log.error("retry.exhausted", operation=operation, attempts=attempt, error=str(exc))
                raise
            delay = policy.delay_for(attempt, rng)
            _log.warning(
                "retry.scheduled",
                operation=operation,
                attempt=attempt,
                max_attempts=policy.max_attempts,
                delay_s=round(delay, 3),
                error=str(exc),
            )
            sleep(delay)
    raise AssertionError("unreachable")  # pragma: no cover

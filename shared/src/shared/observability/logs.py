"""Structured JSON logging shared by all Python services.

Every line is a JSON object with at least ``timestamp``, ``level``,
``service`` and ``event``. Request/event-scoped fields (``event_id``,
``symbol``, ``trace_id``) are bound with ``structlog.contextvars`` so they
propagate through a handler without being passed around explicitly.
"""

from __future__ import annotations

import logging
import sys
import time
from collections.abc import Callable, MutableMapping
from typing import Any

import structlog

_SENSITIVE_MARKERS = ("password", "secret", "token", "api_key", "apikey", "authorization")
REDACTED = "***REDACTED***"


def redact_secrets(
    _logger: Any, _method: str, event_dict: MutableMapping[str, Any]
) -> MutableMapping[str, Any]:
    """Defence in depth: mask values whose *key* looks like a credential."""
    for key in list(event_dict):
        if any(marker in key.lower() for marker in _SENSITIVE_MARKERS):
            event_dict[key] = REDACTED
    return event_dict


def configure_logging(service: str, *, level: str = "INFO", fmt: str = "json") -> None:
    """Configure structlog + stdlib logging once per process."""
    shared_processors: list[structlog.types.Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.processors.TimeStamper(fmt="iso", utc=True, key="timestamp"),
        structlog.processors.CallsiteParameterAdder(
            {structlog.processors.CallsiteParameter.MODULE}
        ),
        redact_secrets,
    ]
    renderer: structlog.types.Processor = (
        structlog.processors.JSONRenderer()
        if fmt == "json"
        else structlog.dev.ConsoleRenderer(colors=sys.stderr.isatty())
    )

    structlog.configure(
        processors=[
            *shared_processors,
            structlog.processors.format_exc_info,
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    formatter = structlog.stdlib.ProcessorFormatter(
        foreign_pre_chain=shared_processors,
        processors=[
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            structlog.processors.format_exc_info,
            renderer,
        ],
    )
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(level)

    structlog.contextvars.clear_contextvars()
    structlog.contextvars.bind_contextvars(service=service)


def get_logger(name: str | None = None) -> structlog.stdlib.BoundLogger:
    logger: structlog.stdlib.BoundLogger = structlog.get_logger(name)
    return logger


class LogThrottle:
    """Allow one log line per key per ``interval_s``; report how many were suppressed.

    librdkafka reports the same connectivity error on every reconnect attempt
    (dozens per second while a broker is down). Logging each one buries
    everything else; logging none hides the outage.
    """

    def __init__(
        self, interval_s: float = 30.0, clock: Callable[[], float] = time.monotonic
    ) -> None:
        self._interval_s = interval_s
        self._clock = clock
        self._last: dict[str, float] = {}
        self._suppressed: dict[str, int] = {}

    def allow(self, key: str) -> int | None:
        """Return the number of suppressed occurrences if this one may be logged, else None."""
        now = self._clock()
        last = self._last.get(key)
        if last is not None and now - last < self._interval_s:
            self._suppressed[key] = self._suppressed.get(key, 0) + 1
            return None
        self._last[key] = now
        return self._suppressed.pop(key, 0)


def kafka_error_logger(
    logger: structlog.stdlib.BoundLogger, interval_s: float = 30.0
) -> Callable[[Any], None]:
    """A librdkafka ``error_cb`` that logs each distinct error at most every ``interval_s``."""
    throttle = LogThrottle(interval_s)

    def _on_error(err: Any) -> None:
        code = err.name() if hasattr(err, "name") else str(err)
        suppressed = throttle.allow(str(code))
        if suppressed is not None:
            logger.error("kafka.client_error", error=str(err), suppressed_since_last=suppressed)

    return _on_error

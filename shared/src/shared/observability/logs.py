"""Structured JSON logging shared by all Python services.

Every line is a JSON object with at least ``timestamp``, ``level``,
``service`` and ``event``. Request/event-scoped fields (``event_id``,
``symbol``, ``trace_id``) are bound with ``structlog.contextvars`` so they
propagate through a handler without being passed around explicitly.
"""

from __future__ import annotations

import logging
import sys
from collections.abc import MutableMapping
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

from __future__ import annotations

import json

import pytest

from shared.observability.logs import REDACTED, configure_logging, get_logger, redact_secrets


def test_redaction_masks_credential_like_keys() -> None:
    event = redact_secrets(
        None, "info", {"event": "x", "db_password": "p", "API_KEY": "k", "symbol": "AAPL"}
    )
    assert event == {"event": "x", "db_password": REDACTED, "API_KEY": REDACTED, "symbol": "AAPL"}


def test_json_logs_have_service_and_context(capsys: pytest.CaptureFixture[str]) -> None:
    configure_logging("unit-test", level="INFO", fmt="json")
    get_logger("t").info("bar.processed", symbol="AAPL", token="should-not-leak")  # noqa: S106
    line = capsys.readouterr().out.strip().splitlines()[-1]
    record = json.loads(line)
    assert record["event"] == "bar.processed"
    assert record["service"] == "unit-test"
    assert record["symbol"] == "AAPL"
    assert record["level"] == "info"
    assert record["token"] == REDACTED
    assert "timestamp" in record

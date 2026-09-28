from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest
from pydantic import ValidationError

from market_producer.config import ProducerSettings
from market_producer.main import build_provider
from market_producer.providers.replay import HistoricalReplayProvider
from market_producer.providers.simulated import SimulatorProvider
from shared.schemas import BarInterval

NOW = datetime(2026, 1, 5, 14, 30, tzinfo=UTC)


def test_env_parsing(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PRODUCER_SYMBOLS", "AAPL, MSFT ,NVDA")
    monkeypatch.setenv("PRODUCER_INTERVAL", "1m")
    monkeypatch.setenv("PRODUCER_VOLATILITY_MULTIPLIER", "3")
    settings = ProducerSettings()
    assert settings.symbols == ["AAPL", "MSFT", "NVDA"]
    assert settings.interval is BarInterval.M1
    assert settings.volatility_multiplier == 3


def test_empty_symbol_list_means_default_universe(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PRODUCER_SYMBOLS", "")
    assert ProducerSettings().symbols == []


@pytest.mark.parametrize(
    ("env", "message"),
    [
        ({"PRODUCER_SYMBOLS": "aapl"}, "upper-case"),
        ({"PRODUCER_JUMP_MIN": "0.1", "PRODUCER_JUMP_MAX": "0.05"}, "jump_min"),
        ({"PRODUCER_MODE": "replay"}, "SNAPSHOT_DIR"),
        ({"PRODUCER_VOLATILITY_MULTIPLIER": "0"}, "greater than 0"),
    ],
)
def test_invalid_settings(
    monkeypatch: pytest.MonkeyPatch, env: dict[str, str], message: str
) -> None:
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    with pytest.raises(ValidationError, match=message):
        ProducerSettings()


def test_default_build_uses_calibrated_universe() -> None:
    provider = build_provider(ProducerSettings(backfill_bars=2), now=NOW)
    assert isinstance(provider, SimulatorProvider)
    first = next(provider.batches())
    symbols = [bar.event.symbol for bar in first.bars]
    assert "AAPL" in symbols
    assert len(symbols) == 12
    aapl = next(bar.event for bar in first.bars if bar.event.symbol == "AAPL")
    assert 100 < aapl.open < 1_000, "starts near the calibrated historical price"


def test_num_symbols_and_synthetic_top_up() -> None:
    provider = build_provider(ProducerSettings(num_symbols=14), now=NOW)
    symbols = [bar.event.symbol for bar in next(provider.batches()).bars]
    assert symbols[-2:] == ["SIM001", "SIM002"]


def test_replay_mode(tmp_path: Path) -> None:
    snapshot = tmp_path / "ds" / "rev"
    snapshot.mkdir(parents=True)
    (snapshot / "AAPL.csv").write_text(
        "open,high,low,close,volume,timestamp\n1,2,0.5,1.5,10,1262615400\n"
    )
    settings = ProducerSettings(mode="replay", replay_snapshot_dir=snapshot, symbols=["AAPL"])
    provider = build_provider(settings, now=NOW)
    assert isinstance(provider, HistoricalReplayProvider)
    (batch,) = list(provider.batches())
    assert batch.bars[0].event.source == "replay:ds"

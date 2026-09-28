from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from market_producer.calibration import DEFAULT_PARAMS
from market_producer.providers.replay import HistoricalReplayProvider
from market_producer.providers.simulated import SimulatorProvider, aligned_start
from market_producer.simulator import MarketSimulator, SymbolSimulator
from shared.schemas import BarInterval


def test_aligned_start_puts_backfill_in_the_past() -> None:
    now = datetime(2026, 1, 5, 14, 30, 17, 500_000, tzinfo=UTC)
    start = aligned_start(now, BarInterval.S5, backfill_bars=10)
    assert start == datetime(2026, 1, 5, 14, 29, 25, tzinfo=UTC)  # 14:30:15 - 50s
    assert aligned_start(now, BarInterval.M1, 0) == datetime(2026, 1, 5, 14, 30, tzinfo=UTC)
    with pytest.raises(ValueError, match="backfill"):
        aligned_start(now, BarInterval.M1, -1)


def test_simulator_provider_schedule() -> None:
    now = datetime(2026, 1, 5, 14, 30, 0, 400_000, tzinfo=UTC)
    sims = [
        SymbolSimulator(s, DEFAULT_PARAMS, interval=BarInterval.S1, seed=1) for s in ("AAA", "BBB")
    ]
    start = aligned_start(now, BarInterval.S1, backfill_bars=3)
    batches = list(SimulatorProvider(MarketSimulator(sims), start=start, max_bars=5).batches())

    assert len(batches) == 5
    assert all(len(b.bars) == 2 for b in batches)
    opens = [b.bars[0].event.timestamp for b in batches]
    assert opens == [start + timedelta(seconds=i) for i in range(5)]
    # Backfilled bars are already closed -> due now; later bars are due when they close.
    assert [b.due_at <= now.timestamp() for b in batches] == [True, True, True, False, False]
    for b in batches:
        assert b.due_at == (b.bars[0].event.timestamp + timedelta(seconds=1)).timestamp()
        assert b.bars[0].event.timestamp.timestamp() + 1 <= b.due_at


def test_simulator_provider_requires_aware_start() -> None:
    sims = [SymbolSimulator("AAA", DEFAULT_PARAMS, interval=BarInterval.S1, seed=1)]
    with pytest.raises(ValueError, match="timezone"):
        SimulatorProvider(MarketSimulator(sims), start=datetime(2026, 1, 1))  # noqa: DTZ001


def write_csv(path: Path, header: str, rows: list[str]) -> None:
    path.write_text("\n".join([header, *rows]) + "\n")


@pytest.fixture
def snapshot(tmp_path: Path) -> Path:
    write_csv(
        tmp_path / "AAA.csv",
        "low,high,open,volume,close,timestamp",
        [
            "9,11,10,100,10.5,1262615400",  # 2010-01-04
            "9,11,10,100,10.5,1262701800",  # 2010-01-05
            "9,8,10,100,10.5,1262788200",  # 2010-01-06: high < low -> invalid, skipped
            "9,11,10,100,10.5,1262701800",  # duplicate timestamp -> skipped
            "9,11,10,100,10.5,1262874600",  # 2010-01-07
        ],
    )
    write_csv(
        tmp_path / "BBB.csv",
        "close,open,high,low,volume,timestamp",
        ["20,20,21,19,50,1262701800", "20,20,21,19,50,1262874600"],
    )
    return tmp_path


def test_replay_merges_symbols_in_time_order_and_skips_bad_rows(snapshot: Path) -> None:
    provider = HistoricalReplayProvider(
        snapshot, ["AAA", "BBB"], bars_per_second=2.0, start_at=1_000.0, source="replay:test"
    )
    batches = list(provider.batches())
    stamps = [b.bars[0].event.timestamp.date().isoformat() for b in batches]
    assert stamps == ["2010-01-04", "2010-01-05", "2010-01-07"]
    assert [sorted(bar.event.symbol for bar in b.bars) for b in batches] == [
        ["AAA"],
        ["AAA", "BBB"],
        ["AAA", "BBB"],
    ]
    assert [b.due_at for b in batches] == [1_000.0, 1_000.5, 1_001.0]
    assert provider.skipped_rows == 2
    event = batches[0].bars[0].event
    assert (event.open, event.high, event.low, event.close, event.volume) == (10, 11, 9, 10.5, 100)
    assert event.interval is BarInterval.D1
    assert event.source == "replay:test"


def test_replay_limit_and_missing_symbols(snapshot: Path) -> None:
    provider = HistoricalReplayProvider(
        snapshot, ["AAA"], bars_per_second=1.0, start_at=0.0, limit=1
    )
    assert len(list(provider.batches())) == 1
    with pytest.raises(FileNotFoundError, match="ZZZ"):
        HistoricalReplayProvider(snapshot, ["ZZZ"], bars_per_second=1.0, start_at=0.0)

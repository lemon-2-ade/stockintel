from __future__ import annotations

from datetime import UTC, datetime, timedelta

from factories import make_bar

from shared.schemas import BarInterval, MarketBarEvent
from stream_processor.processor import BarProcessor, Outcome

T0 = datetime(2026, 1, 5, 14, 30, tzinfo=UTC)


def bar(i: int, symbol: str = "AAPL", close: float = 100.0, **kw: object) -> MarketBarEvent:
    return make_bar(
        symbol=symbol,
        timestamp=T0 + timedelta(minutes=i),
        open=close,
        high=close,
        low=close,
        close=close,
        **kw,
    )


def test_enriched_event_is_derived_and_deterministic() -> None:
    raw = bar(0)
    a = BarProcessor().process(raw)
    b = BarProcessor().process(raw)
    assert a.outcome is Outcome.PROCESSED
    assert a.enriched is not None
    assert b.enriched is not None
    assert a.enriched.event_id == b.enriched.event_id
    assert a.enriched.source_event_id == raw.event_id
    assert a.enriched.processing_latency_ms is not None


def test_duplicates_are_not_folded_in_twice() -> None:
    proc = BarProcessor()
    first = bar(0)
    proc.process(first)
    assert proc.process(first).outcome is Outcome.DUPLICATE
    resend = bar(0, close=100.0)  # same timestamp, new event id
    assert proc.process(resend).outcome is Outcome.DUPLICATE


def test_redelivery_does_not_change_indicators() -> None:
    bars = [bar(i, close=100 + i * 0.5) for i in range(40)]
    clean, noisy = BarProcessor(), BarProcessor()
    clean_out = [clean.process(b).enriched for b in bars]
    noisy_out = []
    for i, b in enumerate(bars):
        result = noisy.process(b)
        noisy_out.append(result.enriched)
        if i % 3 == 0:
            assert noisy.process(b).outcome is Outcome.DUPLICATE
    assert [e.indicators for e in clean_out if e] == [e.indicators for e in noisy_out if e]


def test_late_events_are_dropped_from_realtime_state() -> None:
    proc = BarProcessor()
    proc.process(bar(5))
    assert proc.process(bar(3)).outcome is Outcome.LATE


def test_gaps_are_counted() -> None:
    proc = BarProcessor()
    proc.process(bar(0))
    assert proc.process(bar(1)).missing_bars == 0
    assert proc.process(bar(5)).missing_bars == 3


def test_anomalies_are_published_with_deterministic_ids() -> None:
    proc, again = BarProcessor(), BarProcessor()
    for p in (proc, again):
        p.process(bar(0, close=100.0))
    spike = bar(1, close=103.0)
    out, out2 = proc.process(spike), again.process(spike)
    (anomaly,) = out.anomalies
    assert anomaly.anomaly_type == "price_spike"
    assert anomaly.source_event_id == spike.event_id
    assert anomaly.detector_version == "1.0.0"
    assert anomaly.anomaly_id == out2.anomalies[0].anomaly_id
    assert anomaly.event_id == out2.anomalies[0].event_id


def test_symbols_are_independent_and_state_is_bounded() -> None:
    proc = BarProcessor(max_symbols=2)
    proc.process(bar(0, symbol="AAA"))
    proc.process(bar(0, symbol="BBB"))
    proc.process(bar(0, symbol="CCC"))  # evicts AAA (least recently updated)
    assert proc.tracked_symbols == 2
    assert proc.process(bar(0, symbol="AAA")).outcome is Outcome.PROCESSED
    proc.drop(["AAA", "ZZZ"])
    assert proc.tracked_symbols == 1


def test_interval_change_resets_state() -> None:
    proc = BarProcessor()
    proc.process(bar(5))
    result = proc.process(bar(1, interval=BarInterval.S1))
    assert result.outcome is Outcome.PROCESSED

"""Every validation rule against small, deliberately corrupted snapshots."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest
from ohlcv_fixtures import NOW, frame

from stockml.data.cleaning import CleaningPolicy, clean
from stockml.data.quality import read_raw_lenient, validate


def to_raw(symbol: str, df: pd.DataFrame, tmp_path: Path) -> pd.DataFrame:
    path = tmp_path / f"{symbol}.csv"
    df.to_csv(path, index=False)
    return read_raw_lenient(path, symbol)


def codes_at(result_frame: pd.DataFrame, symbol: str, row: int) -> list[str]:
    match = result_frame[(result_frame["symbol"] == symbol) & (result_frame["row"] == row)]
    return list(match["issues"].iloc[0])


@pytest.mark.parametrize(
    ("column", "value", "code"),
    [
        ("close", "", "E001"),
        ("volume", "abc", "E001"),
        ("timestamp", "not-a-time", "E002"),
        ("timestamp", "100", "E002"),  # 1970: before the plausible range
        ("timestamp", "4102444800", "E002"),  # 2100: in the future
        ("low", "-1", "E003"),
        ("volume", "-5", "E004"),
        ("high", "0.5", "E005"),  # high below open/close
    ],
)
def test_error_rules(tmp_path: Path, column: str, value: str, code: str) -> None:
    df = frame(30)
    df.loc[10, column] = value
    result = validate({"AAA": to_raw("AAA", df, tmp_path)}, now=NOW)
    assert code in codes_at(result.frame, "AAA", 12)  # file line = index + 2
    cleaned = clean(result)
    assert len(cleaned.cleaned) == 29
    removed = cleaned.removed.iloc[0]
    assert (removed["row"], code in removed["codes"]) == (12, True)


def test_duplicates(tmp_path: Path) -> None:
    df = frame(20)
    exact = df.iloc[[5]]
    conflicting = df.iloc[[8]].assign(close="999.0", high="1000.0")
    df = pd.concat([df, exact, conflicting], ignore_index=True)
    result = validate({"AAA": to_raw("AAA", df, tmp_path)}, now=NOW)
    cleaned = clean(result).cleaned
    assert len(cleaned) == 19, "exact repeat dropped once; both conflicting copies dropped"
    assert codes_at(result.frame, "AAA", 22) == ["E007"]
    assert "E006" in codes_at(result.frame, "AAA", 10)
    assert "E006" in codes_at(result.frame, "AAA", 23)
    assert cleaned["timestamp"].is_unique


def test_consensus_calendar_detects_gaps_but_not_market_closures(tmp_path: Path) -> None:
    frames = {s: frame(60, seed=i) for i, s in enumerate(["AAA", "BBB", "CCC"])}
    # A day the whole market is closed (all symbols miss it): not a gap.
    frames = {s: f.drop(index=30).reset_index(drop=True) for s, f in frames.items()}
    # BBB alone misses two sessions: a real gap.
    frames["BBB"] = frames["BBB"].drop(index=[40, 41]).reset_index(drop=True)
    raw = {s: to_raw(s, f, tmp_path) for s, f in frames.items()}
    result = validate(raw, now=NOW)

    assert len(result.calendar) == 59
    assert list(result.gaps) == ["BBB"]
    assert len(result.gaps["BBB"]) == 2
    cleaned = clean(result).cleaned
    bbb = cleaned[cleaned["symbol"] == "BBB"]
    assert bbb["missing_sessions_before"].tolist().count(2) == 1
    assert len(bbb) == 57, "gaps are recorded, never filled"


def test_extreme_move_classification(tmp_path: Path) -> None:
    n = 200
    frames = {s: frame(n, seed=i, sigma=0.005) for i, s in enumerate(["AAA", "BBB", "CCC", "DDD"])}

    def jump(df: pd.DataFrame, i: int, factor: float, volume: int | None = None) -> None:
        for col in ("open", "high", "low", "close"):
            vals = df[col].astype(float)
            vals.iloc[i:] *= factor
            df[col] = vals.round(4).astype(str)
        if volume is not None:
            df.loc[i, "volume"] = str(volume)

    # Day 150: everyone drops 8% (market-wide crash).
    for df in frames.values():
        jump(df, 150, 0.92)
    # Day 120: AAA jumps on 5x volume (news, confirmed by trading).
    jump(frames["AAA"], 120, 1.10, volume=5_000_000)
    # Day 100: BBB spikes and fully reverses next day on normal volume (bad tick?).
    jump(frames["BBB"], 100, 1.12)
    jump(frames["BBB"], 101, 1 / 1.12)
    # Day 170: CCC drops 9% on normal volume, no reversal (idiosyncratic).
    jump(frames["CCC"], 170, 0.91)

    result = validate({s: to_raw(s, f, tmp_path) for s, f in frames.items()}, now=NOW)
    cleaned = clean(result).cleaned
    flagged = cleaned.dropna(subset=["extreme_class"])
    classes = {
        (str(r["symbol"]), pd.Timestamp(r["session_date"]).strftime("%Y-%m-%d")): r["extreme_class"]
        for r in flagged.to_dict("records")
    }

    def day(i: int) -> str:
        return pd.bdate_range("2024-01-02", periods=n)[i].strftime("%Y-%m-%d")

    assert classes[("AAA", day(120))] == "volume_confirmed"
    assert classes[("BBB", day(100))] == "suspect_reversal"
    assert classes[("CCC", day(170))] == "idiosyncratic"
    assert all(classes[(s, day(150))] == "market_wide" for s in ["AAA", "BBB", "CCC", "DDD"])
    assert len(cleaned) == 4 * n, "extreme moves are flagged, never removed by default"

    strict = clean(result, CleaningPolicy(drop_suspect_reversals=True))
    assert len(strict.cleaned) == 4 * n - 1
    assert strict.removed["reason"].tolist() == ["suspect_reversal (policy)"]


def test_extreme_flags_do_not_fire_on_quiet_data(tmp_path: Path) -> None:
    result = validate({"AAA": to_raw("AAA", frame(300, sigma=0.01), tmp_path)}, now=NOW)
    assert clean(result).cleaned["extreme_class"].isna().all()


def test_warnings_for_zero_volume_stale_price_and_session_time(tmp_path: Path) -> None:
    df = frame(80)
    df.loc[20, "volume"] = "0"
    df.loc[40:46, ["open", "high", "low", "close"]] = "50.0"
    df.loc[60, "timestamp"] = str(int(str(df.at[60, "timestamp"])) + 3600 * 5)  # 14:30 NY
    result = validate({"AAA": to_raw("AAA", df, tmp_path)}, now=NOW)
    assert "W102" in codes_at(result.frame, "AAA", 22)
    assert "W105" in codes_at(result.frame, "AAA", 44)
    assert "W104" in codes_at(result.frame, "AAA", 62)
    assert len(clean(result).cleaned) == 80


def test_daylight_saving_time_is_not_irregular(tmp_path: Path) -> None:
    df = frame(200, start="2024-01-02")  # spans the March DST switch: 14:30 then 13:30 UTC
    hours = pd.to_datetime(df["timestamp"].astype(int), unit="s").dt.hour
    assert set(hours) == {13, 14}
    result = validate({"AAA": to_raw("AAA", df, tmp_path)}, now=NOW)
    assert not any("W104" in c for c in result.frame["issues"])


def test_validation_is_deterministic(tmp_path: Path) -> None:
    raw = {"AAA": to_raw("AAA", frame(150), tmp_path)}
    a = clean(validate(raw, now=NOW)).cleaned
    b = clean(validate(raw, now=NOW)).cleaned
    pd.testing.assert_frame_equal(a, b)


def test_cleaned_schema(tmp_path: Path) -> None:
    cleaned = clean(validate({"AAA": to_raw("AAA", frame(30), tmp_path)}, now=NOW)).cleaned
    assert list(cleaned.columns[:8]) == [
        "symbol",
        "session_date",
        "timestamp",
        "open",
        "high",
        "low",
        "close",
        "volume",
    ]
    assert cleaned["volume"].dtype == "int64"
    assert str(cleaned["timestamp"].dt.tz) == "UTC"
    json.dumps(cleaned.head(1).astype(str).to_dict())  # serialisable

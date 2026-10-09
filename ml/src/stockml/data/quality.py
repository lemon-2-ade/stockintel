"""Data-quality validation for raw daily OHLCV snapshots.

Every check is a named, documented rule with a severity:

* **errors** describe observations that cannot be true (or cannot be used):
  they are removed by the cleaning stage, with the rule code as the reason;
* **warnings** describe observations that are unusual but may be *real*:
  they are kept and flagged, never silently deleted. Crashes, squeezes and
  earnings gaps are exactly what a market-data system must not "clean away".

The validator never mutates input. It returns the raw rows with parsed values
and an ``issues`` column, plus a per-rule summary for the quality report.

Extreme-move flags use information *after* the move (next-day reversal) and
are therefore **quality annotations, not model features**: using them as
features would leak the future.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

EXCHANGE_TZ = "America/New_York"
OHLCV = ["open", "high", "low", "close", "volume"]
MIN_TIMESTAMP = pd.Timestamp("1990-01-01", tz="UTC")


class Severity(StrEnum):
    ERROR = "error"
    WARNING = "warning"


@dataclass(frozen=True, slots=True)
class Rule:
    code: str
    name: str
    severity: Severity
    description: str
    action: str


RULES: dict[str, Rule] = {
    rule.code: rule
    for rule in (
        Rule(
            "E001",
            "missing_value",
            Severity.ERROR,
            "A price or volume field is empty or not a number.",
            "drop row",
        ),
        Rule(
            "E002",
            "malformed_timestamp",
            Severity.ERROR,
            "Timestamp missing, unparsable, before 1990 or in the future.",
            "drop row",
        ),
        Rule("E003", "non_positive_price", Severity.ERROR, "open/high/low/close <= 0.", "drop row"),
        Rule("E004", "negative_volume", Severity.ERROR, "volume < 0.", "drop row"),
        Rule(
            "E005",
            "ohlc_inconsistent",
            Severity.ERROR,
            "high < max(open, close, low) or low > min(open, close).",
            "drop row",
        ),
        Rule(
            "E006",
            "conflicting_duplicate",
            Severity.ERROR,
            "Several rows share a timestamp but disagree on values; none can be trusted.",
            "drop all copies",
        ),
        Rule(
            "E007",
            "exact_duplicate",
            Severity.ERROR,
            "Row repeats an earlier row exactly.",
            "drop the repeats, keep the first",
        ),
        Rule(
            "W101",
            "extreme_move",
            Severity.WARNING,
            "|log return| is extreme relative to the symbol's trailing robust volatility; "
            "classified as market_wide, volume_confirmed, suspect_reversal or idiosyncratic.",
            "keep, flag",
        ),
        Rule(
            "W102",
            "zero_volume",
            Severity.WARNING,
            "No shares traded in a regular session.",
            "keep, flag",
        ),
        Rule(
            "W103",
            "missing_sessions",
            Severity.WARNING,
            "Sessions the market traded (consensus across symbols) are absent before this row.",
            "keep, record gap length; no imputation",
        ),
        Rule(
            "W104",
            "irregular_session_time",
            Severity.WARNING,
            "Bar does not start at the 09:30 New York session open.",
            "keep, flag",
        ),
        Rule(
            "W105",
            "stale_price",
            Severity.WARNING,
            "Close unchanged for at least `stale_run` consecutive sessions.",
            "keep, flag",
        ),
    )
}


@dataclass(frozen=True, slots=True)
class QualityConfig:
    extreme_robust_z: float = 6.0
    """|log return - trailing median| / (1.4826 * trailing MAD) at or above this is extreme."""
    trailing_window: int = 252
    min_history: int = 60
    market_wide_z: float = 3.0
    market_wide_fraction: float = 0.5
    volume_confirm_ratio: float = 2.0
    volume_window: int = 50
    reversal_ratio: float = 0.8
    stale_run: int = 5
    calendar_quorum: float = 0.5
    """A date is a trading session if at least this fraction of listed symbols has a bar."""


@dataclass(slots=True)
class ValidationResult:
    frame: pd.DataFrame
    """All rows (raw + parsed values) with ``issues`` (list of rule codes) and flags."""
    calendar: pd.DatetimeIndex
    gaps: dict[str, list[str]] = field(default_factory=dict)
    """symbol -> ISO session dates missing within its listed range."""

    def counts(self) -> pd.DataFrame:
        """Rows per (symbol, rule code)."""
        exploded = self.frame[["symbol", "issues"]].explode("issues").dropna()
        if exploded.empty:
            return pd.DataFrame(columns=["symbol", "code", "rows"])
        return (
            exploded.groupby(["symbol", "issues"])
            .size()
            .rename("rows")
            .reset_index()
            .rename(columns={"issues": "code"})
        )


def read_raw_lenient(path: Path, symbol: str) -> pd.DataFrame:
    """Read a raw CSV as *strings*: malformed values must reach the validator, not crash it."""
    raw = pd.read_csv(path, dtype=str, keep_default_na=False)
    missing = {*OHLCV, "timestamp"} - set(raw.columns)
    if missing:
        raise ValueError(f"{path.name}: missing columns {sorted(missing)}")
    frame = raw[["timestamp", *OHLCV]].rename(columns=lambda c: f"raw_{c}")
    frame.insert(0, "row", np.arange(2, len(frame) + 2))  # 1-based file line (header = 1)
    frame.insert(0, "symbol", symbol)
    return frame


def _add(issues: pd.Series, mask: pd.Series, code: str) -> None:
    """Append ``code`` to the issue list of every row where ``mask`` is true."""
    for index in mask[mask].index:
        codes: list[str] = issues.at[index]  # type: ignore[assignment]
        codes.append(code)


def validate_rows(frame: pd.DataFrame, *, now: pd.Timestamp) -> pd.DataFrame:
    """Parse values and apply the row-level error rules (E001-E007) to one symbol."""
    out = frame.copy()
    for column in OHLCV:
        out[column] = pd.to_numeric(out[f"raw_{column}"].str.strip(), errors="coerce")
    seconds = pd.to_numeric(out["raw_timestamp"].str.strip(), errors="coerce")
    ts = pd.to_datetime(seconds, unit="s", utc=True, errors="coerce")
    out["timestamp"] = ts.where((ts >= MIN_TIMESTAMP) & (ts <= now + pd.Timedelta(days=1)))
    out["issues"] = [[] for _ in range(len(out))]

    numbers = out[OHLCV]
    _add(out["issues"], numbers.isna().any(axis=1) | ~np.isfinite(numbers).all(axis=1), "E001")
    _add(out["issues"], out["timestamp"].isna(), "E002")
    prices = out[["open", "high", "low", "close"]]
    _add(out["issues"], (prices <= 0).any(axis=1), "E003")
    _add(out["issues"], out["volume"] < 0, "E004")
    inconsistent = (out["high"] < out[["open", "close", "low"]].max(axis=1)) | (
        out["low"] > out[["open", "close"]].min(axis=1)
    )
    _add(out["issues"], inconsistent, "E005")

    usable = out["issues"].map(len) == 0
    candidates = out[usable & out["timestamp"].notna()]
    dup_ts = candidates["timestamp"].duplicated(keep=False)
    if dup_ts.any():
        group = candidates[dup_ts]
        distinct = group.groupby("timestamp")[OHLCV].nunique().max(axis=1) > 1
        conflicting_ts = set(distinct[distinct].index)
        conflicting = group["timestamp"].isin(conflicting_ts)
        _add(out["issues"], _as_mask(out.index, out.index.isin(group[conflicting].index)), "E006")
        exact = group[~conflicting]
        repeats = exact["timestamp"].duplicated(keep="first")
        _add(out["issues"], _as_mask(out.index, out.index.isin(exact[repeats].index)), "E007")
    return out


def _as_mask(index: pd.Index, values: Any) -> pd.Series:
    return pd.Series(values, index=index, dtype=bool)


def consensus_calendar(frames: list[pd.DataFrame], quorum: float) -> pd.DatetimeIndex:
    """Trading sessions inferred from the data itself.

    A date is a session if at least ``quorum`` of the symbols *listed on that
    date* (between their first and last bar) have a bar. This needs no
    exchange-calendar dependency and naturally excludes market-wide closures
    (holidays, Hurricane Sandy), while a single symbol's missing day shows up as
    a gap for that symbol.
    """
    present: dict[pd.Timestamp, int] = {}
    listed: dict[pd.Timestamp, int] = {}
    all_dates = sorted({d for f in frames for d in f["session_date"]})
    for f in frames:
        dates = set(f["session_date"])
        if not dates:
            continue
        first, last = min(dates), max(dates)
        for d in all_dates:
            if first <= d <= last:
                listed[d] = listed.get(d, 0) + 1
                present[d] = present.get(d, 0) + (d in dates)
    sessions = [d for d in all_dates if listed.get(d) and present[d] / listed[d] >= quorum]
    return pd.DatetimeIndex(sessions)


def _window_mad(values: np.ndarray) -> float:
    centre = np.nanmedian(values)
    return float(np.nanmedian(np.abs(values - centre)))


def _robust_z(log_ret: pd.Series, window: int, min_history: int) -> pd.Series:
    """Score each return against the *trailing* median/MAD (prior returns only).

    Median and MAD come from the same trailing window, so the score is
    available after ``min_history`` returns (a MAD of rolling deviations would
    need twice the warm-up).
    """
    prior = log_ret.shift(1).rolling(window, min_periods=min_history)
    median = prior.median()
    mad = prior.apply(_window_mad, raw=True)
    scale = 1.4826 * mad
    return (log_ret - median) / scale.where(scale > 0)


def annotate_symbol(
    clean: pd.DataFrame, calendar: pd.DatetimeIndex, cfg: QualityConfig
) -> tuple[pd.DataFrame, list[str]]:
    """Warning rules on a symbol's *usable* rows (sorted by time)."""
    f = clean.sort_values("timestamp").copy()
    f["log_return"] = np.log(f["close"] / f["close"].shift(1))
    f["robust_z"] = _robust_z(f["log_return"], cfg.trailing_window, cfg.min_history)
    f["extreme"] = f["robust_z"].abs() >= cfg.extreme_robust_z

    volume_baseline = f["volume"].shift(1).rolling(cfg.volume_window, min_periods=20).median()
    f["volume_confirmed"] = f["volume"] >= cfg.volume_confirm_ratio * volume_baseline
    next_ret = f["log_return"].shift(-1)
    f["reverses_next_day"] = (np.sign(next_ret) == -np.sign(f["log_return"])) & (
        next_ret.abs() >= cfg.reversal_ratio * f["log_return"].abs()
    )

    local = f["timestamp"].dt.tz_convert(EXCHANGE_TZ)
    f["irregular_time"] = (local.dt.hour != 9) | (local.dt.minute != 30)
    f["zero_volume"] = f["volume"] == 0
    run_id = (f["close"] != f["close"].shift(1)).cumsum()
    run_length = f.groupby(run_id)["close"].transform("size")
    f["stale_price"] = run_length >= cfg.stale_run

    listed = calendar[(calendar >= f["session_date"].min()) & (calendar <= f["session_date"].max())]
    missing = listed.difference(pd.DatetimeIndex(f["session_date"]))
    position = np.searchsorted(listed, pd.DatetimeIndex(f["session_date"]))
    sessions_before = np.diff(np.concatenate([[0], position]))  # expected sessions since last row
    missing_before = np.clip(sessions_before - 1, 0, None)
    if len(missing_before):
        missing_before[0] = 0
    f["missing_sessions_before"] = missing_before
    return f, [str(d) for d in missing.strftime("%Y-%m-%d")]


def classify_extremes(frame: pd.DataFrame, cfg: QualityConfig) -> pd.Series:
    """Label each extreme move (``None`` for ordinary rows).

    Precedence: market_wide > volume_confirmed > suspect_reversal > idiosyncratic.
    """
    big = (frame["robust_z"].abs() >= cfg.market_wide_z).astype(float)
    share_by_date = big.groupby(frame["session_date"]).mean()
    market_wide = frame["session_date"].map(share_by_date).fillna(0.0) >= cfg.market_wide_fraction
    extreme = frame["extreme"].fillna(False).astype(bool)

    labels = pd.Series(None, index=frame.index, dtype=object)
    labels[extreme] = "idiosyncratic"
    labels[extreme & frame["reverses_next_day"].fillna(False).astype(bool)] = "suspect_reversal"
    labels[extreme & frame["volume_confirmed"].fillna(False).astype(bool)] = "volume_confirmed"
    labels[extreme & market_wide] = "market_wide"
    return labels


def validate(
    raw_frames: dict[str, pd.DataFrame],
    *,
    config: QualityConfig | None = None,
    now: pd.Timestamp | None = None,
) -> ValidationResult:
    cfg = config or QualityConfig()
    now = now or pd.Timestamp.now(tz="UTC")

    # Globally unique row index across symbols, so per-symbol results can be joined back.
    offset, indexed = 0, {}
    for symbol, f in raw_frames.items():
        indexed[symbol] = f.set_axis(pd.RangeIndex(offset, offset + len(f)))
        offset += len(f)
    checked = {s: validate_rows(f, now=now) for s, f in indexed.items()}
    usable: dict[str, pd.DataFrame] = {}
    for symbol, f in checked.items():
        ok = f[f["issues"].map(len) == 0].copy()
        ok["session_date"] = (
            ok["timestamp"].dt.tz_convert(EXCHANGE_TZ).dt.tz_localize(None).dt.normalize()
        )
        usable[symbol] = ok
    calendar = consensus_calendar(list(usable.values()), cfg.calendar_quorum)

    annotated, gaps = [], {}
    for symbol, ok in usable.items():
        f, missing = annotate_symbol(ok, calendar, cfg)
        annotated.append(f)
        if missing:
            gaps[symbol] = missing
    flags = pd.concat(annotated) if annotated else pd.DataFrame()
    if not flags.empty:
        flags["extreme_class"] = classify_extremes(flags, cfg)

    merged = pd.concat(checked.values())
    warn_cols = [
        "session_date",
        "log_return",
        "robust_z",
        "extreme_class",
        "volume_confirmed",
        "zero_volume",
        "stale_price",
        "irregular_time",
        "missing_sessions_before",
    ]
    merged = merged.join(flags[warn_cols], how="left") if not flags.empty else merged
    if "extreme_class" in merged:
        _add(merged["issues"], merged["extreme_class"].notna(), "W101")
        _add(merged["issues"], merged["missing_sessions_before"].fillna(0) > 0, "W103")
        for code, column in (
            ("W102", "zero_volume"),
            ("W104", "irregular_time"),
            ("W105", "stale_price"),
        ):
            _add(merged["issues"], merged[column].fillna(False).astype(bool), code)
    return ValidationResult(frame=merged.reset_index(drop=True), calendar=calendar, gaps=gaps)

"""Cleaning policy: turn a validated snapshot into the canonical training input.

Policy (each decision is recorded per row, nothing happens silently):

1. Rows with any **error** (E0xx) are removed; the removal log keeps symbol,
   file line, rule codes and the raw values.
2. Exact duplicates keep their first occurrence; conflicting duplicates are all
   removed (no basis to choose one).
3. **Warnings** are kept and exposed as flag columns. Extreme moves are never
   removed by default: crashes and squeezes are the most informative days in
   the data. ``drop_suspect_reversals`` exists for experiments, defaults off.
4. Missing sessions are **not imputed**: inventing prices would create
   returns that never happened. Gap length is recorded instead, so the feature
   stage can decide (e.g. not compute returns across gaps).
"""

from __future__ import annotations

from dataclasses import dataclass

import pandas as pd

from stockml.data.quality import RULES, Severity, ValidationResult

CLEAN_COLUMNS = [
    "symbol",
    "session_date",
    "timestamp",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "extreme_class",
    "flag_zero_volume",
    "flag_stale_price",
    "flag_irregular_time",
    "missing_sessions_before",
]


@dataclass(frozen=True, slots=True)
class CleaningPolicy:
    drop_suspect_reversals: bool = False


@dataclass(slots=True)
class CleaningResult:
    cleaned: pd.DataFrame
    removed: pd.DataFrame
    """symbol, row, codes, reason and raw values of every removed row."""


ERROR_CODES = {code for code, rule in RULES.items() if rule.severity is Severity.ERROR}


def clean(result: ValidationResult, policy: CleaningPolicy | None = None) -> CleaningResult:
    policy = policy or CleaningPolicy()
    frame = result.frame
    errors = frame["issues"].map(lambda codes: sorted(set(codes) & ERROR_CODES))
    drop = errors.map(bool)
    reasons = errors.map(lambda codes: ", ".join(RULES[c].name for c in codes))

    if policy.drop_suspect_reversals:
        suspect = frame["extreme_class"].eq("suspect_reversal").fillna(False).astype(bool)
        reasons = reasons.where(~suspect | drop, "suspect_reversal (policy)")
        drop = drop | suspect

    removed = frame.loc[drop, ["symbol", "row"]].assign(
        codes=errors[drop].map(", ".join),
        reason=reasons[drop],
        raw_timestamp=frame.loc[drop, "raw_timestamp"],
        raw_close=frame.loc[drop, "raw_close"],
    )

    kept = frame.loc[~drop].copy()
    kept["flag_zero_volume"] = kept["zero_volume"].fillna(False).astype(bool)
    kept["flag_stale_price"] = kept["stale_price"].fillna(False).astype(bool)
    kept["flag_irregular_time"] = kept["irregular_time"].fillna(False).astype(bool)
    kept["missing_sessions_before"] = kept["missing_sessions_before"].fillna(0).astype(int)
    kept["volume"] = kept["volume"].astype("int64")
    kept["extreme_class"] = kept["extreme_class"].astype("string")
    cleaned = kept[CLEAN_COLUMNS].sort_values(["symbol", "timestamp"]).reset_index(drop=True)
    return CleaningResult(cleaned=cleaned, removed=removed.reset_index(drop=True))

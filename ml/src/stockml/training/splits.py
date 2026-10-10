"""Chronological splits for overlapping, forward-looking labels.

Random ``train_test_split`` is wrong for time series: it trains on the future.
Even a chronological split leaks when labels span several sessions: a
training row dated just before the boundary has a label computed from bars
*after* it. So every split here:

* is chronological, with the **same date boundaries for all symbols**
  (cross-sectional leakage: AAPL's future must not train a model tested on
  MSFT's past);
* **purges** training rows whose label window ends after the boundary
  (``label_end > boundary``);
* applies an **embargo**: the first ``embargo`` sessions after a boundary are
  dropped from the evaluation set, because features there are computed from
  windows that overlap the training period's labels (serial correlation).

Split layout used by the project (see docs/ML_PIPELINE.md):

=========  ========================  =======================================
train      2010 .. 2019              fitting
validation 2020 .. 2022              model selection, threshold choice
test       2023 .. end of data       touched **once**, for the final report
=========  ========================  =======================================
"""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass

import pandas as pd


@dataclass(frozen=True, slots=True)
class SplitConfig:
    train_end: str = "2019-12-31"
    validation_end: str = "2022-12-31"
    embargo_sessions: int = 5


@dataclass(frozen=True, slots=True)
class Fold:
    name: str
    train: pd.DataFrame
    test: pd.DataFrame


def _sessions_after(df: pd.DataFrame, boundary: pd.Timestamp, n: int) -> pd.Timestamp:
    """Date of the n-th session after ``boundary`` (inclusive), from the data's calendar."""
    dates = pd.DatetimeIndex(df["session_date"].unique()).sort_values()
    later = dates[dates > boundary]
    if n <= 0 or later.empty:
        return boundary
    return later[min(n, len(later)) - 1]


def purge_and_embargo(
    df: pd.DataFrame,
    train_end: pd.Timestamp,
    test_end: pd.Timestamp | None,
    embargo: int,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Train = up to ``train_end`` with labels fully inside; test after the embargo."""
    train = df[(df["session_date"] <= train_end) & (df["label_end"] <= train_end)]
    embargo_end = _sessions_after(df, train_end, embargo)
    test_mask = df["session_date"] > embargo_end
    if test_end is not None:
        test_mask &= (df["session_date"] <= test_end) & (df["label_end"] <= test_end)
    return train, df[test_mask]


def chronological_split(
    df: pd.DataFrame, cfg: SplitConfig | None = None
) -> dict[str, pd.DataFrame]:
    cfg = cfg or SplitConfig()
    train_end, val_end = pd.Timestamp(cfg.train_end), pd.Timestamp(cfg.validation_end)
    train, validation = purge_and_embargo(df, train_end, val_end, cfg.embargo_sessions)
    _, test = purge_and_embargo(df, val_end, None, cfg.embargo_sessions)
    return {"train": train, "validation": validation, "test": test}


def walk_forward(
    df: pd.DataFrame,
    *,
    first_test_year: int,
    last_test_year: int,
    embargo: int = 5,
) -> Iterator[Fold]:
    """Expanding-window folds: train on everything before year Y, test on year Y.

    Mirrors how the model would be used: periodically retrained on all history,
    then used on the next, unseen period. ``last_test_year`` should stay inside
    the development period so the held-out test set is never touched.
    """
    for year in range(first_test_year, last_test_year + 1):
        train_end = pd.Timestamp(f"{year - 1}-12-31")
        train, test = purge_and_embargo(df, train_end, pd.Timestamp(f"{year}-12-31"), embargo)
        yield Fold(name=str(year), train=train, test=test)

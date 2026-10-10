"""Build the supervised dataset: point-in-time features + forward-looking labels.

Features come from :class:`shared.features.FeatureComputer`, the same code the
inference service runs online, replayed bar by bar over ``cleaned.parquet``.

Labels look *forward* by design; they are the only place future data is
allowed, and they never feed back into features:

* ``y_return``  = log(close[t+H] / close[t])          (regression target)
* ``y_class``   = +1 / -1 / 0 if y_return > band, < -band, else (3-class)
* ``y_up``      = 1 if up, 0 if down, NaN if inside the neutral band (binary)
* ``label_end`` = session date of t+H, used to *purge* train/test overlap

A label is NaN when its window crosses a recorded data gap or runs past the
end of the data, so no target is ever computed from invented or missing bars.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

from shared.features import FEATURE_NAMES, FEATURE_SET_VERSION, BarInput, FeatureComputer


@dataclass(frozen=True, slots=True)
class LabelConfig:
    horizon: int = 5
    """Prediction horizon in sessions (5 = one trading week)."""
    neutral_band: float = 0.0
    """|y_return| <= band is labelled neutral (excluded from the binary task)."""


def compute_features(cleaned: pd.DataFrame) -> pd.DataFrame:
    """Replay each symbol's bars through a fresh FeatureComputer, in time order."""
    blocks = []
    for _, bars in cleaned.sort_values(["symbol", "timestamp"]).groupby("symbol"):
        computer = FeatureComputer()
        columns = zip(
            bars["timestamp"].dt.to_pydatetime(),
            bars["open"].to_numpy(dtype=float),
            bars["high"].to_numpy(dtype=float),
            bars["low"].to_numpy(dtype=float),
            bars["close"].to_numpy(dtype=float),
            bars["volume"].to_numpy(dtype=float),
            bars["missing_sessions_before"].to_numpy(dtype=int),
            strict=True,
        )
        rows = [
            computer.update(BarInput(ts, o, h, lo, c, v, int(gap)))
            for ts, o, h, lo, c, v, gap in columns
        ]
        block = pd.DataFrame(rows, columns=list(FEATURE_NAMES), index=bars.index, dtype="float64")
        meta = bars[["symbol", "session_date", "timestamp", "close"]]
        blocks.append(pd.concat([meta, block], axis=1))
    return pd.concat(blocks).sort_values(["symbol", "timestamp"]).reset_index(drop=True)


def add_labels(frame: pd.DataFrame, cleaned: pd.DataFrame, cfg: LabelConfig) -> pd.DataFrame:
    h = cfg.horizon
    gaps = cleaned.set_index(["symbol", "timestamp"])["missing_sessions_before"]
    out = []
    for _, group in frame.groupby("symbol", sort=False):
        g = group.sort_values("timestamp").copy()
        gap = gaps.loc[list(zip(g["symbol"], g["timestamp"], strict=True))].to_numpy() > 0
        # A gap at any of bars t+1..t+H invalidates the label at t.
        gap_ahead = pd.Series(gap.astype(float)).rolling(h, min_periods=1).max().shift(-h)
        future = g["close"].shift(-h)
        y_return = pd.Series(np.log(future / g["close"]), index=g.index)
        y_return = y_return.where(gap_ahead.to_numpy() == 0)
        g["y_return"] = y_return
        g["y_class"] = np.select(
            [y_return > cfg.neutral_band, y_return < -cfg.neutral_band], [1.0, -1.0], 0.0
        )
        g.loc[y_return.isna(), "y_class"] = np.nan
        g["y_up"] = g["y_class"].map({1.0: 1.0, -1.0: 0.0})  # neutral (0) -> NaN
        g["label_end"] = g["session_date"].shift(-h)
        out.append(g)
    return pd.concat(out).reset_index(drop=True)


def build_dataset(cleaned: pd.DataFrame, cfg: LabelConfig | None = None) -> pd.DataFrame:
    """Rows where every feature exists and the label is defined."""
    cfg = cfg or LabelConfig()
    labelled = add_labels(compute_features(cleaned), cleaned, cfg)
    usable = labelled[list(FEATURE_NAMES)].notna().all(axis=1) & labelled["y_return"].notna()
    dataset = labelled[usable].reset_index(drop=True)
    dataset.attrs["feature_set_version"] = FEATURE_SET_VERSION
    dataset.attrs["horizon"] = cfg.horizon
    return dataset

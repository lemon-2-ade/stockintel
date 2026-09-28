"""Estimate per-symbol simulator parameters from the raw historical snapshot.

    python -m stockml.data.calibrate            # writes the market-producer calibration file

The simulator (``services/market-producer``) consumes the resulting JSON, so
simulated prices start at realistic levels with realistic volatility and
volume, and never depends on pandas or on the raw data being present.

Estimates, over the most recent ``lookback`` trading days:

* ``sigma_annual``: std-dev of daily log returns x sqrt(252)
* ``mu_annual``: mean daily log return x 252 + sigma^2 / 2 (the GBM drift);
  notoriously noisy over a few years, which is why the simulator lets it be
  overridden
* volume: median and log-space mean/std of daily volume
* ``last_close``: the starting price for the simulation

Rows with non-positive prices or missing values are dropped and counted; the
full validation/cleaning pipeline is a separate stage (Phase 5).
"""

from __future__ import annotations

import argparse
import json
import math
import sys
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from shared.config import LogSettings
from shared.observability.logs import configure_logging, get_logger
from stockml.data.catalog import DEFAULT_RAW_ROOT, REPO_ROOT, DatasetSpec

log = get_logger(__name__)

TRADING_DAYS_PER_YEAR = 252
DEFAULT_OUTPUT = (
    REPO_ROOT / "services" / "market-producer" / "calibration" / "us-equities-daily.json"
)
OHLCV = ["open", "high", "low", "close", "volume"]


def load_raw_bars(path: Path) -> pd.DataFrame:
    """Read one raw CSV of this dataset into a canonical frame.

    The upstream files do not share a column order, so columns are selected
    by header name; ``timestamp`` is epoch seconds at the session open.
    """
    frame = pd.read_csv(path)
    missing = {*OHLCV, "timestamp"} - set(frame.columns)
    if missing:
        raise ValueError(f"{path.name}: missing columns {sorted(missing)}")
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], unit="s", utc=True)
    return frame[["timestamp", *OHLCV]].sort_values("timestamp").reset_index(drop=True)


def calibrate_symbol(frame: pd.DataFrame, *, lookback: int) -> dict[str, Any]:
    usable = frame.dropna(subset=["close", "volume"])
    usable = usable[(usable["close"] > 0) & (usable["volume"] >= 0)]
    dropped = len(frame) - len(usable)
    window = usable.tail(lookback + 1)
    if len(window) < 60:
        raise ValueError(f"only {len(window)} usable rows; need >= 60 to calibrate")

    log_returns = np.diff(np.log(window["close"].to_numpy(dtype=float)))
    sigma_daily = float(np.std(log_returns, ddof=1))
    mean_daily = float(np.mean(log_returns))
    volume = window["volume"].to_numpy(dtype=float)
    positive_volume = volume[volume > 0]
    log_volume = np.log(positive_volume)

    return {
        "last_close": round(float(window["close"].iloc[-1]), 4),
        "mu_annual": round(
            mean_daily * TRADING_DAYS_PER_YEAR + 0.5 * sigma_daily**2 * TRADING_DAYS_PER_YEAR, 6
        ),
        "sigma_annual": round(sigma_daily * math.sqrt(TRADING_DAYS_PER_YEAR), 6),
        "median_daily_volume": int(np.median(volume)),
        "log_volume_std": round(float(np.std(log_volume, ddof=1)), 6),
        "window_start": window["timestamp"].iloc[0].date().isoformat(),
        "window_end": window["timestamp"].iloc[-1].date().isoformat(),
        "observations": len(log_returns),
        "dropped_rows": int(dropped),
    }


def calibrate(spec: DatasetSpec, *, raw_root: Path, lookback: int) -> dict[str, Any]:
    snapshot = spec.snapshot_dir(raw_root)
    if not snapshot.exists():
        raise FileNotFoundError(f"{snapshot} missing; run `make data` first")
    symbols = {
        symbol: calibrate_symbol(load_raw_bars(snapshot / f"{symbol}.csv"), lookback=lookback)
        for symbol in spec.symbols
    }
    return {
        "dataset": spec.name,
        "revision": spec.revision,
        "frequency": spec.frequency,
        "lookback_trading_days": lookback,
        "method": "GBM moments of daily log returns; volume in log space",
        "symbols": symbols,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Calibrate the market simulator from history")
    parser.add_argument("--dataset", default="us-equities-daily")
    parser.add_argument("--raw-root", type=Path, default=DEFAULT_RAW_ROOT)
    parser.add_argument("--lookback", type=int, default=3 * TRADING_DAYS_PER_YEAR)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args(argv)

    settings = LogSettings()
    configure_logging("data-calibrate", level=settings.level, fmt="console")
    spec = DatasetSpec.load(args.dataset)
    result = calibrate(spec, raw_root=args.raw_root, lookback=args.lookback)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2) + "\n")
    log.info("calibration.written", path=str(args.output), symbols=len(result["symbols"]))
    return 0


if __name__ == "__main__":
    sys.exit(main())

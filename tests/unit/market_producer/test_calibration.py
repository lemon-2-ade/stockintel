from __future__ import annotations

import pytest

from market_producer.calibration import (
    DEFAULT_CALIBRATION_FILE,
    DEFAULT_PARAMS,
    Calibration,
    SymbolParams,
)


def test_loads_committed_calibration() -> None:
    calibration = Calibration.load(DEFAULT_CALIBRATION_FILE)
    assert "AAPL" in calibration.params
    assert calibration.source.startswith("us-equities-daily@")
    assert calibration.for_symbol("AAPL").start_price > 0


def test_unknown_symbols_get_defaults() -> None:
    assert Calibration.empty().for_symbol("ZZZ") is DEFAULT_PARAMS


def test_universe_resolution() -> None:
    calibration = Calibration(source="t", params={"AAA": DEFAULT_PARAMS, "BBB": DEFAULT_PARAMS})
    assert calibration.universe(["X", "Y", "X"], None) == ["X", "Y"]
    assert calibration.universe(None, None) == ["AAA", "BBB"]
    assert calibration.universe(None, 1) == ["AAA"]
    assert calibration.universe(None, 4) == ["AAA", "BBB", "SIM001", "SIM002"]
    with pytest.raises(ValueError, match="at least one"):
        calibration.universe(None, 0)


def test_param_validation() -> None:
    with pytest.raises(ValueError, match="sigma"):
        SymbolParams(
            start_price=1, mu_annual=0, sigma_annual=0, median_daily_volume=1, log_volume_std=0
        )

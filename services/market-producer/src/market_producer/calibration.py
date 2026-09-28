"""Per-symbol simulation parameters, loaded from the calibration file.

The file is produced offline by ``make calibrate`` from the historical
snapshot (see docs/DATA_PIPELINE.md). Symbols missing from it get neutral
defaults, so the simulator can also run an arbitrary synthetic universe.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

from shared.observability.logs import get_logger

log = get_logger(__name__)

# Shipped as package data so the file is present in the installed/Docker image.
DEFAULT_CALIBRATION_FILE = Path(__file__).with_name("calibrations") / "us-equities-daily.json"


@dataclass(frozen=True, slots=True)
class SymbolParams:
    start_price: float
    mu_annual: float
    sigma_annual: float
    median_daily_volume: float
    log_volume_std: float

    def __post_init__(self) -> None:
        if self.start_price <= 0:
            raise ValueError("start_price must be > 0")
        if not 0 < self.sigma_annual < 5:
            raise ValueError("sigma_annual must be in (0, 5)")
        if self.median_daily_volume <= 0:
            raise ValueError("median_daily_volume must be > 0")
        if self.log_volume_std < 0:
            raise ValueError("log_volume_std must be >= 0")


DEFAULT_PARAMS = SymbolParams(
    start_price=100.0,
    mu_annual=0.0,
    sigma_annual=0.30,
    median_daily_volume=5_000_000,
    log_volume_std=0.35,
)


@dataclass(frozen=True, slots=True)
class Calibration:
    source: str
    params: dict[str, SymbolParams]

    @classmethod
    def load(cls, path: Path) -> Calibration:
        data = json.loads(path.read_text())
        params = {
            symbol: SymbolParams(
                start_price=float(p["last_close"]),
                mu_annual=float(p["mu_annual"]),
                sigma_annual=float(p["sigma_annual"]),
                median_daily_volume=float(p["median_daily_volume"]),
                log_volume_std=float(p["log_volume_std"]),
            )
            for symbol, p in data["symbols"].items()
        }
        return cls(source=f"{data['dataset']}@{str(data['revision'])[:12]}", params=params)

    @classmethod
    def empty(cls) -> Calibration:
        return cls(source="defaults", params={})

    def for_symbol(self, symbol: str) -> SymbolParams:
        if symbol not in self.params:
            log.warning("calibration.default_params", symbol=symbol, source=self.source)
        return self.params.get(symbol, DEFAULT_PARAMS)

    def universe(self, symbols: list[str] | None, count: int | None) -> list[str]:
        """Resolve the simulated symbol list.

        Explicit ``symbols`` win; otherwise the first ``count`` calibrated symbols,
        topped up with synthetic ``SIM001``... tickers if ``count`` exceeds them.
        """
        if symbols:
            return list(dict.fromkeys(symbols))
        known = list(self.params)
        wanted = count if count is not None else len(known)
        if wanted < 1:
            raise ValueError("need at least one symbol")
        extra = [f"SIM{i:03d}" for i in range(1, wanted - len(known) + 1)]
        return (known + extra)[:wanted]

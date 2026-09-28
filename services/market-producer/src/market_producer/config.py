"""Producer configuration (environment variables prefixed ``PRODUCER_``)."""

from __future__ import annotations

import re
from pathlib import Path
from typing import Annotated, Any, Literal

from pydantic import Field, field_validator, model_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict

from market_producer.calibration import DEFAULT_CALIBRATION_FILE
from shared.kafka.topics import Topic
from shared.schemas import BarInterval
from shared.schemas.base import SYMBOL_PATTERN

_SYMBOL_RE = re.compile(SYMBOL_PATTERN)


class ProducerSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="PRODUCER_", extra="ignore")

    mode: Literal["simulator", "replay"] = "simulator"
    topic: Topic = Topic.MARKET_RAW
    client_id: str = "market-producer"

    # Universe: explicit list (comma-separated in env) or the first N calibrated symbols.
    symbols: Annotated[list[str], NoDecode] = Field(default_factory=list)
    num_symbols: int | None = Field(default=None, ge=1, le=10_000)

    # Simulator
    interval: BarInterval = BarInterval.S1
    backfill_bars: int = Field(default=300, ge=0, le=100_000)
    seed: int = 42
    volatility_multiplier: float = Field(default=1.0, gt=0, le=50)
    use_calibrated_drift: bool = False
    drift_annual: float = Field(default=0.0, ge=-5, le=5)
    price_jump_probability: float = Field(default=0.001, ge=0, le=1)
    jump_min: float = Field(default=0.02, gt=0, lt=1)
    jump_max: float = Field(default=0.05, gt=0, lt=1)
    volume_spike_probability: float = Field(default=0.002, ge=0, le=1)
    calibration_file: Path = DEFAULT_CALIBRATION_FILE

    # Replay
    replay_snapshot_dir: Path | None = None
    replay_bars_per_second: float = Field(default=5.0, gt=0, le=10_000)
    replay_limit: int | None = Field(default=None, ge=1)

    # Runtime
    metrics_port: int = Field(default=8001, ge=1, le=65_535)
    flush_timeout_s: float = Field(default=10.0, gt=0)

    @field_validator("symbols", mode="before")
    @classmethod
    def _split_symbols(cls, value: Any) -> Any:
        if isinstance(value, str):
            return [s.strip() for s in value.split(",") if s.strip()]
        return value

    @field_validator("symbols")
    @classmethod
    def _validate_symbols(cls, value: list[str]) -> list[str]:
        bad = [s for s in value if not _SYMBOL_RE.match(s)]
        if bad:
            raise ValueError(f"invalid symbols {bad}: use upper-case tickers")
        return value

    @model_validator(mode="after")
    def _check(self) -> ProducerSettings:
        if self.jump_min > self.jump_max:
            raise ValueError("jump_min must be <= jump_max")
        if self.mode == "replay" and self.replay_snapshot_dir is None:
            raise ValueError("replay mode requires PRODUCER_REPLAY_SNAPSHOT_DIR")
        return self

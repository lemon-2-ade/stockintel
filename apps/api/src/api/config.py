"""API configuration (environment variables prefixed ``API_``)."""

from __future__ import annotations

from typing import Annotated, Any

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict


class ApiSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="API_", extra="ignore")

    host: str = "0.0.0.0"  # noqa: S104 - container-internal bind; exposure is decided by compose
    port: int = Field(default=8000, ge=1, le=65_535)
    cors_origins: Annotated[list[str], NoDecode] = Field(
        default_factory=lambda: ["http://localhost:5173", "http://127.0.0.1:5173"]
    )
    # Per-client token bucket for REST calls (in-process; see docs/API.md).
    rate_limit_per_s: float = Field(default=20.0, gt=0)
    rate_limit_burst: int = Field(default=60, ge=1)
    default_user_id: str = "default"
    history_max_limit: int = Field(default=5_000, ge=1, le=50_000)
    stale_after_bars: int = Field(default=5, ge=1)
    # WebSocket
    ws_flush_interval_ms: int = Field(default=250, ge=20, le=10_000)
    ws_heartbeat_s: float = Field(default=15.0, gt=0)
    ws_max_symbols: int = Field(default=50, ge=1, le=1_000)
    ws_max_pending_anomalies: int = Field(default=100, ge=1)

    @field_validator("cors_origins", mode="before")
    @classmethod
    def _split(cls, value: Any) -> Any:
        if isinstance(value, str):
            return [v.strip() for v in value.split(",") if v.strip()]
        return value

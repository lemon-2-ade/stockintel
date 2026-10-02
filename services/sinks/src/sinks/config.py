"""Sink configuration (environment variables prefixed ``SINK_``)."""

from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class SinkSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="SINK_", extra="ignore")

    batch_size: int = Field(default=1_000, ge=1, le=50_000)
    poll_timeout_s: float = Field(default=0.5, gt=0, le=10)
    flush_timeout_s: float = Field(default=10.0, gt=0)
    retry_attempts: int = Field(default=6, ge=1, le=100)
    metrics_port: int = Field(default=8003, ge=1, le=65_535)

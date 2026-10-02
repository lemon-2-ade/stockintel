"""Stream processor configuration (environment variables prefixed ``STREAM_``)."""

from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from stream_processor.app import AppConfig
from stream_processor.detectors import DetectorConfig


class StreamSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="STREAM_", extra="ignore")

    group_id: str = "stream-processor"
    client_id: str = "stream-processor"
    batch_size: int = Field(default=500, ge=1, le=10_000)
    poll_timeout_s: float = Field(default=0.5, gt=0, le=10)
    flush_timeout_s: float = Field(default=10.0, gt=0)
    warmup_messages: int = Field(default=2_000, ge=0, le=1_000_000)
    max_symbols: int = Field(default=10_000, ge=1)
    metrics_port: int = Field(default=8002, ge=1, le=65_535)

    return_threshold: float = Field(default=0.01, gt=0, lt=1)
    zscore_threshold: float = Field(default=5.0, gt=0)
    zscore_window: int = Field(default=120, ge=2)
    volume_ratio_threshold: float = Field(default=6.0, gt=1)
    volume_window: int = Field(default=60, ge=1)

    def app_config(self) -> AppConfig:
        return AppConfig(
            group_id=self.group_id,
            batch_size=self.batch_size,
            poll_timeout_s=self.poll_timeout_s,
            flush_timeout_s=self.flush_timeout_s,
            warmup_messages=self.warmup_messages,
        )

    def detector_config(self) -> DetectorConfig:
        return DetectorConfig(
            return_threshold=self.return_threshold,
            zscore_threshold=self.zscore_threshold,
            zscore_window=self.zscore_window,
            zscore_min_samples=min(30, self.zscore_window),
            volume_ratio_threshold=self.volume_ratio_threshold,
            volume_window=self.volume_window,
            volume_min_samples=min(20, self.volume_window),
        )

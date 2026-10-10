"""Inference configuration (environment variables prefixed ``INFERENCE_``).

The registry location comes from MLflow's own ``MLFLOW_TRACKING_URI``.
"""

from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from shared.features import FEATURE_SET_VERSION


class InferenceSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="INFERENCE_", extra="ignore")

    host: str = "0.0.0.0"  # noqa: S104 - container-internal bind; exposure is decided by compose
    port: int = Field(default=8010, ge=1, le=65_535)
    model_name: str = "stockintel-direction-h5"
    model_alias: str = "champion"
    refresh_interval_s: float = Field(default=60.0, gt=0)
    """How often the alias is re-resolved; a promotion is picked up within this time."""
    feature_set_version: str = FEATURE_SET_VERSION
    """Version of the feature code deployed with this service; models built on
    another version are refused at load time."""

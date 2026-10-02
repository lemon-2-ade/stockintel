"""Environment-driven configuration.

Every setting comes from environment variables (12-factor); ``.env.example``
documents them. Secrets are ``SecretStr`` so they never appear in ``repr`` or
structured logs.
"""

from __future__ import annotations

from functools import lru_cache
from typing import Literal
from urllib.parse import quote_plus

from pydantic import Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

_COMMON = SettingsConfigDict(extra="ignore", case_sensitive=False)


class KafkaSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="KAFKA_", **_COMMON)

    bootstrap_servers: str = "localhost:9094"
    security_protocol: Literal["PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"] = "PLAINTEXT"
    sasl_mechanism: Literal["PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"] | None = None
    sasl_username: str | None = None
    sasl_password: SecretStr | None = None

    # Topic provisioning. 1/1 for the single local broker; 3/2 in production.
    topic_replication_factor: int = Field(default=1, ge=1)
    topic_min_insync_replicas: int = Field(default=1, ge=1)

    @model_validator(mode="after")
    def _check(self) -> KafkaSettings:
        if self.topic_min_insync_replicas > self.topic_replication_factor:
            raise ValueError("topic_min_insync_replicas cannot exceed topic_replication_factor")
        if self.security_protocol.startswith("SASL") and not (
            self.sasl_mechanism and self.sasl_username and self.sasl_password
        ):
            raise ValueError("SASL security_protocol requires mechanism, username and password")
        return self


class PostgresSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="POSTGRES_", **_COMMON)

    host: str = "localhost"
    port: int = 5432
    db: str = "stockintel"
    user: str = "stockintel"
    password: SecretStr = SecretStr("")
    pool_size: int = Field(default=5, ge=1)
    max_overflow: int = Field(default=5, ge=0)
    statement_timeout_ms: int = Field(default=15_000, ge=0)

    def sqlalchemy_url(self, driver: str = "postgresql+psycopg") -> str:
        """Connection URL. Contains the password: never log it."""
        password = quote_plus(self.password.get_secret_value())
        return f"{driver}://{quote_plus(self.user)}:{password}@{self.host}:{self.port}/{self.db}"


class RedisSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="REDIS_", **_COMMON)

    host: str = "localhost"
    port: int = 6379
    db: int = Field(default=0, ge=0)
    password: SecretStr | None = None
    socket_timeout_s: float = Field(default=2.0, gt=0)

    def client_kwargs(self) -> dict[str, object]:
        """Keyword arguments for ``redis.Redis`` / ``redis.asyncio.Redis``."""
        return {
            "host": self.host,
            "port": self.port,
            "db": self.db,
            "password": self.password.get_secret_value() if self.password else None,
            "socket_timeout": self.socket_timeout_s,
            "socket_connect_timeout": self.socket_timeout_s,
            "decode_responses": True,
        }


class LogSettings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="LOG_", **_COMMON)

    level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = "INFO"
    format: Literal["json", "console"] = "json"


@lru_cache(maxsize=1)
def kafka_settings() -> KafkaSettings:
    return KafkaSettings()


@lru_cache(maxsize=1)
def postgres_settings() -> PostgresSettings:
    return PostgresSettings()


@lru_cache(maxsize=1)
def redis_settings() -> RedisSettings:
    return RedisSettings()

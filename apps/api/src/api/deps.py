"""FastAPI dependency providers (overridable in tests via ``app.dependency_overrides``)."""

from __future__ import annotations

from typing import Annotated

from fastapi import Depends, Header, Path, Request

from api.config import ApiSettings
from api.service import MarketService
from shared.schemas.base import SYMBOL_PATTERN


def get_settings(request: Request) -> ApiSettings:
    settings: ApiSettings = request.app.state.settings
    return settings


def get_service(request: Request) -> MarketService:
    service: MarketService = request.app.state.service
    return service


def get_user_id(
    settings: Annotated[ApiSettings, Depends(get_settings)],
    x_user_id: Annotated[str | None, Header(max_length=64, pattern=r"^[A-Za-z0-9_.\-]+$")] = None,
) -> str:
    """Opaque user id until authentication exists (documented limitation)."""
    return x_user_id or settings.default_user_id


SymbolPath = Annotated[
    str, Path(pattern=SYMBOL_PATTERN, description="Upper-case ticker, e.g. AAPL")
]
Service = Annotated[MarketService, Depends(get_service)]
Settings = Annotated[ApiSettings, Depends(get_settings)]
UserId = Annotated[str, Depends(get_user_id)]

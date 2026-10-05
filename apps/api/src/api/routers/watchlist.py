"""Per-user watchlist (user = ``X-User-Id`` header until auth exists)."""

from __future__ import annotations

from fastapi import APIRouter, Response, status

from api.deps import Service, SymbolPath, UserId
from api.schemas import WatchlistAdd, WatchlistItem

router = APIRouter(prefix="/api/v1/watchlist", tags=["watchlist"])


@router.get("", response_model=list[WatchlistItem])
async def get_watchlist(service: Service, user_id: UserId) -> list[WatchlistItem]:
    return await service.watchlist(user_id)


@router.post(
    "",
    status_code=status.HTTP_201_CREATED,
    responses={200: {"description": "Already on the watchlist"}},
)
async def add(
    body: WatchlistAdd, service: Service, user_id: UserId, response: Response
) -> dict[str, str]:
    created = await service.add_to_watchlist(user_id, body.symbol)
    if not created:
        response.status_code = status.HTTP_200_OK
    return {"symbol": body.symbol}


@router.delete("/{symbol}", status_code=status.HTTP_204_NO_CONTENT)
async def remove(symbol: SymbolPath, service: Service, user_id: UserId) -> Response:
    await service.remove_from_watchlist(user_id, symbol)
    return Response(status_code=status.HTTP_204_NO_CONTENT)

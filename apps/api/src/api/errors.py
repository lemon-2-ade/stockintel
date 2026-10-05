"""Structured errors: every failure is ``{"error": {"code", "message", "request_id"}}``."""

from __future__ import annotations

from typing import Any

from fastapi import FastAPI, Request, status
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from shared.observability.logs import get_logger

log = get_logger(__name__)


class ApiError(Exception):
    status_code = status.HTTP_400_BAD_REQUEST
    code = "bad_request"

    def __init__(self, message: str, *, details: dict[str, Any] | None = None) -> None:
        super().__init__(message)
        self.message = message
        self.details = details or {}


class NotFoundError(ApiError):
    status_code = status.HTTP_404_NOT_FOUND
    code = "not_found"


class ConflictError(ApiError):
    status_code = status.HTTP_409_CONFLICT
    code = "conflict"


class ServiceUnavailableError(ApiError):
    status_code = status.HTTP_503_SERVICE_UNAVAILABLE
    code = "service_unavailable"


def error_body(code: str, message: str, request: Request, **extra: Any) -> dict[str, Any]:
    return {
        "error": {
            "code": code,
            "message": message,
            "request_id": getattr(request.state, "request_id", None),
            **extra,
        }
    }


def install_error_handlers(app: FastAPI) -> None:
    @app.exception_handler(ApiError)
    async def _api_error(request: Request, exc: ApiError) -> JSONResponse:
        body = error_body(
            exc.code, exc.message, request, **({"details": exc.details} if exc.details else {})
        )
        return JSONResponse(body, status_code=exc.status_code)

    @app.exception_handler(RequestValidationError)
    async def _validation(request: Request, exc: RequestValidationError) -> JSONResponse:
        details = [{"loc": list(err["loc"]), "msg": err["msg"]} for err in exc.errors()]
        return JSONResponse(
            error_body("validation_error", "request validation failed", request, details=details),
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
        )

    @app.exception_handler(Exception)
    async def _unexpected(request: Request, exc: Exception) -> JSONResponse:
        log.exception("api.unhandled_error", path=request.url.path)
        return JSONResponse(
            error_body("internal_error", "unexpected server error", request),
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        )

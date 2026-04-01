"""
Standardised API Gateway response helpers.

All responses follow the shape:
  { "data": <payload>, "meta": { ... } }   -- success
  { "error": { "code": ..., "message": ... } }  -- error
"""

import json
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Optional
from uuid import UUID


# ─────────────────────────────────────────────────────────────
# JSON serialiser that handles Postgres types
# ─────────────────────────────────────────────────────────────
class _Encoder(json.JSONEncoder):
    def default(self, obj: Any) -> Any:
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        if isinstance(obj, Decimal):
            return float(obj)
        if isinstance(obj, UUID):
            return str(obj)
        return super().default(obj)


def _dumps(obj: Any) -> str:
    return json.dumps(obj, cls=_Encoder)


# ─────────────────────────────────────────────────────────────
# CORS headers (returned on every response)
# ─────────────────────────────────────────────────────────────
_CORS_HEADERS = {
    "Content-Type": "application/json",
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Headers": "Content-Type,Authorization",
    "Access-Control-Allow-Methods": "GET,POST,PUT,DELETE,OPTIONS",
}


# ─────────────────────────────────────────────────────────────
# Success responses
# ─────────────────────────────────────────────────────────────
def ok(data: Any, status: int = 200, meta: Optional[dict] = None) -> dict:
    body = {"data": data}
    if meta:
        body["meta"] = meta
    return {"statusCode": status, "headers": _CORS_HEADERS, "body": _dumps(body)}


def created(data: Any) -> dict:
    return ok(data, status=201)


def no_content() -> dict:
    return {"statusCode": 204, "headers": _CORS_HEADERS, "body": ""}


def paginated(data: list, total: int, page: int, limit: int) -> dict:
    meta = {
        "total": total,
        "page": page,
        "limit": limit,
        "pages": max(1, -(-total // limit)),  # ceiling division
    }
    return ok(data, meta=meta)


# ─────────────────────────────────────────────────────────────
# Error responses
# ─────────────────────────────────────────────────────────────
def error(code: str, message: str, status: int) -> dict:
    body = {"error": {"code": code, "message": message}}
    return {"statusCode": status, "headers": _CORS_HEADERS, "body": _dumps(body)}


def bad_request(message: str) -> dict:
    return error("BAD_REQUEST", message, 400)


def not_found(resource: str = "Resource") -> dict:
    return error("NOT_FOUND", f"{resource} not found.", 404)


def conflict(message: str) -> dict:
    return error("CONFLICT", message, 409)


def internal_error(message: str = "An unexpected error occurred.") -> dict:
    return error("INTERNAL_ERROR", message, 500)

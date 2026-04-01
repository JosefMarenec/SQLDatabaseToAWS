"""
Pagination and query-string parsing helpers.
"""

from typing import Tuple
import json


def parse_pagination(params: dict) -> Tuple[int, int, int]:
    """
    Extract and validate page/limit from query string params.

    Returns:
        (page, limit, offset)
    """
    try:
        page = max(1, int(params.get("page", 1)))
    except (ValueError, TypeError):
        page = 1

    try:
        limit = min(100, max(1, int(params.get("limit", 20))))
    except (ValueError, TypeError):
        limit = 20

    offset = (page - 1) * limit
    return page, limit, offset


def parse_body(event: dict) -> dict:
    """Safely parse JSON body from an API Gateway event."""
    body = event.get("body") or "{}"
    if isinstance(body, str):
        return json.loads(body)
    return body


def path_param(event: dict, name: str) -> str:
    """Extract a path parameter, raising ValueError if missing."""
    params = event.get("pathParameters") or {}
    value = params.get(name)
    if not value:
        raise ValueError(f"Missing path parameter: {name}")
    return value

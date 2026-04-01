"""
GET /health

Returns API + database health status.
"""

import logging
import os

from api.utils import response as res
from api.utils.db import get_cursor

logger = logging.getLogger(__name__)
logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))


def lambda_handler(event: dict, context) -> dict:
    """Health check — verifies DB connectivity and returns service status."""
    db_ok = False
    db_version = None

    try:
        with get_cursor() as cur:
            cur.execute("SELECT version() AS v")
            row = cur.fetchone()
            db_version = row["v"] if row else None
            db_ok = True
    except Exception as exc:
        logger.error("Health check DB error: %s", exc)

    status = "healthy" if db_ok else "degraded"
    payload = {
        "status": status,
        "service": "ecommerce-serverless-api",
        "database": {
            "connected": db_ok,
            "version": db_version,
        },
    }
    http_status = 200 if db_ok else 503
    return res.ok(payload, status=http_status)

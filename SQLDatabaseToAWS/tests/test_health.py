"""
Unit tests for the health Lambda handler.
"""

import json

import pytest

from tests.conftest import make_event
from api.handlers import health


class TestHealth:
    def test_healthy_when_db_connected(self, mock_cursor):
        mock_cursor.fetchone.return_value = {"v": "PostgreSQL 15.4"}

        event = make_event("GET", "/health", "/health")
        resp = health.lambda_handler(event, None)

        assert resp["statusCode"] == 200
        body = json.loads(resp["body"])
        assert body["data"]["status"] == "healthy"
        assert body["data"]["database"]["connected"] is True

    def test_degraded_when_db_fails(self):
        from unittest.mock import patch
        with patch("api.utils.db.get_cursor", side_effect=Exception("connection refused")):
            event = make_event("GET", "/health", "/health")
            resp = health.lambda_handler(event, None)

        assert resp["statusCode"] == 503
        body = json.loads(resp["body"])
        assert body["data"]["status"] == "degraded"
        assert body["data"]["database"]["connected"] is False

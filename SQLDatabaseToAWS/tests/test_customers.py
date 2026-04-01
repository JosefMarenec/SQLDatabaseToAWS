"""
Unit tests for the customers Lambda handler.
"""

import json

import pytest

from tests.conftest import SAMPLE_CUSTOMER, make_event
from api.handlers import customers


class TestListCustomers:
    def test_returns_paginated_list(self, mock_cursor):
        mock_cursor.fetchone.return_value = {"total": 1}
        mock_cursor.fetchall.return_value = [SAMPLE_CUSTOMER]

        event = make_event("GET", "/customers", "/customers")
        resp = customers.lambda_handler(event, None)

        assert resp["statusCode"] == 200
        body = json.loads(resp["body"])
        assert body["meta"]["total"] == 1

    def test_search_filter(self, mock_cursor):
        mock_cursor.fetchone.return_value = {"total": 0}
        mock_cursor.fetchall.return_value = []

        event = make_event("GET", "/customers", "/customers", query_params={"search": "alice"})
        resp = customers.lambda_handler(event, None)
        assert resp["statusCode"] == 200


class TestGetCustomer:
    def test_returns_customer(self, mock_cursor):
        mock_cursor.fetchone.return_value = SAMPLE_CUSTOMER

        event = make_event(
            "GET", "/customers/bbbbbbbb-0000-0000-0000-000000000001", "/customers/{id}",
            path_params={"id": "bbbbbbbb-0000-0000-0000-000000000001"},
        )
        resp = customers.lambda_handler(event, None)
        assert resp["statusCode"] == 200
        body = json.loads(resp["body"])
        assert body["data"]["email"] == "alice@example.com"

    def test_not_found(self, mock_cursor):
        mock_cursor.fetchone.return_value = None

        event = make_event(
            "GET", "/customers/nope", "/customers/{id}",
            path_params={"id": "nope"},
        )
        resp = customers.lambda_handler(event, None)
        assert resp["statusCode"] == 404


class TestCreateCustomer:
    def test_creates_customer(self, mock_cursor):
        mock_cursor.fetchone.return_value = SAMPLE_CUSTOMER

        event = make_event(
            "POST", "/customers", "/customers",
            body={"email": "alice@example.com", "first_name": "Alice", "last_name": "Smith"},
        )
        resp = customers.lambda_handler(event, None)
        assert resp["statusCode"] == 201

    def test_missing_required_fields(self, mock_cursor):
        event = make_event(
            "POST", "/customers", "/customers",
            body={"email": "bob@example.com"},  # missing first_name, last_name
        )
        resp = customers.lambda_handler(event, None)
        assert resp["statusCode"] == 400


class TestUpdateCustomer:
    def test_updates_customer(self, mock_cursor):
        updated = {**SAMPLE_CUSTOMER, "city": "New York"}
        mock_cursor.fetchone.return_value = updated

        event = make_event(
            "PUT", "/customers/bbbbbbbb-0000-0000-0000-000000000001", "/customers/{id}",
            path_params={"id": "bbbbbbbb-0000-0000-0000-000000000001"},
            body={"city": "New York"},
        )
        resp = customers.lambda_handler(event, None)
        assert resp["statusCode"] == 200

    def test_no_valid_fields(self, mock_cursor):
        event = make_event(
            "PUT", "/customers/abc", "/customers/{id}",
            path_params={"id": "abc"},
            body={"nonexistent": "field"},
        )
        resp = customers.lambda_handler(event, None)
        assert resp["statusCode"] == 400

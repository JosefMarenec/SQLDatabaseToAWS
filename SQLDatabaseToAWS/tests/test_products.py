"""
Unit tests for the products Lambda handler.
"""

import json
from unittest.mock import MagicMock, patch

import pytest

from tests.conftest import SAMPLE_PRODUCT, make_event
from api.handlers import products


# ─────────────────────────────────────────────────────────────
# GET /products
# ─────────────────────────────────────────────────────────────
class TestListProducts:
    def test_returns_paginated_list(self, mock_cursor):
        mock_cursor.fetchone.return_value = {"total": 1}
        mock_cursor.fetchall.return_value = [SAMPLE_PRODUCT]

        event = make_event("GET", "/products", "/products")
        resp = products.lambda_handler(event, None)

        assert resp["statusCode"] == 200
        body = json.loads(resp["body"])
        assert len(body["data"]) == 1
        assert body["meta"]["total"] == 1

    def test_filters_by_category(self, mock_cursor):
        mock_cursor.fetchone.return_value = {"total": 0}
        mock_cursor.fetchall.return_value = []

        event = make_event("GET", "/products", "/products", query_params={"category": "widgets"})
        resp = products.lambda_handler(event, None)
        assert resp["statusCode"] == 200

    def test_empty_result(self, mock_cursor):
        mock_cursor.fetchone.return_value = {"total": 0}
        mock_cursor.fetchall.return_value = []

        event = make_event("GET", "/products", "/products")
        resp = products.lambda_handler(event, None)
        body = json.loads(resp["body"])
        assert body["data"] == []
        assert body["meta"]["total"] == 0


# ─────────────────────────────────────────────────────────────
# GET /products/{id}
# ─────────────────────────────────────────────────────────────
class TestGetProduct:
    def test_returns_product(self, mock_cursor):
        mock_cursor.fetchone.return_value = SAMPLE_PRODUCT

        event = make_event(
            "GET", "/products/aaaaaaaa-0000-0000-0000-000000000001",
            "/products/{id}",
            path_params={"id": "aaaaaaaa-0000-0000-0000-000000000001"},
        )
        resp = products.lambda_handler(event, None)
        assert resp["statusCode"] == 200
        body = json.loads(resp["body"])
        assert body["data"]["sku"] == "SKU-001"

    def test_not_found(self, mock_cursor):
        mock_cursor.fetchone.return_value = None

        event = make_event(
            "GET", "/products/nonexistent", "/products/{id}",
            path_params={"id": "nonexistent"},
        )
        resp = products.lambda_handler(event, None)
        assert resp["statusCode"] == 404


# ─────────────────────────────────────────────────────────────
# POST /products
# ─────────────────────────────────────────────────────────────
class TestCreateProduct:
    def test_creates_product(self, mock_cursor):
        mock_cursor.fetchone.return_value = SAMPLE_PRODUCT

        event = make_event(
            "POST", "/products", "/products",
            body={"sku": "SKU-001", "name": "Test Widget", "category": "widgets", "price": 9.99},
        )
        resp = products.lambda_handler(event, None)
        assert resp["statusCode"] == 201

    def test_missing_required_fields(self, mock_cursor):
        event = make_event(
            "POST", "/products", "/products",
            body={"sku": "SKU-002"},          # missing name, category, price
        )
        resp = products.lambda_handler(event, None)
        assert resp["statusCode"] == 400
        body = json.loads(resp["body"])
        assert body["error"]["code"] == "BAD_REQUEST"

    def test_invalid_json(self, mock_cursor):
        event = make_event("POST", "/products", "/products")
        event["body"] = "not-json{"
        resp = products.lambda_handler(event, None)
        assert resp["statusCode"] == 400


# ─────────────────────────────────────────────────────────────
# PUT /products/{id}
# ─────────────────────────────────────────────────────────────
class TestUpdateProduct:
    def test_updates_product(self, mock_cursor):
        updated = {**SAMPLE_PRODUCT, "price": 14.99}
        mock_cursor.fetchone.return_value = updated

        event = make_event(
            "PUT", "/products/aaaaaaaa-0000-0000-0000-000000000001", "/products/{id}",
            path_params={"id": "aaaaaaaa-0000-0000-0000-000000000001"},
            body={"price": 14.99},
        )
        resp = products.lambda_handler(event, None)
        assert resp["statusCode"] == 200

    def test_no_valid_fields(self, mock_cursor):
        event = make_event(
            "PUT", "/products/abc", "/products/{id}",
            path_params={"id": "abc"},
            body={"unknown_field": "value"},
        )
        resp = products.lambda_handler(event, None)
        assert resp["statusCode"] == 400


# ─────────────────────────────────────────────────────────────
# DELETE /products/{id}
# ─────────────────────────────────────────────────────────────
class TestDeleteProduct:
    def test_soft_deletes_product(self, mock_cursor):
        mock_cursor.fetchone.return_value = {"id": "aaaaaaaa-0000-0000-0000-000000000001"}

        event = make_event(
            "DELETE", "/products/aaaaaaaa-0000-0000-0000-000000000001", "/products/{id}",
            path_params={"id": "aaaaaaaa-0000-0000-0000-000000000001"},
        )
        resp = products.lambda_handler(event, None)
        assert resp["statusCode"] == 204

    def test_not_found(self, mock_cursor):
        mock_cursor.fetchone.return_value = None

        event = make_event(
            "DELETE", "/products/nonexistent", "/products/{id}",
            path_params={"id": "nonexistent"},
        )
        resp = products.lambda_handler(event, None)
        assert resp["statusCode"] == 404

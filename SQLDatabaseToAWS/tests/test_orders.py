"""
Unit tests for the orders Lambda handler.
"""

import json
from decimal import Decimal

import pytest

from tests.conftest import SAMPLE_CUSTOMER, SAMPLE_ORDER, SAMPLE_PRODUCT, make_event
from api.handlers import orders


SAMPLE_ITEM = {
    "id": "dddddddd-0000-0000-0000-000000000001",
    "order_id": SAMPLE_ORDER["id"],
    "product_id": SAMPLE_PRODUCT["id"],
    "quantity": 2,
    "unit_price": Decimal("9.99"),
    "line_total": Decimal("19.98"),
    "product_name": "Test Widget",
    "sku": "SKU-001",
    "category": "widgets",
}


class TestListOrders:
    def test_returns_paginated_list(self, mock_cursor):
        mock_cursor.fetchone.return_value = {"total": 1}
        mock_cursor.fetchall.return_value = [SAMPLE_ORDER]

        event = make_event("GET", "/orders", "/orders")
        resp = orders.lambda_handler(event, None)

        assert resp["statusCode"] == 200
        body = json.loads(resp["body"])
        assert body["meta"]["total"] == 1

    def test_invalid_status_filter(self, mock_cursor):
        event = make_event("GET", "/orders", "/orders", query_params={"status": "flying"})
        resp = orders.lambda_handler(event, None)
        assert resp["statusCode"] == 400


class TestGetOrder:
    def test_returns_order_with_items(self, mock_cursor):
        # First fetchone → order row, fetchall → items
        mock_cursor.fetchone.return_value = {**SAMPLE_ORDER, "email": "alice@example.com",
                                             "first_name": "Alice", "last_name": "Smith"}
        mock_cursor.fetchall.return_value = [SAMPLE_ITEM]

        event = make_event(
            "GET", f"/orders/{SAMPLE_ORDER['id']}", "/orders/{id}",
            path_params={"id": SAMPLE_ORDER["id"]},
        )
        resp = orders.lambda_handler(event, None)
        assert resp["statusCode"] == 200
        body = json.loads(resp["body"])
        assert "items" in body["data"]

    def test_not_found(self, mock_cursor):
        mock_cursor.fetchone.return_value = None

        event = make_event(
            "GET", "/orders/nope", "/orders/{id}",
            path_params={"id": "nope"},
        )
        resp = orders.lambda_handler(event, None)
        assert resp["statusCode"] == 404


class TestCreateOrder:
    def test_creates_order_successfully(self, mock_cursor):
        # Sequence of fetchone calls:
        # 1. customer check → customer row
        # 2. order INSERT RETURNING → order id
        # 3. final order SELECT → order row
        mock_cursor.fetchone.side_effect = [
            {"id": SAMPLE_CUSTOMER["id"]},                    # customer exists
            {"id": SAMPLE_ORDER["id"]},                       # order created
            {**SAMPLE_ORDER},                                  # order fetch
        ]
        mock_cursor.fetchall.side_effect = [
            [SAMPLE_PRODUCT],    # products FOR UPDATE
            [SAMPLE_ITEM],       # order items
        ]

        event = make_event(
            "POST", "/orders", "/orders",
            body={
                "customer_id": SAMPLE_CUSTOMER["id"],
                "items": [{"product_id": SAMPLE_PRODUCT["id"], "quantity": 2}],
                "tax": 1.80,
                "shipping": 5.00,
            },
        )
        resp = orders.lambda_handler(event, None)
        assert resp["statusCode"] == 201

    def test_missing_customer_id(self, mock_cursor):
        event = make_event(
            "POST", "/orders", "/orders",
            body={"items": [{"product_id": "abc", "quantity": 1}]},
        )
        resp = orders.lambda_handler(event, None)
        assert resp["statusCode"] == 400

    def test_missing_items(self, mock_cursor):
        event = make_event(
            "POST", "/orders", "/orders",
            body={"customer_id": SAMPLE_CUSTOMER["id"]},
        )
        resp = orders.lambda_handler(event, None)
        assert resp["statusCode"] == 400

    def test_customer_not_found(self, mock_cursor):
        mock_cursor.fetchone.return_value = None  # customer not found

        event = make_event(
            "POST", "/orders", "/orders",
            body={
                "customer_id": "nonexistent",
                "items": [{"product_id": SAMPLE_PRODUCT["id"], "quantity": 1}],
            },
        )
        resp = orders.lambda_handler(event, None)
        assert resp["statusCode"] == 404


class TestUpdateOrderStatus:
    def test_updates_status(self, mock_cursor):
        mock_cursor.fetchone.return_value = {**SAMPLE_ORDER, "status": "confirmed"}

        event = make_event(
            "PUT", f"/orders/{SAMPLE_ORDER['id']}", "/orders/{id}",
            path_params={"id": SAMPLE_ORDER["id"]},
            body={"status": "confirmed"},
        )
        resp = orders.lambda_handler(event, None)
        assert resp["statusCode"] == 200

    def test_invalid_status(self, mock_cursor):
        event = make_event(
            "PUT", "/orders/abc", "/orders/{id}",
            path_params={"id": "abc"},
            body={"status": "teleported"},
        )
        resp = orders.lambda_handler(event, None)
        assert resp["statusCode"] == 400

    def test_missing_status(self, mock_cursor):
        event = make_event(
            "PUT", "/orders/abc", "/orders/{id}",
            path_params={"id": "abc"},
            body={"notes": "something"},
        )
        resp = orders.lambda_handler(event, None)
        assert resp["statusCode"] == 400


class TestListCustomerOrders:
    def test_returns_customer_orders(self, mock_cursor):
        mock_cursor.fetchone.side_effect = [
            {"id": SAMPLE_CUSTOMER["id"]},   # customer exists check
            {"total": 1},                     # count
        ]
        mock_cursor.fetchall.return_value = [SAMPLE_ORDER]

        event = make_event(
            "GET", f"/customers/{SAMPLE_CUSTOMER['id']}/orders",
            "/customers/{id}/orders",
            path_params={"id": SAMPLE_CUSTOMER["id"]},
        )
        resp = orders.lambda_handler(event, None)
        assert resp["statusCode"] == 200

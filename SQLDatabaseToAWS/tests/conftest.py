"""
Shared pytest fixtures.

All DB calls are mocked via unittest.mock so tests run without
a real Postgres connection.
"""

import json
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest


# ─────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────
def make_event(
    method: str = "GET",
    path: str = "/",
    resource: str = "/",
    path_params: dict | None = None,
    query_params: dict | None = None,
    body: dict | None = None,
) -> dict:
    return {
        "httpMethod": method,
        "path": path,
        "resource": resource,
        "pathParameters": path_params,
        "queryStringParameters": query_params,
        "body": json.dumps(body) if body else None,
    }


def fake_row(**kwargs) -> MagicMock:
    """Return a dict-like object (mimics RealDictRow)."""
    return dict(**kwargs)


# ─────────────────────────────────────────────────────────────
# Fixtures
# ─────────────────────────────────────────────────────────────
@pytest.fixture
def mock_cursor():
    """
    Yields a MagicMock cursor and patches get_cursor in every handler
    module so tests never touch a real database.
    """
    cursor = MagicMock()
    cursor.__enter__ = MagicMock(return_value=cursor)
    cursor.__exit__ = MagicMock(return_value=False)

    targets = [
        "api.handlers.products.get_cursor",
        "api.handlers.customers.get_cursor",
        "api.handlers.orders.get_cursor",
        "api.handlers.health.get_cursor",
    ]

    with (
        patch(targets[0], return_value=cursor),
        patch(targets[1], return_value=cursor),
        patch(targets[2], return_value=cursor),
        patch(targets[3], return_value=cursor),
    ):
        yield cursor


SAMPLE_PRODUCT = {
    "id": "aaaaaaaa-0000-0000-0000-000000000001",
    "sku": "SKU-001",
    "name": "Test Widget",
    "description": "A widget for testing",
    "category": "widgets",
    "price": Decimal("9.99"),
    "stock_qty": 100,
    "is_active": True,
    "created_at": "2026-01-01T00:00:00+00:00",
    "updated_at": "2026-01-01T00:00:00+00:00",
}

SAMPLE_CUSTOMER = {
    "id": "bbbbbbbb-0000-0000-0000-000000000001",
    "email": "alice@example.com",
    "first_name": "Alice",
    "last_name": "Smith",
    "phone": "+1-555-0100",
    "address_line1": "1 Main St",
    "address_line2": None,
    "city": "Anytown",
    "state": "CA",
    "postal_code": "90210",
    "country": "US",
    "is_active": True,
    "created_at": "2026-01-01T00:00:00+00:00",
    "updated_at": "2026-01-01T00:00:00+00:00",
}

SAMPLE_ORDER = {
    "id": "cccccccc-0000-0000-0000-000000000001",
    "customer_id": SAMPLE_CUSTOMER["id"],
    "status": "pending",
    "subtotal": Decimal("19.98"),
    "tax": Decimal("1.80"),
    "shipping": Decimal("5.00"),
    "total": Decimal("26.78"),
    "notes": None,
    "shipped_at": None,
    "delivered_at": None,
    "created_at": "2026-01-01T00:00:00+00:00",
    "updated_at": "2026-01-01T00:00:00+00:00",
}

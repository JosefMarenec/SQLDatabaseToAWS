"""
Orders resource handler.

Routes:
    GET    /orders                        list_orders
    POST   /orders                        create_order
    GET    /orders/{id}                   get_order  (with line items)
    PUT    /orders/{id}                   update_order_status
    GET    /customers/{id}/orders         list_customer_orders
"""

import logging
import os

import psycopg2

from api.utils import response as res
from api.utils.db import get_cursor
from api.utils.pagination import parse_body, parse_pagination, path_param

logger = logging.getLogger(__name__)
logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))

VALID_STATUSES = {
    "pending", "confirmed", "processing",
    "shipped", "delivered", "cancelled", "refunded",
}


# ─────────────────────────────────────────────────────────────
# Router
# ─────────────────────────────────────────────────────────────
def lambda_handler(event: dict, context) -> dict:
    method   = event.get("httpMethod", "")
    resource = event.get("resource", "")
    has_id   = bool((event.get("pathParameters") or {}).get("id"))

    # Customer sub-resource: GET /customers/{id}/orders
    if "/customers/" in resource and resource.endswith("/orders"):
        return list_customer_orders(event)

    if method == "GET"  and not has_id: return list_orders(event)
    if method == "POST" and not has_id: return create_order(event)
    if method == "GET"  and has_id:     return get_order(event)
    if method == "PUT"  and has_id:     return update_order_status(event)
    if method == "OPTIONS":             return res.ok({})

    return res.error("METHOD_NOT_ALLOWED", f"Method {method} not supported.", 405)


# ─────────────────────────────────────────────────────────────
# GET /orders
# ─────────────────────────────────────────────────────────────
def list_orders(event: dict) -> dict:
    params = event.get("queryStringParameters") or {}
    page, limit, offset = parse_pagination(params)

    status      = params.get("status")
    customer_id = params.get("customer_id")

    filters: list = []
    args:    list = []

    if status:
        if status not in VALID_STATUSES:
            return res.bad_request(f"Invalid status. Must be one of: {', '.join(sorted(VALID_STATUSES))}")
        filters.append("o.status = %s")
        args.append(status)
    if customer_id:
        filters.append("o.customer_id = %s")
        args.append(customer_id)

    where = ("WHERE " + " AND ".join(filters)) if filters else ""

    try:
        with get_cursor() as cur:
            cur.execute(f"SELECT COUNT(*) AS total FROM orders o {where}", args)
            total = cur.fetchone()["total"]

            cur.execute(
                f"""
                SELECT o.id, o.customer_id, o.status,
                       o.subtotal, o.tax, o.shipping, o.total,
                       o.notes, o.shipped_at, o.delivered_at,
                       o.created_at, o.updated_at,
                       c.email, c.first_name, c.last_name
                FROM orders o
                JOIN customers c ON c.id = o.customer_id
                {where}
                ORDER BY o.created_at DESC
                LIMIT %s OFFSET %s
                """,
                args + [limit, offset],
            )
            orders = [dict(row) for row in cur.fetchall()]

        return res.paginated(orders, total=total, page=page, limit=limit)
    except Exception as exc:
        logger.exception("list_orders error")
        return res.internal_error(str(exc))


# ─────────────────────────────────────────────────────────────
# POST /orders
# ─────────────────────────────────────────────────────────────
def create_order(event: dict) -> dict:
    """
    Expected body:
    {
        "customer_id": "<uuid>",
        "items": [
            {"product_id": "<uuid>", "quantity": 2},
            ...
        ],
        "tax": 5.00,        // optional
        "shipping": 9.99,   // optional
        "notes": "..."      // optional
    }
    """
    try:
        body = parse_body(event)
    except Exception:
        return res.bad_request("Invalid JSON body.")

    if not body.get("customer_id"):
        return res.bad_request("Missing required field: customer_id")
    if not body.get("items"):
        return res.bad_request("Order must contain at least one item.")

    customer_id = body["customer_id"]
    items       = body["items"]
    tax         = float(body.get("tax", 0))
    shipping    = float(body.get("shipping", 0))
    notes       = body.get("notes")

    # Validate items structure
    for item in items:
        if not item.get("product_id") or not item.get("quantity"):
            return res.bad_request("Each item must have product_id and quantity.")

    try:
        with get_cursor(commit=True) as cur:
            # Verify customer exists
            cur.execute("SELECT id FROM customers WHERE id = %s AND is_active = TRUE", [customer_id])
            if not cur.fetchone():
                return res.not_found("Customer")

            # Lock product rows + validate stock
            product_ids = [item["product_id"] for item in items]
            cur.execute(
                """
                SELECT id, price, stock_qty, name
                FROM products
                WHERE id = ANY(%s) AND is_active = TRUE
                FOR UPDATE
                """,
                [product_ids],
            )
            products_map = {str(row["id"]): dict(row) for row in cur.fetchall()}

            for item in items:
                pid = item["product_id"]
                if pid not in products_map:
                    return res.not_found(f"Product {pid}")
                if products_map[pid]["stock_qty"] < item["quantity"]:
                    return res.bad_request(
                        f"Insufficient stock for '{products_map[pid]['name']}' "
                        f"(requested {item['quantity']}, available {products_map[pid]['stock_qty']})."
                    )

            # Create the order
            cur.execute(
                """
                INSERT INTO orders (customer_id, tax, shipping, notes)
                VALUES (%s, %s, %s, %s)
                RETURNING id
                """,
                [customer_id, tax, shipping, notes],
            )
            order_id = str(cur.fetchone()["id"])

            # Insert order items + decrement stock
            for item in items:
                pid       = item["product_id"]
                qty       = item["quantity"]
                unit_price = products_map[pid]["price"]

                cur.execute(
                    """
                    INSERT INTO order_items (order_id, product_id, quantity, unit_price)
                    VALUES (%s, %s, %s, %s)
                    """,
                    [order_id, pid, qty, unit_price],
                )
                cur.execute(
                    "UPDATE products SET stock_qty = stock_qty - %s WHERE id = %s",
                    [qty, pid],
                )

            # Fetch the completed order
            cur.execute(
                """
                SELECT o.id, o.customer_id, o.status,
                       o.subtotal, o.tax, o.shipping, o.total,
                       o.notes, o.created_at, o.updated_at
                FROM orders o WHERE o.id = %s
                """,
                [order_id],
            )
            order = dict(cur.fetchone())

            cur.execute(
                """
                SELECT oi.id, oi.product_id, oi.quantity,
                       oi.unit_price, oi.line_total,
                       p.name AS product_name, p.sku
                FROM order_items oi
                JOIN products p ON p.id = oi.product_id
                WHERE oi.order_id = %s
                """,
                [order_id],
            )
            order["items"] = [dict(row) for row in cur.fetchall()]

        return res.created(order)
    except res.__class__:
        raise
    except Exception as exc:
        logger.exception("create_order error")
        return res.internal_error(str(exc))


# ─────────────────────────────────────────────────────────────
# GET /orders/{id}
# ─────────────────────────────────────────────────────────────
def get_order(event: dict) -> dict:
    try:
        order_id = path_param(event, "id")
    except ValueError as exc:
        return res.bad_request(str(exc))

    try:
        with get_cursor() as cur:
            cur.execute(
                """
                SELECT o.id, o.customer_id, o.status,
                       o.subtotal, o.tax, o.shipping, o.total,
                       o.notes, o.shipped_at, o.delivered_at,
                       o.created_at, o.updated_at,
                       c.email, c.first_name, c.last_name
                FROM orders o
                JOIN customers c ON c.id = o.customer_id
                WHERE o.id = %s
                """,
                [order_id],
            )
            row = cur.fetchone()
            if not row:
                return res.not_found("Order")

            order = dict(row)

            cur.execute(
                """
                SELECT oi.id, oi.product_id, oi.quantity,
                       oi.unit_price, oi.line_total,
                       p.name AS product_name, p.sku, p.category
                FROM order_items oi
                JOIN products p ON p.id = oi.product_id
                WHERE oi.order_id = %s
                ORDER BY oi.created_at
                """,
                [order_id],
            )
            order["items"] = [dict(r) for r in cur.fetchall()]

        return res.ok(order)
    except Exception as exc:
        logger.exception("get_order error")
        return res.internal_error(str(exc))


# ─────────────────────────────────────────────────────────────
# PUT /orders/{id}   (status transition only)
# ─────────────────────────────────────────────────────────────
def update_order_status(event: dict) -> dict:
    try:
        order_id = path_param(event, "id")
        body = parse_body(event)
    except ValueError as exc:
        return res.bad_request(str(exc))
    except Exception:
        return res.bad_request("Invalid JSON body.")

    new_status = body.get("status")
    if not new_status:
        return res.bad_request("Missing required field: status")
    if new_status not in VALID_STATUSES:
        return res.bad_request(f"Invalid status. Must be one of: {', '.join(sorted(VALID_STATUSES))}")

    extra_fields: dict = {}
    if new_status == "shipped":
        extra_fields["shipped_at"] = "NOW()"
    elif new_status == "delivered":
        extra_fields["delivered_at"] = "NOW()"

    set_parts = ["status = %s"]
    values    = [new_status]

    for field, value in extra_fields.items():
        set_parts.append(f"{field} = {value}")   # NOW() is a SQL function, not a param

    set_clause = ", ".join(set_parts)

    try:
        with get_cursor(commit=True) as cur:
            cur.execute(
                f"""
                UPDATE orders SET {set_clause}
                WHERE id = %s
                RETURNING id, customer_id, status, subtotal, tax,
                          shipping, total, notes, shipped_at, delivered_at,
                          created_at, updated_at
                """,
                values + [order_id],
            )
            row = cur.fetchone()

        if not row:
            return res.not_found("Order")
        return res.ok(dict(row))
    except Exception as exc:
        logger.exception("update_order_status error")
        return res.internal_error(str(exc))


# ─────────────────────────────────────────────────────────────
# GET /customers/{id}/orders
# ─────────────────────────────────────────────────────────────
def list_customer_orders(event: dict) -> dict:
    try:
        customer_id = path_param(event, "id")
    except ValueError as exc:
        return res.bad_request(str(exc))

    params = event.get("queryStringParameters") or {}
    page, limit, offset = parse_pagination(params)
    status = params.get("status")

    filters = ["o.customer_id = %s"]
    args    = [customer_id]

    if status:
        if status not in VALID_STATUSES:
            return res.bad_request(f"Invalid status.")
        filters.append("o.status = %s")
        args.append(status)

    where = "WHERE " + " AND ".join(filters)

    try:
        with get_cursor() as cur:
            # Verify customer exists
            cur.execute("SELECT id FROM customers WHERE id = %s", [customer_id])
            if not cur.fetchone():
                return res.not_found("Customer")

            cur.execute(f"SELECT COUNT(*) AS total FROM orders o {where}", args)
            total = cur.fetchone()["total"]

            cur.execute(
                f"""
                SELECT o.id, o.status, o.subtotal, o.tax,
                       o.shipping, o.total, o.notes,
                       o.shipped_at, o.delivered_at,
                       o.created_at, o.updated_at
                FROM orders o
                {where}
                ORDER BY o.created_at DESC
                LIMIT %s OFFSET %s
                """,
                args + [limit, offset],
            )
            orders = [dict(row) for row in cur.fetchall()]

        return res.paginated(orders, total=total, page=page, limit=limit)
    except Exception as exc:
        logger.exception("list_customer_orders error")
        return res.internal_error(str(exc))

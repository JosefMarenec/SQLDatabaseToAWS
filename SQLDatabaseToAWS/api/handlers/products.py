"""
Products resource handler.

Routes:
    GET    /products                 list_products
    POST   /products                 create_product
    GET    /products/{id}            get_product
    PUT    /products/{id}            update_product
    DELETE /products/{id}            delete_product
"""

import logging
import os

import psycopg2

from api.utils import response as res
from api.utils.db import get_cursor
from api.utils.pagination import parse_body, parse_pagination, path_param

logger = logging.getLogger(__name__)
logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))

# Fields that can be set on create / update
_ALLOWED_FIELDS = {"sku", "name", "description", "category", "price", "stock_qty", "is_active"}
_REQUIRED_CREATE = {"sku", "name", "category", "price"}


# ─────────────────────────────────────────────────────────────
# Router
# ─────────────────────────────────────────────────────────────
def lambda_handler(event: dict, context) -> dict:
    method = event.get("httpMethod", "")
    path   = event.get("path", "")
    has_id = bool((event.get("pathParameters") or {}).get("id"))

    if method == "GET"    and not has_id: return list_products(event)
    if method == "POST"   and not has_id: return create_product(event)
    if method == "GET"    and has_id:     return get_product(event)
    if method == "PUT"    and has_id:     return update_product(event)
    if method == "DELETE" and has_id:     return delete_product(event)
    if method == "OPTIONS":               return res.ok({})

    return res.error("METHOD_NOT_ALLOWED", f"Method {method} not supported.", 405)


# ─────────────────────────────────────────────────────────────
# GET /products
# ─────────────────────────────────────────────────────────────
def list_products(event: dict) -> dict:
    params   = event.get("queryStringParameters") or {}
    page, limit, offset = parse_pagination(params)

    category   = params.get("category")
    is_active  = params.get("is_active")
    min_price  = params.get("min_price")
    max_price  = params.get("max_price")
    search     = params.get("search")

    filters = []
    args: list = []

    if category:
        filters.append("category = %s")
        args.append(category)
    if is_active is not None:
        filters.append("is_active = %s")
        args.append(is_active.lower() == "true")
    if min_price:
        filters.append("price >= %s")
        args.append(float(min_price))
    if max_price:
        filters.append("price <= %s")
        args.append(float(max_price))
    if search:
        filters.append("(name ILIKE %s OR description ILIKE %s OR sku ILIKE %s)")
        term = f"%{search}%"
        args += [term, term, term]

    where = ("WHERE " + " AND ".join(filters)) if filters else ""

    try:
        with get_cursor() as cur:
            cur.execute(f"SELECT COUNT(*) AS total FROM products {where}", args)
            total = cur.fetchone()["total"]

            cur.execute(
                f"""
                SELECT id, sku, name, description, category,
                       price, stock_qty, is_active, created_at, updated_at
                FROM products
                {where}
                ORDER BY created_at DESC
                LIMIT %s OFFSET %s
                """,
                args + [limit, offset],
            )
            products = [dict(row) for row in cur.fetchall()]

        return res.paginated(products, total=total, page=page, limit=limit)
    except Exception as exc:
        logger.exception("list_products error")
        return res.internal_error(str(exc))


# ─────────────────────────────────────────────────────────────
# POST /products
# ─────────────────────────────────────────────────────────────
def create_product(event: dict) -> dict:
    try:
        body = parse_body(event)
    except Exception:
        return res.bad_request("Invalid JSON body.")

    missing = _REQUIRED_CREATE - body.keys()
    if missing:
        return res.bad_request(f"Missing required fields: {', '.join(sorted(missing))}")

    fields = {k: v for k, v in body.items() if k in _ALLOWED_FIELDS}
    cols   = ", ".join(fields.keys())
    placeholders = ", ".join(["%s"] * len(fields))

    try:
        with get_cursor(commit=True) as cur:
            cur.execute(
                f"""
                INSERT INTO products ({cols})
                VALUES ({placeholders})
                RETURNING id, sku, name, description, category,
                          price, stock_qty, is_active, created_at, updated_at
                """,
                list(fields.values()),
            )
            product = dict(cur.fetchone())
        return res.created(product)
    except psycopg2.errors.UniqueViolation:
        return res.conflict(f"A product with SKU '{body.get('sku')}' already exists.")
    except Exception as exc:
        logger.exception("create_product error")
        return res.internal_error(str(exc))


# ─────────────────────────────────────────────────────────────
# GET /products/{id}
# ─────────────────────────────────────────────────────────────
def get_product(event: dict) -> dict:
    try:
        product_id = path_param(event, "id")
    except ValueError as exc:
        return res.bad_request(str(exc))

    try:
        with get_cursor() as cur:
            cur.execute(
                """
                SELECT id, sku, name, description, category,
                       price, stock_qty, is_active, created_at, updated_at
                FROM products WHERE id = %s
                """,
                [product_id],
            )
            row = cur.fetchone()

        if not row:
            return res.not_found("Product")
        return res.ok(dict(row))
    except Exception as exc:
        logger.exception("get_product error")
        return res.internal_error(str(exc))


# ─────────────────────────────────────────────────────────────
# PUT /products/{id}
# ─────────────────────────────────────────────────────────────
def update_product(event: dict) -> dict:
    try:
        product_id = path_param(event, "id")
        body = parse_body(event)
    except ValueError as exc:
        return res.bad_request(str(exc))
    except Exception:
        return res.bad_request("Invalid JSON body.")

    updates = {k: v for k, v in body.items() if k in _ALLOWED_FIELDS}
    if not updates:
        return res.bad_request("No valid fields provided for update.")

    set_clause = ", ".join(f"{k} = %s" for k in updates)
    values = list(updates.values()) + [product_id]

    try:
        with get_cursor(commit=True) as cur:
            cur.execute(
                f"""
                UPDATE products SET {set_clause}
                WHERE id = %s
                RETURNING id, sku, name, description, category,
                          price, stock_qty, is_active, created_at, updated_at
                """,
                values,
            )
            row = cur.fetchone()

        if not row:
            return res.not_found("Product")
        return res.ok(dict(row))
    except psycopg2.errors.UniqueViolation:
        return res.conflict("A product with that SKU already exists.")
    except Exception as exc:
        logger.exception("update_product error")
        return res.internal_error(str(exc))


# ─────────────────────────────────────────────────────────────
# DELETE /products/{id}
# ─────────────────────────────────────────────────────────────
def delete_product(event: dict) -> dict:
    try:
        product_id = path_param(event, "id")
    except ValueError as exc:
        return res.bad_request(str(exc))

    try:
        with get_cursor(commit=True) as cur:
            # Soft delete — keeps referential integrity with order_items
            cur.execute(
                "UPDATE products SET is_active = FALSE WHERE id = %s RETURNING id",
                [product_id],
            )
            row = cur.fetchone()

        if not row:
            return res.not_found("Product")
        return res.no_content()
    except Exception as exc:
        logger.exception("delete_product error")
        return res.internal_error(str(exc))

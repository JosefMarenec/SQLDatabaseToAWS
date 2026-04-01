"""
Customers resource handler.

Routes:
    GET    /customers                list_customers
    POST   /customers                create_customer
    GET    /customers/{id}           get_customer
    PUT    /customers/{id}           update_customer
"""

import logging
import os

import psycopg2

from api.utils import response as res
from api.utils.db import get_cursor
from api.utils.pagination import parse_body, parse_pagination, path_param

logger = logging.getLogger(__name__)
logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))

_ALLOWED_FIELDS = {
    "email", "first_name", "last_name", "phone",
    "address_line1", "address_line2", "city", "state",
    "postal_code", "country", "is_active",
}
_REQUIRED_CREATE = {"email", "first_name", "last_name"}


# ─────────────────────────────────────────────────────────────
# Router
# ─────────────────────────────────────────────────────────────
def lambda_handler(event: dict, context) -> dict:
    method = event.get("httpMethod", "")
    has_id = bool((event.get("pathParameters") or {}).get("id"))

    if method == "GET"  and not has_id: return list_customers(event)
    if method == "POST" and not has_id: return create_customer(event)
    if method == "GET"  and has_id:     return get_customer(event)
    if method == "PUT"  and has_id:     return update_customer(event)
    if method == "OPTIONS":             return res.ok({})

    return res.error("METHOD_NOT_ALLOWED", f"Method {method} not supported.", 405)


# ─────────────────────────────────────────────────────────────
# GET /customers
# ─────────────────────────────────────────────────────────────
def list_customers(event: dict) -> dict:
    params = event.get("queryStringParameters") or {}
    page, limit, offset = parse_pagination(params)

    search    = params.get("search")
    country   = params.get("country")
    is_active = params.get("is_active")

    filters: list = []
    args:    list = []

    if search:
        filters.append(
            "(email ILIKE %s OR first_name ILIKE %s OR last_name ILIKE %s)"
        )
        term = f"%{search}%"
        args += [term, term, term]
    if country:
        filters.append("country = %s")
        args.append(country.upper())
    if is_active is not None:
        filters.append("is_active = %s")
        args.append(is_active.lower() == "true")

    where = ("WHERE " + " AND ".join(filters)) if filters else ""

    try:
        with get_cursor() as cur:
            cur.execute(f"SELECT COUNT(*) AS total FROM customers {where}", args)
            total = cur.fetchone()["total"]

            cur.execute(
                f"""
                SELECT id, email, first_name, last_name, phone,
                       address_line1, address_line2, city, state,
                       postal_code, country, is_active, created_at, updated_at
                FROM customers
                {where}
                ORDER BY created_at DESC
                LIMIT %s OFFSET %s
                """,
                args + [limit, offset],
            )
            customers = [dict(row) for row in cur.fetchall()]

        return res.paginated(customers, total=total, page=page, limit=limit)
    except Exception as exc:
        logger.exception("list_customers error")
        return res.internal_error(str(exc))


# ─────────────────────────────────────────────────────────────
# POST /customers
# ─────────────────────────────────────────────────────────────
def create_customer(event: dict) -> dict:
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
                INSERT INTO customers ({cols})
                VALUES ({placeholders})
                RETURNING id, email, first_name, last_name, phone,
                          address_line1, address_line2, city, state,
                          postal_code, country, is_active, created_at, updated_at
                """,
                list(fields.values()),
            )
            customer = dict(cur.fetchone())
        return res.created(customer)
    except psycopg2.errors.UniqueViolation:
        return res.conflict(f"A customer with email '{body.get('email')}' already exists.")
    except Exception as exc:
        logger.exception("create_customer error")
        return res.internal_error(str(exc))


# ─────────────────────────────────────────────────────────────
# GET /customers/{id}
# ─────────────────────────────────────────────────────────────
def get_customer(event: dict) -> dict:
    try:
        customer_id = path_param(event, "id")
    except ValueError as exc:
        return res.bad_request(str(exc))

    try:
        with get_cursor() as cur:
            cur.execute(
                """
                SELECT id, email, first_name, last_name, phone,
                       address_line1, address_line2, city, state,
                       postal_code, country, is_active, created_at, updated_at
                FROM customers WHERE id = %s
                """,
                [customer_id],
            )
            row = cur.fetchone()

        if not row:
            return res.not_found("Customer")
        return res.ok(dict(row))
    except Exception as exc:
        logger.exception("get_customer error")
        return res.internal_error(str(exc))


# ─────────────────────────────────────────────────────────────
# PUT /customers/{id}
# ─────────────────────────────────────────────────────────────
def update_customer(event: dict) -> dict:
    try:
        customer_id = path_param(event, "id")
        body = parse_body(event)
    except ValueError as exc:
        return res.bad_request(str(exc))
    except Exception:
        return res.bad_request("Invalid JSON body.")

    updates = {k: v for k, v in body.items() if k in _ALLOWED_FIELDS}
    if not updates:
        return res.bad_request("No valid fields provided for update.")

    set_clause = ", ".join(f"{k} = %s" for k in updates)
    values = list(updates.values()) + [customer_id]

    try:
        with get_cursor(commit=True) as cur:
            cur.execute(
                f"""
                UPDATE customers SET {set_clause}
                WHERE id = %s
                RETURNING id, email, first_name, last_name, phone,
                          address_line1, address_line2, city, state,
                          postal_code, country, is_active, created_at, updated_at
                """,
                values,
            )
            row = cur.fetchone()

        if not row:
            return res.not_found("Customer")
        return res.ok(dict(row))
    except psycopg2.errors.UniqueViolation:
        return res.conflict("A customer with that email already exists.")
    except Exception as exc:
        logger.exception("update_customer error")
        return res.internal_error(str(exc))

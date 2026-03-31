#!/usr/bin/env python3
"""
seed_data.py — Generates realistic e-commerce data and loads it into RDS PostgreSQL.

Usage:
    python scripts/seed_data.py --host <RDS_HOST> --password <PASSWORD>
    python scripts/seed_data.py --host <RDS_HOST> --password <PASSWORD> --customers 50000

Requires: psycopg2-binary, faker, boto3, tqdm
"""

import argparse
import random
import uuid
from datetime import datetime, timedelta
from decimal import Decimal

import psycopg2
from psycopg2.extras import execute_batch
from faker import Faker
from tqdm import tqdm

fake = Faker()
random.seed(42)
Faker.seed(42)

# ── Constants ──────────────────────────────────────────────────────────────────

CATEGORIES = [
    "Electronics", "Clothing", "Home & Garden", "Sports", "Books",
    "Toys", "Beauty", "Automotive", "Food & Grocery", "Jewelry",
]

ORDER_STATUSES = ["pending", "processing", "shipped", "delivered", "cancelled", "refunded"]
EVENT_TYPES = ["page_view", "product_view", "add_to_cart", "remove_from_cart",
               "checkout_start", "purchase", "search", "wishlist_add"]

DISCOUNT_TYPES = ["percentage", "fixed_amount", "free_shipping"]

# ── Connection ─────────────────────────────────────────────────────────────────

def get_connection(host, port, dbname, user, password):
    return psycopg2.connect(
        host=host, port=port, dbname=dbname,
        user=user, password=password,
        connect_timeout=10,
    )


# ── Generators ─────────────────────────────────────────────────────────────────

def generate_customers(n: int) -> list[dict]:
    print(f"\n[1/6] Generating {n:,} customers…")
    customers = []
    for _ in tqdm(range(n)):
        created = fake.date_time_between(start_date="-5y", end_date="now")
        customers.append({
            "id": str(uuid.uuid4()),
            "email": fake.unique.email(),
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "phone": fake.phone_number()[:20],
            "date_of_birth": fake.date_of_birth(minimum_age=18, maximum_age=80),
            "gender": random.choice(["M", "F", "Other", None]),
            "country": fake.country_code(),
            "city": fake.city(),
            "postal_code": fake.postcode()[:10],
            "acquisition_channel": random.choice(["organic", "paid_search", "social", "email", "referral", "direct"]),
            "is_active": random.random() > 0.05,
            "created_at": created,
            "updated_at": created + timedelta(days=random.randint(0, 365)),
        })
    return customers


def generate_products(n: int) -> list[dict]:
    print(f"\n[2/6] Generating {n:,} products…")
    products = []
    for _ in tqdm(range(n)):
        base_price = round(random.uniform(2.99, 999.99), 2)
        cost = round(base_price * random.uniform(0.25, 0.65), 2)
        created = fake.date_time_between(start_date="-4y", end_date="-6m")
        products.append({
            "id": str(uuid.uuid4()),
            "sku": fake.unique.bothify(text="??-####-??").upper(),
            "name": fake.catch_phrase(),
            "description": fake.paragraph(nb_sentences=3),
            "category": random.choice(CATEGORIES),
            "brand": fake.company(),
            "base_price": base_price,
            "cost_price": cost,
            "weight_kg": round(random.uniform(0.05, 30.0), 3),
            "is_active": random.random() > 0.1,
            "created_at": created,
            "updated_at": created + timedelta(days=random.randint(0, 180)),
        })
    return products


def generate_inventory(product_ids: list[str]) -> list[dict]:
    print(f"\n[3/6] Generating inventory for {len(product_ids):,} products…")
    rows = []
    for pid in tqdm(product_ids):
        qty = random.randint(0, 500)
        rows.append({
            "product_id": pid,
            "warehouse_code": random.choice(["WH-US-EAST", "WH-US-WEST", "WH-EU-CENTRAL", "WH-APAC"]),
            "quantity_on_hand": qty,
            "quantity_reserved": random.randint(0, max(0, qty - 1)),
            "reorder_point": random.randint(5, 50),
            "reorder_quantity": random.randint(50, 500),
            "updated_at": datetime.utcnow(),
        })
    return rows


def generate_orders(customer_ids: list[str], product_ids: list[str],
                    n_orders: int) -> tuple[list[dict], list[dict]]:
    print(f"\n[4/6] Generating {n_orders:,} orders with line items…")
    orders, items = [], []
    for _ in tqdm(range(n_orders)):
        customer_id = random.choice(customer_ids)
        order_id = str(uuid.uuid4())
        created = fake.date_time_between(start_date="-3y", end_date="now")
        status = random.choices(ORDER_STATUSES, weights=[5, 10, 15, 60, 8, 2])[0]
        shipping = round(random.choice([0.0, 4.99, 7.99, 12.99, 19.99]), 2)
        tax_rate = round(random.uniform(0.05, 0.12), 4)

        n_items = random.randint(1, 6)
        subtotal = Decimal("0")
        for _ in range(n_items):
            pid = random.choice(product_ids)
            qty = random.randint(1, 4)
            unit_price = round(random.uniform(2.99, 499.99), 2)
            discount = round(unit_price * random.choice([0, 0, 0, 0.05, 0.10, 0.15, 0.20]), 2)
            line_total = round((unit_price - discount) * qty, 2)
            subtotal += Decimal(str(line_total))
            items.append({
                "id": str(uuid.uuid4()),
                "order_id": order_id,
                "product_id": pid,
                "quantity": qty,
                "unit_price": unit_price,
                "discount_amount": discount,
                "line_total": line_total,
            })

        tax_amount = round(float(subtotal) * tax_rate, 2)
        orders.append({
            "id": order_id,
            "customer_id": customer_id,
            "status": status,
            "subtotal": float(subtotal),
            "discount_total": 0.0,
            "shipping_amount": shipping,
            "tax_amount": tax_amount,
            "total_amount": float(subtotal) + shipping + tax_amount,
            "currency": "USD",
            "shipping_country": fake.country_code(),
            "shipping_city": fake.city(),
            "payment_method": random.choice(["credit_card", "debit_card", "paypal", "apple_pay", "bank_transfer"]),
            "created_at": created,
            "updated_at": created + timedelta(hours=random.randint(0, 72)),
        })
    return orders, items


def generate_events(customer_ids: list[str], product_ids: list[str], n: int) -> list[dict]:
    print(f"\n[5/6] Generating {n:,} behavioral events…")
    events = []
    for _ in tqdm(range(n)):
        ts = fake.date_time_between(start_date="-1y", end_date="now")
        events.append({
            "id": str(uuid.uuid4()),
            "customer_id": random.choice([random.choice(customer_ids), None]),
            "session_id": str(uuid.uuid4()),
            "event_type": random.choices(EVENT_TYPES, weights=[30, 25, 15, 5, 8, 5, 8, 4])[0],
            "product_id": random.choice([random.choice(product_ids), None]),
            "page_url": fake.uri_path(),
            "referrer_url": random.choice([fake.uri_path(), None, None]),
            "device_type": random.choice(["desktop", "mobile", "tablet"]),
            "browser": random.choice(["Chrome", "Safari", "Firefox", "Edge"]),
            "ip_address": fake.ipv4(),
            "country_code": fake.country_code(),
            "created_at": ts,
        })
    return events


def generate_discount_codes(n: int = 100) -> list[dict]:
    print(f"\n[6/6] Generating {n} discount codes…")
    codes = []
    for _ in range(n):
        dtype = random.choice(DISCOUNT_TYPES)
        value = random.choice([5, 10, 15, 20, 25, 30, 50]) if dtype == "percentage" \
            else round(random.uniform(2, 50), 2) if dtype == "fixed_amount" else 0
        start = fake.date_time_between(start_date="-2y", end_date="now")
        codes.append({
            "id": str(uuid.uuid4()),
            "code": fake.unique.bothify(text="????-####").upper(),
            "discount_type": dtype,
            "discount_value": value,
            "minimum_order_amount": random.choice([0, 25, 50, 100]),
            "usage_limit": random.choice([None, 50, 100, 500, 1000]),
            "usage_count": random.randint(0, 200),
            "is_active": random.random() > 0.3,
            "starts_at": start,
            "expires_at": start + timedelta(days=random.randint(30, 365)),
            "created_at": start,
        })
    return codes


# ── Loaders ────────────────────────────────────────────────────────────────────

def load_customers(conn, rows):
    sql = """
        INSERT INTO customers
            (id, email, first_name, last_name, phone, date_of_birth, gender,
             country, city, postal_code, acquisition_channel, is_active, created_at, updated_at)
        VALUES
            (%(id)s, %(email)s, %(first_name)s, %(last_name)s, %(phone)s,
             %(date_of_birth)s, %(gender)s, %(country)s, %(city)s, %(postal_code)s,
             %(acquisition_channel)s, %(is_active)s, %(created_at)s, %(updated_at)s)
        ON CONFLICT (email) DO NOTHING
    """
    with conn.cursor() as cur:
        execute_batch(cur, sql, rows, page_size=500)
    conn.commit()


def load_products(conn, rows):
    sql = """
        INSERT INTO products
            (id, sku, name, description, category, brand, base_price, cost_price,
             weight_kg, is_active, created_at, updated_at)
        VALUES
            (%(id)s, %(sku)s, %(name)s, %(description)s, %(category)s, %(brand)s,
             %(base_price)s, %(cost_price)s, %(weight_kg)s, %(is_active)s,
             %(created_at)s, %(updated_at)s)
        ON CONFLICT (sku) DO NOTHING
    """
    with conn.cursor() as cur:
        execute_batch(cur, sql, rows, page_size=500)
    conn.commit()


def load_inventory(conn, rows):
    sql = """
        INSERT INTO inventory
            (product_id, warehouse_code, quantity_on_hand, quantity_reserved,
             reorder_point, reorder_quantity, updated_at)
        VALUES
            (%(product_id)s, %(warehouse_code)s, %(quantity_on_hand)s,
             %(quantity_reserved)s, %(reorder_point)s, %(reorder_quantity)s, %(updated_at)s)
        ON CONFLICT (product_id, warehouse_code) DO NOTHING
    """
    with conn.cursor() as cur:
        execute_batch(cur, sql, rows, page_size=500)
    conn.commit()


def load_orders(conn, orders, items):
    order_sql = """
        INSERT INTO orders
            (id, customer_id, status, subtotal, discount_total, shipping_amount,
             tax_amount, total_amount, currency, shipping_country, shipping_city,
             payment_method, created_at, updated_at)
        VALUES
            (%(id)s, %(customer_id)s, %(status)s, %(subtotal)s, %(discount_total)s,
             %(shipping_amount)s, %(tax_amount)s, %(total_amount)s, %(currency)s,
             %(shipping_country)s, %(shipping_city)s, %(payment_method)s,
             %(created_at)s, %(updated_at)s)
        ON CONFLICT (id) DO NOTHING
    """
    item_sql = """
        INSERT INTO order_items
            (id, order_id, product_id, quantity, unit_price, discount_amount, line_total)
        VALUES
            (%(id)s, %(order_id)s, %(product_id)s, %(quantity)s, %(unit_price)s,
             %(discount_amount)s, %(line_total)s)
        ON CONFLICT (id) DO NOTHING
    """
    with conn.cursor() as cur:
        execute_batch(cur, order_sql, orders, page_size=500)
        execute_batch(cur, item_sql, items, page_size=1000)
    conn.commit()


def load_events(conn, rows):
    sql = """
        INSERT INTO events
            (id, customer_id, session_id, event_type, product_id, page_url,
             referrer_url, device_type, browser, ip_address, country_code, created_at)
        VALUES
            (%(id)s, %(customer_id)s, %(session_id)s, %(event_type)s, %(product_id)s,
             %(page_url)s, %(referrer_url)s, %(device_type)s, %(browser)s,
             %(ip_address)s, %(country_code)s, %(created_at)s)
        ON CONFLICT (id) DO NOTHING
    """
    with conn.cursor() as cur:
        execute_batch(cur, sql, rows, page_size=1000)
    conn.commit()


def load_discount_codes(conn, rows):
    sql = """
        INSERT INTO discount_codes
            (id, code, discount_type, discount_value, minimum_order_amount, usage_limit,
             usage_count, is_active, starts_at, expires_at, created_at)
        VALUES
            (%(id)s, %(code)s, %(discount_type)s, %(discount_value)s,
             %(minimum_order_amount)s, %(usage_limit)s, %(usage_count)s,
             %(is_active)s, %(starts_at)s, %(expires_at)s, %(created_at)s)
        ON CONFLICT (code) DO NOTHING
    """
    with conn.cursor() as cur:
        execute_batch(cur, sql, rows, page_size=200)
    conn.commit()


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Seed RDS with realistic e-commerce data")
    parser.add_argument("--host",      required=True,          help="RDS endpoint hostname")
    parser.add_argument("--port",      default=5432, type=int, help="PostgreSQL port")
    parser.add_argument("--dbname",    default="ecommerce",    help="Database name")
    parser.add_argument("--user",      default="dbadmin",      help="DB username")
    parser.add_argument("--password",  required=True,          help="DB password")
    parser.add_argument("--customers", default=10_000, type=int)
    parser.add_argument("--products",  default=5_000,  type=int)
    parser.add_argument("--orders",    default=100_000, type=int)
    parser.add_argument("--events",    default=500_000, type=int)
    args = parser.parse_args()

    print("=" * 60)
    print("  E-Commerce Data Seeder")
    print(f"  Target: {args.host}:{args.port}/{args.dbname}")
    print("=" * 60)

    conn = get_connection(args.host, args.port, args.dbname, args.user, args.password)
    print("  Connected to RDS successfully.")

    customers = generate_customers(args.customers)
    load_customers(conn, customers)
    customer_ids = [c["id"] for c in customers]

    products = generate_products(args.products)
    load_products(conn, products)
    product_ids = [p["id"] for p in products]

    inventory = generate_inventory(product_ids)
    load_inventory(conn, inventory)

    orders, items = generate_orders(customer_ids, product_ids, args.orders)
    load_orders(conn, orders, items)

    events = generate_events(customer_ids, product_ids, args.events)
    load_events(conn, events)

    discount_codes = generate_discount_codes(100)
    load_discount_codes(conn, discount_codes)

    conn.close()

    print("\n" + "=" * 60)
    print("  Seeding complete!")
    print(f"  Customers:     {args.customers:>10,}")
    print(f"  Products:      {args.products:>10,}")
    print(f"  Orders:        {args.orders:>10,}")
    print(f"  Order items:   {len(items):>10,}")
    print(f"  Events:        {args.events:>10,}")
    print(f"  Discount codes:{100:>10,}")
    print("=" * 60)


if __name__ == "__main__":
    main()

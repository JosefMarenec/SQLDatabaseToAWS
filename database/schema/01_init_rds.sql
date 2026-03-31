-- =============================================================================
-- Real-Time E-Commerce Analytics Platform
-- 01_init_rds.sql — Full OLTP Schema for RDS PostgreSQL 15
-- =============================================================================
-- Run order: 01 → 02 → 03 → 04 → 05
-- Dependencies: None (base schema)
-- =============================================================================

-- Enable required extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pg_stat_statements";
CREATE EXTENSION IF NOT EXISTS "btree_gin";
CREATE EXTENSION IF NOT EXISTS "pg_trgm";    -- fuzzy text search on product names

-- =============================================================================
-- ENUM TYPES
-- =============================================================================

CREATE TYPE order_status AS ENUM (
    'pending',
    'confirmed',
    'processing',
    'shipped',
    'delivered',
    'cancelled',
    'refunded',
    'partially_refunded'
);

CREATE TYPE payment_status AS ENUM (
    'pending',
    'authorized',
    'captured',
    'failed',
    'refunded',
    'partially_refunded',
    'disputed'
);

CREATE TYPE payment_method AS ENUM (
    'credit_card',
    'debit_card',
    'paypal',
    'apple_pay',
    'google_pay',
    'bank_transfer',
    'gift_card',
    'store_credit'
);

CREATE TYPE event_type AS ENUM (
    'page_view',
    'product_view',
    'add_to_cart',
    'remove_from_cart',
    'checkout_start',
    'checkout_step',
    'purchase',
    'search',
    'wishlist_add',
    'coupon_applied',
    'session_start',
    'session_end'
);

CREATE TYPE discount_type AS ENUM (
    'percentage',
    'fixed_amount',
    'free_shipping',
    'buy_x_get_y'
);

CREATE TYPE review_status AS ENUM (
    'pending_moderation',
    'published',
    'rejected',
    'flagged'
);

-- =============================================================================
-- UTILITY: updated_at TRIGGER FUNCTION
-- =============================================================================

CREATE OR REPLACE FUNCTION fn_set_updated_at()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$;

-- =============================================================================
-- TABLE: customers
-- =============================================================================

CREATE TABLE customers (
    customer_id         UUID            PRIMARY KEY DEFAULT uuid_generate_v4(),
    email               VARCHAR(320)    NOT NULL,
    email_verified      BOOLEAN         NOT NULL DEFAULT FALSE,
    first_name          VARCHAR(100)    NOT NULL,
    last_name           VARCHAR(100)    NOT NULL,
    phone               VARCHAR(30),
    date_of_birth       DATE,
    gender              CHAR(1)         CHECK (gender IN ('M', 'F', 'O', 'U')),

    -- Address (shipping default)
    address_line1       VARCHAR(255),
    address_line2       VARCHAR(255),
    city                VARCHAR(100),
    state_province      VARCHAR(100),
    postal_code         VARCHAR(20),
    country_code        CHAR(2)         NOT NULL DEFAULT 'US',

    -- Account details
    password_hash       VARCHAR(255),
    is_active           BOOLEAN         NOT NULL DEFAULT TRUE,
    is_guest            BOOLEAN         NOT NULL DEFAULT FALSE,
    marketing_opt_in    BOOLEAN         NOT NULL DEFAULT FALSE,

    -- Acquisition
    acquisition_source  VARCHAR(100),
    acquisition_medium  VARCHAR(100),
    acquisition_campaign VARCHAR(100),
    referrer_customer_id UUID           REFERENCES customers(customer_id),

    -- Loyalty
    loyalty_points      INTEGER         NOT NULL DEFAULT 0 CHECK (loyalty_points >= 0),
    loyalty_tier        VARCHAR(20)     NOT NULL DEFAULT 'bronze'
                            CHECK (loyalty_tier IN ('bronze', 'silver', 'gold', 'platinum')),

    -- Metadata
    created_at          TIMESTAMPTZ     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMPTZ     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_login_at       TIMESTAMPTZ,
    deleted_at          TIMESTAMPTZ,    -- soft delete

    CONSTRAINT customers_email_unique UNIQUE (email)
);

-- Indexes
CREATE INDEX idx_customers_email ON customers(email);
CREATE INDEX idx_customers_country ON customers(country_code);
CREATE INDEX idx_customers_loyalty_tier ON customers(loyalty_tier) WHERE is_active = TRUE;
CREATE INDEX idx_customers_created_at ON customers USING BRIN(created_at);
CREATE INDEX idx_customers_acquisition ON customers(acquisition_source, acquisition_medium);
CREATE INDEX idx_customers_active ON customers(is_active, created_at) WHERE deleted_at IS NULL;

-- Trigger
CREATE TRIGGER trg_customers_updated_at
    BEFORE UPDATE ON customers
    FOR EACH ROW EXECUTE FUNCTION fn_set_updated_at();

COMMENT ON TABLE customers IS 'Core customer/user accounts for the e-commerce platform';

-- =============================================================================
-- TABLE: product_categories
-- =============================================================================

CREATE TABLE product_categories (
    category_id     SERIAL          PRIMARY KEY,
    parent_id       INTEGER         REFERENCES product_categories(category_id),
    name            VARCHAR(150)    NOT NULL,
    slug            VARCHAR(150)    NOT NULL,
    description     TEXT,
    image_url       VARCHAR(500),
    is_active       BOOLEAN         NOT NULL DEFAULT TRUE,
    sort_order      INTEGER         NOT NULL DEFAULT 0,
    created_at      TIMESTAMPTZ     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at      TIMESTAMPTZ     NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT categories_slug_unique UNIQUE (slug)
);

CREATE INDEX idx_categories_parent ON product_categories(parent_id);
CREATE INDEX idx_categories_active ON product_categories(is_active);

CREATE TRIGGER trg_categories_updated_at
    BEFORE UPDATE ON product_categories
    FOR EACH ROW EXECUTE FUNCTION fn_set_updated_at();

-- =============================================================================
-- TABLE: products
-- =============================================================================

CREATE TABLE products (
    product_id          UUID            PRIMARY KEY DEFAULT uuid_generate_v4(),
    sku                 VARCHAR(100)    NOT NULL,
    name                VARCHAR(500)    NOT NULL,
    slug                VARCHAR(500)    NOT NULL,
    description         TEXT,
    short_description   VARCHAR(1000),
    category_id         INTEGER         NOT NULL REFERENCES product_categories(category_id),

    -- Pricing (stored in cents to avoid floating point)
    base_price_cents    INTEGER         NOT NULL CHECK (base_price_cents >= 0),
    sale_price_cents    INTEGER         CHECK (sale_price_cents >= 0),
    cost_price_cents    INTEGER         CHECK (cost_price_cents >= 0),

    -- Attributes
    brand               VARCHAR(200),
    weight_grams        INTEGER         CHECK (weight_grams > 0),
    dimensions_cm       JSONB,          -- {"l": 10, "w": 5, "h": 3}
    attributes          JSONB,          -- flexible product attributes

    -- Status
    is_active           BOOLEAN         NOT NULL DEFAULT TRUE,
    is_digital          BOOLEAN         NOT NULL DEFAULT FALSE,
    requires_shipping   BOOLEAN         NOT NULL DEFAULT TRUE,
    is_taxable          BOOLEAN         NOT NULL DEFAULT TRUE,
    tax_class           VARCHAR(50)     DEFAULT 'standard',

    -- SEO
    meta_title          VARCHAR(200),
    meta_description    VARCHAR(500),

    -- Ratings (denormalized for query performance)
    review_count        INTEGER         NOT NULL DEFAULT 0,
    avg_rating          NUMERIC(3,2)    CHECK (avg_rating BETWEEN 0 AND 5),

    -- Metadata
    created_at          TIMESTAMPTZ     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMPTZ     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    deleted_at          TIMESTAMPTZ,

    CONSTRAINT products_sku_unique UNIQUE (sku),
    CONSTRAINT products_slug_unique UNIQUE (slug)
);

-- Indexes
CREATE INDEX idx_products_category ON products(category_id) WHERE is_active = TRUE;
CREATE INDEX idx_products_brand ON products(brand) WHERE is_active = TRUE;
CREATE INDEX idx_products_price ON products(base_price_cents) WHERE is_active = TRUE;
CREATE INDEX idx_products_rating ON products(avg_rating DESC NULLS LAST) WHERE is_active = TRUE;
CREATE INDEX idx_products_created_at ON products USING BRIN(created_at);
-- Full-text search on product names
CREATE INDEX idx_products_name_trgm ON products USING GIN (name gin_trgm_ops);
-- JSON attribute search
CREATE INDEX idx_products_attributes ON products USING GIN (attributes);

CREATE TRIGGER trg_products_updated_at
    BEFORE UPDATE ON products
    FOR EACH ROW EXECUTE FUNCTION fn_set_updated_at();

COMMENT ON TABLE products IS 'Product catalog. Prices stored in cents (integer) to avoid floating-point rounding errors.';
COMMENT ON COLUMN products.dimensions_cm IS 'JSON: {"l": float, "w": float, "h": float}';

-- =============================================================================
-- TABLE: product_pricing_history
-- (Tracks price changes over time — important for analytics)
-- =============================================================================

CREATE TABLE product_pricing_history (
    history_id          BIGSERIAL       PRIMARY KEY,
    product_id          UUID            NOT NULL REFERENCES products(product_id),
    old_price_cents     INTEGER,
    new_price_cents     INTEGER         NOT NULL,
    change_reason       VARCHAR(200),
    changed_by          VARCHAR(100),
    effective_from      TIMESTAMPTZ     NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_pricing_history_product ON product_pricing_history(product_id, effective_from DESC);

-- =============================================================================
-- TABLE: inventory
-- =============================================================================

CREATE TABLE inventory (
    inventory_id            BIGSERIAL       PRIMARY KEY,
    product_id              UUID            NOT NULL REFERENCES products(product_id),
    warehouse_id            VARCHAR(50)     NOT NULL DEFAULT 'WH-001',
    quantity_on_hand        INTEGER         NOT NULL DEFAULT 0 CHECK (quantity_on_hand >= 0),
    quantity_reserved       INTEGER         NOT NULL DEFAULT 0 CHECK (quantity_reserved >= 0),
    quantity_available      INTEGER         GENERATED ALWAYS AS
                                (quantity_on_hand - quantity_reserved) STORED,
    reorder_point           INTEGER         NOT NULL DEFAULT 10,
    reorder_quantity        INTEGER         NOT NULL DEFAULT 100,
    unit_cost_cents         INTEGER,
    updated_at              TIMESTAMPTZ     NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT inventory_product_warehouse_unique UNIQUE (product_id, warehouse_id)
);

CREATE INDEX idx_inventory_product ON inventory(product_id);
CREATE INDEX idx_inventory_low_stock ON inventory(quantity_available)
    WHERE quantity_available <= reorder_point;

CREATE TRIGGER trg_inventory_updated_at
    BEFORE UPDATE ON inventory
    FOR EACH ROW EXECUTE FUNCTION fn_set_updated_at();

-- =============================================================================
-- TABLE: discount_codes
-- =============================================================================

CREATE TABLE discount_codes (
    discount_id         BIGSERIAL       PRIMARY KEY,
    code                VARCHAR(50)     NOT NULL,
    description         VARCHAR(500),
    discount_type       discount_type   NOT NULL,
    discount_value      NUMERIC(10,2)   NOT NULL CHECK (discount_value > 0),
    minimum_order_cents INTEGER         DEFAULT 0,
    maximum_uses        INTEGER,
    uses_per_customer   INTEGER         DEFAULT 1,
    current_uses        INTEGER         NOT NULL DEFAULT 0,
    is_active           BOOLEAN         NOT NULL DEFAULT TRUE,
    valid_from          TIMESTAMPTZ     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    valid_until         TIMESTAMPTZ,
    applicable_category_ids INTEGER[],  -- NULL = all categories
    created_at          TIMESTAMPTZ     NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT discount_codes_code_unique UNIQUE (code)
);

CREATE INDEX idx_discount_codes_active ON discount_codes(code)
    WHERE is_active = TRUE AND (valid_until IS NULL OR valid_until > CURRENT_TIMESTAMP);

-- =============================================================================
-- TABLE: orders
-- (Partitioned by created_at month — see 02_partitioning.sql)
-- =============================================================================

CREATE TABLE orders (
    order_id            UUID            DEFAULT uuid_generate_v4(),
    order_number        VARCHAR(50)     NOT NULL,   -- human-readable: ORD-2024-00001
    customer_id         UUID            NOT NULL REFERENCES customers(customer_id),
    status              order_status    NOT NULL DEFAULT 'pending',
    payment_status      payment_status  NOT NULL DEFAULT 'pending',
    payment_method      payment_method,

    -- Financials (all in cents)
    subtotal_cents      INTEGER         NOT NULL CHECK (subtotal_cents >= 0),
    discount_cents      INTEGER         NOT NULL DEFAULT 0,
    shipping_cents      INTEGER         NOT NULL DEFAULT 0,
    tax_cents           INTEGER         NOT NULL DEFAULT 0,
    total_cents         INTEGER         NOT NULL CHECK (total_cents >= 0),
    refunded_cents      INTEGER         NOT NULL DEFAULT 0,

    -- Applied discount
    discount_id         BIGINT          REFERENCES discount_codes(discount_id),
    discount_code       VARCHAR(50),

    -- Shipping
    shipping_name       VARCHAR(200),
    shipping_address1   VARCHAR(255),
    shipping_address2   VARCHAR(255),
    shipping_city       VARCHAR(100),
    shipping_state      VARCHAR(100),
    shipping_postal     VARCHAR(20),
    shipping_country    CHAR(2)         NOT NULL DEFAULT 'US',
    shipping_method     VARCHAR(100),
    tracking_number     VARCHAR(200),

    -- Session tracking
    session_id          UUID,
    ip_address          INET,
    user_agent          TEXT,

    -- Metadata
    notes               TEXT,
    created_at          TIMESTAMPTZ     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMPTZ     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    confirmed_at        TIMESTAMPTZ,
    shipped_at          TIMESTAMPTZ,
    delivered_at        TIMESTAMPTZ,
    cancelled_at        TIMESTAMPTZ,

    PRIMARY KEY (order_id, created_at)
) PARTITION BY RANGE (created_at);

-- Indexes (will apply to all partitions)
CREATE INDEX idx_orders_customer ON orders(customer_id, created_at DESC);
CREATE INDEX idx_orders_status ON orders(status) WHERE status NOT IN ('delivered', 'cancelled');
CREATE INDEX idx_orders_number ON orders(order_number);
CREATE INDEX idx_orders_created_at ON orders USING BRIN(created_at);
CREATE INDEX idx_orders_payment_status ON orders(payment_status)
    WHERE payment_status NOT IN ('captured', 'refunded');

CREATE TRIGGER trg_orders_updated_at
    BEFORE UPDATE ON orders
    FOR EACH ROW EXECUTE FUNCTION fn_set_updated_at();

-- =============================================================================
-- TABLE: order_items
-- =============================================================================

CREATE TABLE order_items (
    item_id             BIGSERIAL       PRIMARY KEY,
    order_id            UUID            NOT NULL,
    order_created_at    TIMESTAMPTZ     NOT NULL,   -- needed for FK to partitioned orders
    product_id          UUID            NOT NULL REFERENCES products(product_id),
    quantity            INTEGER         NOT NULL CHECK (quantity > 0),
    unit_price_cents    INTEGER         NOT NULL CHECK (unit_price_cents >= 0),
    discount_cents      INTEGER         NOT NULL DEFAULT 0,
    tax_cents           INTEGER         NOT NULL DEFAULT 0,
    total_cents         INTEGER         NOT NULL CHECK (total_cents >= 0),
    -- Snapshot of product name/sku at time of order (products can change)
    product_name        VARCHAR(500)    NOT NULL,
    product_sku         VARCHAR(100)    NOT NULL,

    CONSTRAINT fk_order_items_order
        FOREIGN KEY (order_id, order_created_at)
        REFERENCES orders(order_id, created_at)
);

CREATE INDEX idx_order_items_order ON order_items(order_id);
CREATE INDEX idx_order_items_product ON order_items(product_id);

-- =============================================================================
-- TABLE: sessions
-- =============================================================================

CREATE TABLE sessions (
    session_id          UUID            PRIMARY KEY DEFAULT uuid_generate_v4(),
    customer_id         UUID            REFERENCES customers(customer_id),  -- NULL for guests
    anonymous_id        UUID,           -- browser-level anonymous tracking
    ip_address          INET,
    user_agent          TEXT,
    device_type         VARCHAR(20)     CHECK (device_type IN ('desktop', 'mobile', 'tablet', 'unknown')),
    browser             VARCHAR(50),
    os                  VARCHAR(50),
    referrer            TEXT,
    utm_source          VARCHAR(100),
    utm_medium          VARCHAR(100),
    utm_campaign        VARCHAR(100),
    utm_content         VARCHAR(100),
    utm_term            VARCHAR(100),
    country_code        CHAR(2),
    region              VARCHAR(100),
    city                VARCHAR(100),
    started_at          TIMESTAMPTZ     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    ended_at            TIMESTAMPTZ,
    page_views          INTEGER         NOT NULL DEFAULT 0,
    duration_seconds    INTEGER         GENERATED ALWAYS AS
                            (EXTRACT(EPOCH FROM (ended_at - started_at))::INTEGER) STORED
);

CREATE INDEX idx_sessions_customer ON sessions(customer_id) WHERE customer_id IS NOT NULL;
CREATE INDEX idx_sessions_anonymous ON sessions(anonymous_id);
CREATE INDEX idx_sessions_started_at ON sessions USING BRIN(started_at);
CREATE INDEX idx_sessions_utm ON sessions(utm_source, utm_medium, utm_campaign);

-- =============================================================================
-- TABLE: events
-- (Partitioned by occurred_at day — see 02_partitioning.sql)
-- =============================================================================

CREATE TABLE events (
    event_id            UUID            DEFAULT uuid_generate_v4(),
    session_id          UUID            REFERENCES sessions(session_id),
    customer_id         UUID            REFERENCES customers(customer_id),
    anonymous_id        UUID,
    event_type          event_type      NOT NULL,
    occurred_at         TIMESTAMPTZ     NOT NULL DEFAULT CURRENT_TIMESTAMP,

    -- Page context
    page_url            TEXT,
    page_title          VARCHAR(500),
    referrer_url        TEXT,

    -- Event-specific payload
    product_id          UUID            REFERENCES products(product_id),
    order_id            UUID,
    search_query        VARCHAR(500),
    properties          JSONB,          -- flexible additional event data

    PRIMARY KEY (event_id, occurred_at)
) PARTITION BY RANGE (occurred_at);

CREATE INDEX idx_events_customer ON events(customer_id, occurred_at DESC) WHERE customer_id IS NOT NULL;
CREATE INDEX idx_events_session ON events(session_id, occurred_at);
CREATE INDEX idx_events_type ON events(event_type, occurred_at DESC);
CREATE INDEX idx_events_product ON events(product_id) WHERE product_id IS NOT NULL;
CREATE INDEX idx_events_occurred_at ON events USING BRIN(occurred_at);

-- =============================================================================
-- TABLE: reviews
-- =============================================================================

CREATE TABLE reviews (
    review_id           BIGSERIAL       PRIMARY KEY,
    product_id          UUID            NOT NULL REFERENCES products(product_id),
    customer_id         UUID            NOT NULL REFERENCES customers(customer_id),
    order_id            UUID,
    rating              SMALLINT        NOT NULL CHECK (rating BETWEEN 1 AND 5),
    title               VARCHAR(300),
    body                TEXT,
    status              review_status   NOT NULL DEFAULT 'pending_moderation',
    helpful_votes       INTEGER         NOT NULL DEFAULT 0,
    total_votes         INTEGER         NOT NULL DEFAULT 0,
    verified_purchase   BOOLEAN         NOT NULL DEFAULT FALSE,
    created_at          TIMESTAMPTZ     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMPTZ     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    published_at        TIMESTAMPTZ,

    -- One review per customer per product (allow updating)
    CONSTRAINT reviews_product_customer_unique UNIQUE (product_id, customer_id)
);

CREATE INDEX idx_reviews_product ON reviews(product_id, rating) WHERE status = 'published';
CREATE INDEX idx_reviews_customer ON reviews(customer_id);
CREATE INDEX idx_reviews_status ON reviews(status) WHERE status = 'pending_moderation';

CREATE TRIGGER trg_reviews_updated_at
    BEFORE UPDATE ON reviews
    FOR EACH ROW EXECUTE FUNCTION fn_set_updated_at();

-- Auto-update product rating denormalization when reviews change
CREATE OR REPLACE FUNCTION fn_update_product_rating()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    v_product_id UUID;
BEGIN
    v_product_id := COALESCE(NEW.product_id, OLD.product_id);

    UPDATE products
    SET
        review_count = subq.cnt,
        avg_rating   = subq.avg_r
    FROM (
        SELECT
            COUNT(*)::INTEGER                   AS cnt,
            ROUND(AVG(rating)::NUMERIC, 2)      AS avg_r
        FROM reviews
        WHERE product_id = v_product_id
          AND status = 'published'
    ) subq
    WHERE product_id = v_product_id;

    RETURN COALESCE(NEW, OLD);
END;
$$;

CREATE TRIGGER trg_reviews_update_product_rating
    AFTER INSERT OR UPDATE OR DELETE ON reviews
    FOR EACH ROW EXECUTE FUNCTION fn_update_product_rating();

-- =============================================================================
-- SEQUENCES for human-readable order numbers
-- =============================================================================

CREATE SEQUENCE order_number_seq START 1000 INCREMENT 1;

CREATE OR REPLACE FUNCTION fn_generate_order_number()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    NEW.order_number := 'ORD-' ||
        TO_CHAR(NEW.created_at, 'YYYY') || '-' ||
        LPAD(nextval('order_number_seq')::TEXT, 8, '0');
    RETURN NEW;
END;
$$;

CREATE TRIGGER trg_orders_generate_number
    BEFORE INSERT ON orders
    FOR EACH ROW
    WHEN (NEW.order_number IS NULL OR NEW.order_number = '')
    EXECUTE FUNCTION fn_generate_order_number();

-- =============================================================================
-- GRANT STATEMENTS (application role)
-- =============================================================================

-- Create application roles (run as superuser)
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'app_readwrite') THEN
        CREATE ROLE app_readwrite;
    END IF;
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'analytics_read') THEN
        CREATE ROLE analytics_read;
    END IF;
END
$$;

GRANT SELECT, INSERT, UPDATE ON customers, products, product_categories,
    product_pricing_history, inventory, discount_codes, order_items,
    sessions, reviews TO app_readwrite;
GRANT ALL ON orders, events TO app_readwrite;
GRANT USAGE ON ALL SEQUENCES IN SCHEMA public TO app_readwrite;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO analytics_read;

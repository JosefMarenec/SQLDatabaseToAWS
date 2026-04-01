-- =============================================================
-- V2: Performance indexes
-- =============================================================

-- Products
CREATE INDEX IF NOT EXISTS idx_products_category      ON products (category)   WHERE is_active = TRUE;
CREATE INDEX IF NOT EXISTS idx_products_sku            ON products (sku);
CREATE INDEX IF NOT EXISTS idx_products_price          ON products (price)      WHERE is_active = TRUE;
CREATE INDEX IF NOT EXISTS idx_products_created_at     ON products (created_at DESC);

-- Customers
CREATE INDEX IF NOT EXISTS idx_customers_email         ON customers (email);
CREATE INDEX IF NOT EXISTS idx_customers_country       ON customers (country)   WHERE is_active = TRUE;
CREATE INDEX IF NOT EXISTS idx_customers_created_at    ON customers (created_at DESC);

-- Orders
CREATE INDEX IF NOT EXISTS idx_orders_customer_id      ON orders (customer_id);
CREATE INDEX IF NOT EXISTS idx_orders_status           ON orders (status);
CREATE INDEX IF NOT EXISTS idx_orders_created_at       ON orders (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_orders_customer_status  ON orders (customer_id, status);

-- Order Items
CREATE INDEX IF NOT EXISTS idx_order_items_order_id    ON order_items (order_id);
CREATE INDEX IF NOT EXISTS idx_order_items_product_id  ON order_items (product_id);

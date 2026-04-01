-- =============================================================
-- V3: Reporting views
-- =============================================================

-- Full order detail (order + customer + line items + products)
CREATE OR REPLACE VIEW v_order_details AS
SELECT
    o.id                AS order_id,
    o.status,
    o.subtotal,
    o.tax,
    o.shipping,
    o.total,
    o.created_at        AS order_date,
    o.shipped_at,
    o.delivered_at,
    c.id                AS customer_id,
    c.email,
    c.first_name || ' ' || c.last_name  AS customer_name,
    c.country,
    oi.id               AS item_id,
    oi.quantity,
    oi.unit_price,
    oi.line_total,
    p.id                AS product_id,
    p.sku,
    p.name              AS product_name,
    p.category
FROM orders o
JOIN customers c  ON c.id  = o.customer_id
JOIN order_items oi ON oi.order_id = o.id
JOIN products p   ON p.id  = oi.product_id;

-- Revenue summary by category (last 90 days)
CREATE OR REPLACE VIEW v_revenue_by_category AS
SELECT
    p.category,
    COUNT(DISTINCT o.id)        AS order_count,
    SUM(oi.quantity)            AS units_sold,
    SUM(oi.line_total)          AS revenue,
    AVG(oi.unit_price)          AS avg_unit_price
FROM order_items oi
JOIN orders o   ON o.id  = oi.order_id
JOIN products p ON p.id  = oi.product_id
WHERE o.status NOT IN ('cancelled', 'refunded')
  AND o.created_at >= NOW() - INTERVAL '90 days'
GROUP BY p.category;

-- Customer lifetime value
CREATE OR REPLACE VIEW v_customer_ltv AS
SELECT
    c.id            AS customer_id,
    c.email,
    c.first_name || ' ' || c.last_name AS customer_name,
    COUNT(DISTINCT o.id)    AS total_orders,
    COALESCE(SUM(o.total), 0)   AS lifetime_value,
    MIN(o.created_at)       AS first_order_at,
    MAX(o.created_at)       AS last_order_at
FROM customers c
LEFT JOIN orders o ON o.customer_id = c.id
    AND o.status NOT IN ('cancelled', 'refunded')
WHERE c.is_active = TRUE
GROUP BY c.id, c.email, c.first_name, c.last_name;

-- Low stock alert view
CREATE OR REPLACE VIEW v_low_stock AS
SELECT
    id,
    sku,
    name,
    category,
    stock_qty,
    price
FROM products
WHERE is_active = TRUE
  AND stock_qty <= 10
ORDER BY stock_qty ASC;

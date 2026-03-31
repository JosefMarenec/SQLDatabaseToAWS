-- =============================================================================
-- Real-Time E-Commerce Analytics Platform
-- 04_views_and_materialized.sql — Views and Materialized Views
-- =============================================================================
-- Run after: 01_init_rds.sql, 02_partitioning.sql, 03_stored_procedures.sql
-- All materialized views use CONCURRENTLY refresh (require unique index).
-- =============================================================================

-- =============================================================================
-- VIEW: v_customer_360
-- Complete customer profile joining all relevant tables.
-- Used by customer service, personalization engine, and CRM sync.
-- =============================================================================

CREATE OR REPLACE VIEW v_customer_360 AS
WITH order_summary AS (
    SELECT
        customer_id,
        COUNT(*)                                            AS total_orders,
        SUM(total_cents)                                    AS lifetime_revenue_cents,
        AVG(total_cents)                                    AS avg_order_value_cents,
        MIN(created_at)                                     AS first_order_at,
        MAX(created_at)                                     AS last_order_at,
        SUM(CASE WHEN status = 'cancelled' THEN 1 ELSE 0 END) AS cancelled_orders,
        SUM(CASE WHEN status = 'refunded' THEN 1 ELSE 0 END) AS refunded_orders,
        COUNT(DISTINCT DATE_TRUNC('month', created_at))     AS active_months
    FROM orders
    WHERE payment_status IN ('captured', 'partially_refunded', 'refunded')
    GROUP BY customer_id
),
event_summary AS (
    SELECT
        customer_id,
        COUNT(*) FILTER (WHERE event_type = 'page_view')    AS total_page_views,
        COUNT(*) FILTER (WHERE event_type = 'product_view') AS total_product_views,
        COUNT(*) FILTER (WHERE event_type = 'add_to_cart')  AS total_cart_adds,
        COUNT(*) FILTER (WHERE event_type = 'search')       AS total_searches,
        MAX(occurred_at)                                    AS last_event_at
    FROM events
    WHERE customer_id IS NOT NULL
      AND occurred_at > CURRENT_TIMESTAMP - INTERVAL '90 days'
    GROUP BY customer_id
),
review_summary AS (
    SELECT
        customer_id,
        COUNT(*)            AS total_reviews,
        AVG(rating)::NUMERIC(3,2) AS avg_rating_given
    FROM reviews
    WHERE status = 'published'
    GROUP BY customer_id
)
SELECT
    c.customer_id,
    c.email,
    c.first_name,
    c.last_name,
    c.first_name || ' ' || c.last_name AS full_name,
    c.phone,
    c.country_code,
    c.city,
    c.state_province,
    c.loyalty_tier,
    c.loyalty_points,
    c.acquisition_source,
    c.acquisition_medium,
    c.marketing_opt_in,
    c.is_active,
    c.created_at AS customer_since,
    c.last_login_at,

    -- Order metrics
    COALESCE(os.total_orders, 0)                AS total_orders,
    COALESCE(os.lifetime_revenue_cents, 0)      AS lifetime_revenue_cents,
    ROUND(COALESCE(os.avg_order_value_cents, 0)) AS avg_order_value_cents,
    os.first_order_at,
    os.last_order_at,
    COALESCE(os.cancelled_orders, 0)            AS cancelled_orders,
    COALESCE(os.refunded_orders, 0)             AS refunded_orders,

    -- Recency (days since last order)
    CASE WHEN os.last_order_at IS NOT NULL
         THEN EXTRACT(DAY FROM CURRENT_TIMESTAMP - os.last_order_at)::INT
    END AS days_since_last_order,

    -- Engagement metrics (last 90 days)
    COALESCE(ev.total_page_views, 0)            AS page_views_90d,
    COALESCE(ev.total_product_views, 0)         AS product_views_90d,
    COALESCE(ev.total_cart_adds, 0)             AS cart_adds_90d,
    COALESCE(ev.total_searches, 0)              AS searches_90d,
    ev.last_event_at,

    -- Review metrics
    COALESCE(rv.total_reviews, 0)               AS total_reviews,
    rv.avg_rating_given,

    -- Computed segments
    CASE
        WHEN os.last_order_at > CURRENT_TIMESTAMP - INTERVAL '30 days'  THEN 'active'
        WHEN os.last_order_at > CURRENT_TIMESTAMP - INTERVAL '90 days'  THEN 'at_risk'
        WHEN os.last_order_at > CURRENT_TIMESTAMP - INTERVAL '180 days' THEN 'lapsing'
        WHEN os.last_order_at IS NOT NULL                                THEN 'churned'
        ELSE 'never_purchased'
    END AS engagement_segment,

    CASE
        WHEN os.lifetime_revenue_cents >= 100000 THEN 'high_value'    -- >$1,000
        WHEN os.lifetime_revenue_cents >= 50000  THEN 'mid_value'     -- >$500
        WHEN os.lifetime_revenue_cents > 0       THEN 'low_value'
        ELSE 'no_purchases'
    END AS value_segment

FROM customers c
LEFT JOIN order_summary os  ON os.customer_id = c.customer_id
LEFT JOIN event_summary ev  ON ev.customer_id = c.customer_id
LEFT JOIN review_summary rv ON rv.customer_id = c.customer_id
WHERE c.deleted_at IS NULL;

COMMENT ON VIEW v_customer_360 IS
    'Full customer profile view. Not materialized (real-time). '
    'Consider mv_customer_cohorts for aggregated cohort analysis.';

-- =============================================================================
-- VIEW: v_inventory_status
-- Real-time inventory with low-stock and out-of-stock alerts.
-- =============================================================================

CREATE OR REPLACE VIEW v_inventory_status AS
SELECT
    p.product_id,
    p.sku,
    p.name                                              AS product_name,
    pc.name                                             AS category_name,
    p.base_price_cents,
    i.warehouse_id,
    i.quantity_on_hand,
    i.quantity_reserved,
    i.quantity_available,
    i.reorder_point,
    i.reorder_quantity,
    -- Status
    CASE
        WHEN i.quantity_available <= 0              THEN 'out_of_stock'
        WHEN i.quantity_available <= i.reorder_point THEN 'low_stock'
        ELSE                                             'in_stock'
    END AS stock_status,
    -- How many days of stock remain based on last 30 days sales velocity
    CASE WHEN COALESCE(sales_velocity.avg_daily_units, 0) > 0
         THEN ROUND(i.quantity_available / sales_velocity.avg_daily_units)::INT
    END AS days_of_stock,
    i.updated_at AS inventory_updated_at
FROM inventory i
JOIN products p ON p.product_id = i.product_id
JOIN product_categories pc ON pc.category_id = p.category_id
LEFT JOIN LATERAL (
    SELECT ROUND(SUM(oi.quantity) / 30.0, 2) AS avg_daily_units
    FROM order_items oi
    JOIN orders o ON o.order_id = oi.order_id AND o.order_created_at = oi.order_created_at
    WHERE oi.product_id = i.product_id
      AND o.created_at > CURRENT_TIMESTAMP - INTERVAL '30 days'
      AND o.status NOT IN ('cancelled')
) sales_velocity ON TRUE
WHERE p.is_active = TRUE
  AND p.deleted_at IS NULL
ORDER BY
    CASE WHEN i.quantity_available <= 0 THEN 0
         WHEN i.quantity_available <= i.reorder_point THEN 1
         ELSE 2
    END,
    p.name;

-- =============================================================================
-- MATERIALIZED VIEW: mv_daily_sales_summary
-- Daily revenue, order counts, and AOV — refreshed daily at 2am UTC.
-- =============================================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_daily_sales_summary AS
SELECT
    DATE_TRUNC('day', o.created_at)::DATE           AS sale_date,
    COUNT(DISTINCT o.order_id)                       AS order_count,
    COUNT(DISTINCT o.customer_id)                    AS unique_customers,
    SUM(o.total_cents)                               AS gross_revenue_cents,
    SUM(o.discount_cents)                            AS total_discounts_cents,
    SUM(o.shipping_cents)                            AS total_shipping_cents,
    SUM(o.tax_cents)                                 AS total_tax_cents,
    SUM(o.total_cents - o.shipping_cents - o.tax_cents) AS net_revenue_cents,
    ROUND(AVG(o.total_cents))::BIGINT                AS avg_order_value_cents,
    COUNT(*) FILTER (WHERE o.status = 'cancelled')   AS cancelled_orders,
    COUNT(*) FILTER (WHERE o.status = 'refunded')    AS refunded_orders,
    -- New vs returning customers
    COUNT(DISTINCT o.customer_id) FILTER (
        WHERE NOT EXISTS (
            SELECT 1 FROM orders prev
            WHERE prev.customer_id = o.customer_id
              AND prev.created_at < DATE_TRUNC('day', o.created_at)
              AND prev.status NOT IN ('cancelled')
        )
    )                                                AS new_customer_count,
    -- Payment method breakdown
    COUNT(*) FILTER (WHERE o.payment_method = 'credit_card')  AS credit_card_orders,
    COUNT(*) FILTER (WHERE o.payment_method = 'paypal')       AS paypal_orders
FROM orders o
WHERE o.payment_status IN ('captured', 'partially_refunded', 'refunded')
GROUP BY DATE_TRUNC('day', o.created_at)::DATE
WITH DATA;

-- Unique index required for CONCURRENT refresh
CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_daily_sales_date
    ON mv_daily_sales_summary (sale_date);

COMMENT ON MATERIALIZED VIEW mv_daily_sales_summary IS
    'Daily sales aggregation. Refresh with: REFRESH MATERIALIZED VIEW CONCURRENTLY mv_daily_sales_summary';

-- =============================================================================
-- MATERIALIZED VIEW: mv_product_performance
-- Per-product revenue, units sold, return rate, rating.
-- =============================================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_product_performance AS
WITH sales AS (
    SELECT
        oi.product_id,
        SUM(oi.quantity)                                    AS units_sold,
        COUNT(DISTINCT oi.order_id)                         AS order_count,
        SUM(oi.total_cents)                                 AS revenue_cents,
        COUNT(DISTINCT o.customer_id)                       AS unique_customers,
        MIN(o.created_at::DATE)                             AS first_sale_date,
        MAX(o.created_at::DATE)                             AS last_sale_date
    FROM order_items oi
    JOIN orders o ON o.order_id = oi.order_id AND o.order_created_at = oi.order_created_at
    WHERE o.status NOT IN ('cancelled')
      AND o.payment_status IN ('captured', 'partially_refunded', 'refunded')
    GROUP BY oi.product_id
),
returns AS (
    SELECT
        oi.product_id,
        COUNT(DISTINCT oi.order_id) AS returned_orders
    FROM order_items oi
    JOIN orders o ON o.order_id = oi.order_id AND o.order_created_at = oi.order_created_at
    WHERE o.status IN ('refunded', 'partially_refunded')
    GROUP BY oi.product_id
),
cart_events AS (
    SELECT
        product_id,
        COUNT(*) FILTER (WHERE event_type = 'product_view')  AS view_count,
        COUNT(*) FILTER (WHERE event_type = 'add_to_cart')   AS cart_add_count
    FROM events
    WHERE product_id IS NOT NULL
      AND occurred_at > CURRENT_TIMESTAMP - INTERVAL '90 days'
    GROUP BY product_id
)
SELECT
    p.product_id,
    p.sku,
    p.name                                              AS product_name,
    pc.name                                             AS category_name,
    p.base_price_cents,
    p.avg_rating,
    p.review_count,
    COALESCE(s.units_sold, 0)                           AS units_sold,
    COALESCE(s.order_count, 0)                          AS order_count,
    COALESCE(s.revenue_cents, 0)                        AS revenue_cents,
    COALESCE(s.unique_customers, 0)                     AS unique_customers,
    s.first_sale_date,
    s.last_sale_date,
    -- Return rate
    CASE WHEN s.order_count > 0
         THEN ROUND(COALESCE(r.returned_orders, 0)::NUMERIC / s.order_count * 100, 2)
    END                                                  AS return_rate_pct,
    -- Conversion rate (add to cart → purchase)
    CASE WHEN COALESCE(ce.cart_add_count, 0) > 0
         THEN ROUND(COALESCE(s.order_count, 0)::NUMERIC / ce.cart_add_count * 100, 2)
    END                                                  AS cart_conversion_rate_pct,
    COALESCE(ce.view_count, 0)                           AS views_90d,
    COALESCE(ce.cart_add_count, 0)                       AS cart_adds_90d,
    CURRENT_TIMESTAMP                                    AS refreshed_at
FROM products p
JOIN product_categories pc ON pc.category_id = p.category_id
LEFT JOIN sales s ON s.product_id = p.product_id
LEFT JOIN returns r ON r.product_id = p.product_id
LEFT JOIN cart_events ce ON ce.product_id = p.product_id
WHERE p.deleted_at IS NULL
WITH DATA;

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_product_perf_id
    ON mv_product_performance (product_id);
CREATE INDEX IF NOT EXISTS idx_mv_product_perf_revenue
    ON mv_product_performance (revenue_cents DESC);
CREATE INDEX IF NOT EXISTS idx_mv_product_perf_category
    ON mv_product_performance (category_name);

-- =============================================================================
-- MATERIALIZED VIEW: mv_customer_cohorts
-- Monthly cohort analysis — retention by cohort month.
-- =============================================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_customer_cohorts AS
WITH cohorts AS (
    -- Assign each customer to their acquisition cohort (first purchase month)
    SELECT
        c.customer_id,
        DATE_TRUNC('month', MIN(o.created_at))::DATE AS cohort_month
    FROM customers c
    JOIN orders o ON o.customer_id = c.customer_id
    WHERE o.status NOT IN ('cancelled')
      AND o.payment_status IN ('captured', 'partially_refunded')
    GROUP BY c.customer_id
),
cohort_orders AS (
    SELECT
        co.customer_id,
        co.cohort_month,
        DATE_TRUNC('month', o.created_at)::DATE AS order_month,
        -- Month number since cohort start (0 = cohort month)
        (
            EXTRACT(YEAR FROM DATE_TRUNC('month', o.created_at)) * 12
            + EXTRACT(MONTH FROM DATE_TRUNC('month', o.created_at))
        ) - (
            EXTRACT(YEAR FROM co.cohort_month) * 12
            + EXTRACT(MONTH FROM co.cohort_month)
        )                                        AS period_number
    FROM cohorts co
    JOIN orders o ON o.customer_id = co.customer_id
    WHERE o.status NOT IN ('cancelled')
      AND o.payment_status IN ('captured', 'partially_refunded')
)
SELECT
    cohort_month,
    period_number,
    COUNT(DISTINCT customer_id)             AS active_customers,
    -- Cohort size at period 0
    FIRST_VALUE(COUNT(DISTINCT customer_id)) OVER (
        PARTITION BY cohort_month
        ORDER BY period_number
    )                                       AS cohort_size,
    ROUND(
        COUNT(DISTINCT customer_id)::NUMERIC
        / FIRST_VALUE(COUNT(DISTINCT customer_id)) OVER (
            PARTITION BY cohort_month ORDER BY period_number
        ) * 100,
        2
    )                                       AS retention_rate_pct
FROM cohort_orders
GROUP BY cohort_month, period_number
WITH DATA;

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_cohorts_pk
    ON mv_customer_cohorts (cohort_month, period_number);

-- =============================================================================
-- MATERIALIZED VIEW: mv_funnel_analysis
-- Conversion funnel from page view to purchase.
-- =============================================================================

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_funnel_analysis AS
WITH daily_funnel AS (
    SELECT
        DATE_TRUNC('day', occurred_at)::DATE    AS event_date,
        COUNT(DISTINCT session_id) FILTER (WHERE event_type = 'page_view')      AS sessions,
        COUNT(DISTINCT session_id) FILTER (WHERE event_type = 'product_view')   AS product_views,
        COUNT(DISTINCT session_id) FILTER (WHERE event_type = 'add_to_cart')    AS cart_adds,
        COUNT(DISTINCT session_id) FILTER (WHERE event_type = 'checkout_start') AS checkout_starts,
        COUNT(DISTINCT session_id) FILTER (WHERE event_type = 'purchase')       AS purchases
    FROM events
    GROUP BY DATE_TRUNC('day', occurred_at)::DATE
)
SELECT
    event_date,
    sessions,
    product_views,
    cart_adds,
    checkout_starts,
    purchases,
    -- Step-over-step conversion rates
    CASE WHEN sessions > 0
         THEN ROUND(product_views::NUMERIC / sessions * 100, 2) END         AS pv_session_rate,
    CASE WHEN product_views > 0
         THEN ROUND(cart_adds::NUMERIC / product_views * 100, 2) END        AS atc_pv_rate,
    CASE WHEN cart_adds > 0
         THEN ROUND(checkout_starts::NUMERIC / cart_adds * 100, 2) END      AS checkout_atc_rate,
    CASE WHEN checkout_starts > 0
         THEN ROUND(purchases::NUMERIC / checkout_starts * 100, 2) END      AS purchase_checkout_rate,
    -- End-to-end session-to-purchase rate
    CASE WHEN sessions > 0
         THEN ROUND(purchases::NUMERIC / sessions * 100, 2) END             AS overall_conversion_rate
FROM daily_funnel
WITH DATA;

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_funnel_date
    ON mv_funnel_analysis (event_date);

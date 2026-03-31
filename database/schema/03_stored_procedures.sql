-- =============================================================================
-- Real-Time E-Commerce Analytics Platform
-- 03_stored_procedures.sql — Business Logic Stored Procedures
-- =============================================================================
-- Implements core business operations as transactional stored procedures.
-- Run after: 01_init_rds.sql, 02_partitioning.sql
-- =============================================================================

-- =============================================================================
-- PROCEDURE: sp_process_order
-- Full order creation in a single atomic transaction:
--   1. Validate customer and products exist
--   2. Check and reserve inventory
--   3. Validate and apply discount code
--   4. Create order and order_items records
--   5. Deduct inventory (quantity_reserved)
--   6. Award loyalty points
-- Returns: order_id on success, raises exception on any failure.
-- =============================================================================

CREATE OR REPLACE FUNCTION sp_process_order(
    p_customer_id       UUID,
    p_items             JSONB,    -- [{"product_id": "...", "quantity": N}]
    p_discount_code     TEXT DEFAULT NULL,
    p_shipping_cents    INT  DEFAULT 0,
    p_shipping_address  JSONB DEFAULT NULL,
    p_payment_method    payment_method DEFAULT 'credit_card',
    p_session_id        UUID DEFAULT NULL,
    p_ip_address        INET DEFAULT NULL
)
RETURNS UUID
LANGUAGE plpgsql
AS $$
DECLARE
    v_order_id          UUID;
    v_item              JSONB;
    v_product           RECORD;
    v_discount          RECORD;
    v_subtotal_cents    INT  := 0;
    v_discount_cents    INT  := 0;
    v_tax_cents         INT  := 0;
    v_total_cents       INT;
    v_discount_id       BIGINT;
    v_loyalty_points    INT;
    v_item_total        INT;
BEGIN
    -- ── Validate customer exists and is active ──────────────────────────────
    IF NOT EXISTS (
        SELECT 1 FROM customers
        WHERE customer_id = p_customer_id AND is_active = TRUE AND deleted_at IS NULL
    ) THEN
        RAISE EXCEPTION 'Customer % not found or inactive', p_customer_id
            USING ERRCODE = 'P0001';
    END IF;

    -- ── Validate items array is not empty ───────────────────────────────────
    IF p_items IS NULL OR jsonb_array_length(p_items) = 0 THEN
        RAISE EXCEPTION 'Order must contain at least one item'
            USING ERRCODE = 'P0002';
    END IF;

    -- ── Lock inventory rows to prevent concurrent over-reservation ──────────
    -- Use SELECT FOR UPDATE to serialize concurrent orders for the same products
    FOR v_item IN SELECT * FROM jsonb_array_elements(p_items) LOOP
        PERFORM 1
        FROM inventory
        WHERE product_id = (v_item->>'product_id')::UUID
        FOR UPDATE NOWAIT;

        IF NOT FOUND THEN
            RAISE EXCEPTION 'Product % has no inventory record',
                v_item->>'product_id' USING ERRCODE = 'P0003';
        END IF;
    END LOOP;

    -- ── Generate order ID ───────────────────────────────────────────────────
    v_order_id := uuid_generate_v4();

    -- ── Process each item ───────────────────────────────────────────────────
    FOR v_item IN SELECT * FROM jsonb_array_elements(p_items) LOOP

        -- Fetch product details
        SELECT p.product_id, p.name, p.sku,
               COALESCE(p.sale_price_cents, p.base_price_cents) AS effective_price_cents,
               i.quantity_available
        INTO v_product
        FROM products p
        JOIN inventory i ON i.product_id = p.product_id
        WHERE p.product_id = (v_item->>'product_id')::UUID
          AND p.is_active = TRUE
          AND p.deleted_at IS NULL;

        IF NOT FOUND THEN
            RAISE EXCEPTION 'Product % not found or inactive', v_item->>'product_id'
                USING ERRCODE = 'P0004';
        END IF;

        -- Check availability
        IF v_product.quantity_available < (v_item->>'quantity')::INT THEN
            RAISE EXCEPTION 'Insufficient inventory for product % (available: %, requested: %)',
                v_product.sku,
                v_product.quantity_available,
                (v_item->>'quantity')::INT
            USING ERRCODE = 'P0005';
        END IF;

        v_item_total := v_product.effective_price_cents * (v_item->>'quantity')::INT;
        v_subtotal_cents := v_subtotal_cents + v_item_total;

        -- Insert order item
        INSERT INTO order_items (
            order_id, order_created_at, product_id, quantity,
            unit_price_cents, total_cents, product_name, product_sku
        )
        VALUES (
            v_order_id, CURRENT_TIMESTAMP,
            v_product.product_id, (v_item->>'quantity')::INT,
            v_product.effective_price_cents, v_item_total,
            v_product.name, v_product.sku
        );

        -- Reserve inventory
        UPDATE inventory
        SET quantity_reserved = quantity_reserved + (v_item->>'quantity')::INT
        WHERE product_id = v_product.product_id;

    END LOOP;

    -- ── Apply discount code ─────────────────────────────────────────────────
    IF p_discount_code IS NOT NULL AND p_discount_code != '' THEN

        SELECT discount_id, discount_type, discount_value,
               minimum_order_cents, maximum_uses, current_uses, uses_per_customer
        INTO v_discount
        FROM discount_codes
        WHERE code = UPPER(p_discount_code)
          AND is_active = TRUE
          AND (valid_until IS NULL OR valid_until > CURRENT_TIMESTAMP)
          AND valid_from <= CURRENT_TIMESTAMP
        FOR UPDATE;

        IF NOT FOUND THEN
            RAISE EXCEPTION 'Discount code % is invalid or expired', p_discount_code
                USING ERRCODE = 'P0006';
        END IF;

        IF v_subtotal_cents < v_discount.minimum_order_cents THEN
            RAISE EXCEPTION 'Order subtotal ($%.2f) is below minimum for this discount ($%.2f)',
                v_subtotal_cents / 100.0,
                v_discount.minimum_order_cents / 100.0
            USING ERRCODE = 'P0007';
        END IF;

        IF v_discount.maximum_uses IS NOT NULL
            AND v_discount.current_uses >= v_discount.maximum_uses
        THEN
            RAISE EXCEPTION 'Discount code % has reached its maximum usage limit',
                p_discount_code USING ERRCODE = 'P0008';
        END IF;

        -- Calculate discount amount
        CASE v_discount.discount_type
            WHEN 'percentage' THEN
                v_discount_cents := ROUND(v_subtotal_cents * v_discount.discount_value / 100)::INT;
            WHEN 'fixed_amount' THEN
                v_discount_cents := LEAST(
                    ROUND(v_discount.discount_value * 100)::INT,
                    v_subtotal_cents
                );
            WHEN 'free_shipping' THEN
                v_discount_cents := p_shipping_cents;
            ELSE
                v_discount_cents := 0;
        END CASE;

        v_discount_id := v_discount.discount_id;

        -- Increment usage counter
        UPDATE discount_codes
        SET current_uses = current_uses + 1
        WHERE discount_id = v_discount.discount_id;
    END IF;

    -- ── Calculate tax (simplified 8% flat rate — real impl would use product tax class) ──
    v_tax_cents := ROUND((v_subtotal_cents - v_discount_cents) * 0.08)::INT;

    -- ── Total ───────────────────────────────────────────────────────────────
    v_total_cents := v_subtotal_cents - v_discount_cents + p_shipping_cents + v_tax_cents;

    -- ── Insert order ────────────────────────────────────────────────────────
    INSERT INTO orders (
        order_id, customer_id, status, payment_status, payment_method,
        subtotal_cents, discount_cents, shipping_cents, tax_cents, total_cents,
        discount_id, discount_code,
        shipping_name, shipping_address1, shipping_address2,
        shipping_city, shipping_state, shipping_postal, shipping_country,
        session_id, ip_address,
        created_at
    )
    VALUES (
        v_order_id, p_customer_id, 'pending', 'pending', p_payment_method,
        v_subtotal_cents, v_discount_cents, p_shipping_cents, v_tax_cents, v_total_cents,
        v_discount_id, UPPER(p_discount_code),
        p_shipping_address->>'name',
        p_shipping_address->>'address1', p_shipping_address->>'address2',
        p_shipping_address->>'city', p_shipping_address->>'state',
        p_shipping_address->>'postal', COALESCE(p_shipping_address->>'country', 'US'),
        p_session_id, p_ip_address,
        CURRENT_TIMESTAMP
    );

    -- ── Award loyalty points (1 point per $1 spent) ─────────────────────────
    v_loyalty_points := FLOOR(v_total_cents / 100)::INT;
    IF v_loyalty_points > 0 THEN
        UPDATE customers
        SET loyalty_points = loyalty_points + v_loyalty_points
        WHERE customer_id = p_customer_id;
    END IF;

    RETURN v_order_id;

EXCEPTION
    WHEN OTHERS THEN
        -- Re-raise with context
        RAISE EXCEPTION 'sp_process_order failed: % (SQLSTATE: %)', SQLERRM, SQLSTATE;
END;
$$;

COMMENT ON FUNCTION sp_process_order IS
    'Atomically creates an order with inventory reservation, discount application, '
    'and loyalty point award. Raises descriptive exceptions on any validation failure.';

-- =============================================================================
-- FUNCTION: sp_calculate_customer_ltv
-- Calculates lifetime value metrics for one or all customers.
-- Returns: table of customer LTV metrics.
-- =============================================================================

CREATE OR REPLACE FUNCTION sp_calculate_customer_ltv(
    p_customer_id UUID DEFAULT NULL   -- NULL = calculate for all customers
)
RETURNS TABLE (
    customer_id         UUID,
    email               VARCHAR(320),
    first_order_date    DATE,
    last_order_date     DATE,
    order_count         BIGINT,
    total_revenue_cents BIGINT,
    avg_order_value_cents NUMERIC,
    orders_per_month    NUMERIC,
    tenure_months       NUMERIC,
    predicted_ltv_12m_cents BIGINT,
    ltv_tier            TEXT
)
LANGUAGE plpgsql
STABLE
AS $$
BEGIN
    RETURN QUERY
    WITH order_metrics AS (
        SELECT
            o.customer_id,
            COUNT(*)                        AS order_count,
            MIN(o.created_at::DATE)         AS first_order_date,
            MAX(o.created_at::DATE)         AS last_order_date,
            SUM(o.total_cents)::BIGINT       AS total_revenue_cents,
            ROUND(AVG(o.total_cents), 2)    AS avg_order_value_cents,
            -- Tenure: months since first order
            GREATEST(1,
                EXTRACT(YEAR FROM AGE(CURRENT_DATE, MIN(o.created_at::DATE))) * 12
                + EXTRACT(MONTH FROM AGE(CURRENT_DATE, MIN(o.created_at::DATE)))
            )::NUMERIC                      AS tenure_months
        FROM orders o
        WHERE o.status NOT IN ('cancelled')
          AND o.payment_status IN ('captured', 'partially_refunded')
          AND (p_customer_id IS NULL OR o.customer_id = p_customer_id)
        GROUP BY o.customer_id
    ),
    ltv_calc AS (
        SELECT
            m.*,
            ROUND(m.order_count::NUMERIC / m.tenure_months, 4) AS orders_per_month,
            -- Simple LTV projection: avg_order_value * orders_per_month * 12
            -- (In production: use Pareto-NBD model or BG/NBD)
            ROUND(
                m.avg_order_value_cents
                * (m.order_count::NUMERIC / m.tenure_months)
                * 12
            )::BIGINT AS predicted_ltv_12m_cents
        FROM order_metrics m
    )
    SELECT
        l.customer_id,
        c.email,
        l.first_order_date,
        l.last_order_date,
        l.order_count,
        l.total_revenue_cents,
        l.avg_order_value_cents,
        l.orders_per_month,
        l.tenure_months,
        l.predicted_ltv_12m_cents,
        CASE
            WHEN l.predicted_ltv_12m_cents >= 100000  THEN 'platinum'   -- >$1,000
            WHEN l.predicted_ltv_12m_cents >= 50000   THEN 'gold'       -- >$500
            WHEN l.predicted_ltv_12m_cents >= 20000   THEN 'silver'     -- >$200
            ELSE                                            'bronze'
        END AS ltv_tier
    FROM ltv_calc l
    JOIN customers c USING (customer_id);
END;
$$;

-- =============================================================================
-- PROCEDURE: sp_refresh_materialized_views
-- Refreshes all materialized views concurrently (non-blocking).
-- Logs execution time for monitoring.
-- =============================================================================

CREATE OR REPLACE PROCEDURE sp_refresh_materialized_views()
LANGUAGE plpgsql
AS $$
DECLARE
    v_start         TIMESTAMPTZ;
    v_views         TEXT[] := ARRAY[
        'mv_daily_sales_summary',
        'mv_product_performance',
        'mv_customer_cohorts',
        'mv_funnel_analysis'
    ];
    v_view          TEXT;
BEGIN
    FOREACH v_view IN ARRAY v_views LOOP
        v_start := clock_timestamp();

        BEGIN
            EXECUTE format('REFRESH MATERIALIZED VIEW CONCURRENTLY %I', v_view);

            INSERT INTO mv_refresh_log (view_name, refreshed_at, duration_ms, success)
            VALUES (
                v_view,
                CURRENT_TIMESTAMP,
                EXTRACT(MILLISECONDS FROM (clock_timestamp() - v_start))::INT,
                TRUE
            );

        EXCEPTION WHEN OTHERS THEN
            INSERT INTO mv_refresh_log (view_name, refreshed_at, duration_ms, success, error_message)
            VALUES (
                v_view, CURRENT_TIMESTAMP,
                EXTRACT(MILLISECONDS FROM (clock_timestamp() - v_start))::INT,
                FALSE, SQLERRM
            );
            RAISE WARNING 'Failed to refresh %: %', v_view, SQLERRM;
        END;
    END LOOP;
END;
$$;

-- Audit log table for MV refreshes
CREATE TABLE IF NOT EXISTS mv_refresh_log (
    log_id          BIGSERIAL       PRIMARY KEY,
    view_name       VARCHAR(200)    NOT NULL,
    refreshed_at    TIMESTAMPTZ     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    duration_ms     INT,
    success         BOOLEAN         NOT NULL,
    error_message   TEXT
);

-- =============================================================================
-- PROCEDURE: sp_archive_old_data
-- Archives orders and events older than 2 years to S3 via aws_s3 extension,
-- then detaches and drops the old partitions.
-- =============================================================================

CREATE OR REPLACE PROCEDURE sp_archive_old_data(
    p_s3_bucket     TEXT,
    p_s3_prefix     TEXT DEFAULT 'archive/',
    p_dry_run       BOOLEAN DEFAULT FALSE
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_cutoff            DATE := CURRENT_DATE - INTERVAL '2 years';
    v_rec               RECORD;
    v_s3_path           TEXT;
    v_row_count         BIGINT;
    v_rows_exported     INT;
BEGIN
    RAISE NOTICE 'Starting archive for data older than %', v_cutoff;

    -- Archive old orders partitions
    FOR v_rec IN
        SELECT c.relname AS partition_name,
               pg_get_expr(c.relpartbound, c.oid, TRUE) AS range_expr
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        JOIN pg_inherits i ON i.inhrelid = c.oid
        JOIN pg_class parent ON parent.oid = i.inhparent
        WHERE parent.relname = 'orders'
          AND n.nspname = 'public'
          AND c.relname != 'orders_default'
        ORDER BY c.relname
    LOOP
        -- Only archive partitions fully before cutoff
        -- (Orders partition names: orders_YYYY_MM)
        CONTINUE WHEN v_rec.partition_name >=
            'orders_' || TO_CHAR(v_cutoff, 'YYYY_MM');

        EXECUTE format('SELECT COUNT(*) FROM %I', v_rec.partition_name)
            INTO v_row_count;

        v_s3_path := format('%s%s/%s.csv',
            p_s3_prefix,
            CURRENT_DATE::TEXT,
            v_rec.partition_name
        );

        RAISE NOTICE 'Archiving % (% rows) to s3://%/%',
            v_rec.partition_name, v_row_count, p_s3_bucket, v_s3_path;

        IF NOT p_dry_run THEN
            -- Export to S3 using aws_s3 extension
            SELECT rows_uploaded INTO v_rows_exported
            FROM aws_s3.query_export_to_s3(
                format('SELECT * FROM %I', v_rec.partition_name),
                aws_commons.create_s3_uri(p_s3_bucket, v_s3_path, 'us-east-1'),
                options := 'FORMAT CSV, HEADER true'
            );

            RAISE NOTICE 'Exported % rows to S3', v_rows_exported;

            -- Detach and drop partition
            EXECUTE format(
                'ALTER TABLE orders DETACH PARTITION %I',
                v_rec.partition_name
            );
            EXECUTE format('DROP TABLE %I', v_rec.partition_name);

            INSERT INTO data_archive_log (
                table_name, partition_name, row_count,
                s3_bucket, s3_key, archived_at
            )
            VALUES (
                'orders', v_rec.partition_name, v_row_count,
                p_s3_bucket, v_s3_path, CURRENT_TIMESTAMP
            );
        END IF;
    END LOOP;

    RAISE NOTICE 'Archive complete (dry_run=%)', p_dry_run;
END;
$$;

CREATE TABLE IF NOT EXISTS data_archive_log (
    log_id          BIGSERIAL       PRIMARY KEY,
    table_name      VARCHAR(100)    NOT NULL,
    partition_name  VARCHAR(200)    NOT NULL,
    row_count       BIGINT,
    s3_bucket       TEXT            NOT NULL,
    s3_key          TEXT            NOT NULL,
    archived_at     TIMESTAMPTZ     NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- =============================================================================
-- FUNCTION: sp_detect_fraud
-- Basic fraud detection heuristics applied to a batch of orders.
-- Returns flagged orders with reason codes.
-- In production, this would call a ML endpoint via pg_http extension.
-- =============================================================================

CREATE OR REPLACE FUNCTION sp_detect_fraud(
    p_lookback_hours INT DEFAULT 24
)
RETURNS TABLE (
    order_id        UUID,
    customer_id     UUID,
    total_cents     INT,
    risk_score      NUMERIC,
    risk_flags      TEXT[],
    recommendation  TEXT
)
LANGUAGE plpgsql
STABLE
AS $$
BEGIN
    RETURN QUERY
    WITH recent_orders AS (
        SELECT
            o.order_id,
            o.customer_id,
            o.total_cents,
            o.created_at,
            o.ip_address,
            o.shipping_country,
            -- Orders by this customer in the last 24h
            COUNT(*) OVER (
                PARTITION BY o.customer_id
                ORDER BY o.created_at
                RANGE BETWEEN INTERVAL '24 hours' PRECEDING AND CURRENT ROW
            ) AS customer_orders_24h,
            -- Orders from this IP in the last 1h
            COUNT(*) OVER (
                PARTITION BY o.ip_address
                ORDER BY o.created_at
                RANGE BETWEEN INTERVAL '1 hour' PRECEDING AND CURRENT ROW
            ) AS ip_orders_1h
        FROM orders o
        WHERE o.created_at > CURRENT_TIMESTAMP - make_interval(hours => p_lookback_hours)
          AND o.status NOT IN ('cancelled')
    ),
    customer_history AS (
        SELECT
            o.customer_id,
            AVG(o.total_cents)          AS avg_historical_order,
            STDDEV(o.total_cents)       AS stddev_historical_order,
            COUNT(*)                    AS lifetime_orders
        FROM orders o
        WHERE o.created_at < CURRENT_TIMESTAMP - INTERVAL '24 hours'
          AND o.status NOT IN ('cancelled')
          AND o.payment_status IN ('captured', 'partially_refunded')
        GROUP BY o.customer_id
    ),
    fraud_scored AS (
        SELECT
            ro.order_id,
            ro.customer_id,
            ro.total_cents,
            ro.customer_orders_24h,
            ro.ip_orders_1h,
            ch.avg_historical_order,
            ch.stddev_historical_order,
            ch.lifetime_orders,
            -- Risk flags
            ARRAY_REMOVE(ARRAY[
                CASE WHEN ro.customer_orders_24h > 5
                     THEN 'HIGH_VELOCITY: >5 orders in 24h' END,
                CASE WHEN ro.ip_orders_1h > 3
                     THEN 'IP_VELOCITY: >3 orders from same IP in 1h' END,
                CASE WHEN ro.total_cents > 50000
                     THEN 'HIGH_VALUE: order > $500' END,
                CASE WHEN ch.avg_historical_order IS NOT NULL
                          AND ch.stddev_historical_order > 0
                          AND ro.total_cents >
                              ch.avg_historical_order + 3 * ch.stddev_historical_order
                     THEN 'ANOMALOUS_AMOUNT: 3+ std deviations above customer average' END,
                CASE WHEN ch.lifetime_orders IS NULL OR ch.lifetime_orders = 0
                     THEN 'NEW_CUSTOMER: no prior purchase history' END,
                CASE WHEN ro.shipping_country NOT IN ('US', 'CA', 'GB', 'AU')
                     THEN 'HIGH_RISK_COUNTRY: non-standard shipping destination' END
            ], NULL) AS risk_flags
        FROM recent_orders ro
        LEFT JOIN customer_history ch ON ch.customer_id = ro.customer_id
    )
    SELECT
        fs.order_id,
        fs.customer_id,
        fs.total_cents,
        -- Simple additive risk score (0-100)
        LEAST(100, (
            CASE WHEN fs.customer_orders_24h > 5 THEN 25 ELSE 0 END +
            CASE WHEN fs.ip_orders_1h > 3 THEN 30 ELSE 0 END +
            CASE WHEN fs.total_cents > 50000 THEN 15 ELSE 0 END +
            CASE WHEN ch.avg_historical_order IS NOT NULL
                      AND ch.stddev_historical_order > 0
                      AND fs.total_cents >
                          ch.avg_historical_order + 3 * ch.stddev_historical_order
                 THEN 25 ELSE 0 END +
            CASE WHEN ch.lifetime_orders IS NULL OR ch.lifetime_orders = 0 THEN 10 ELSE 0 END +
            CASE WHEN fs.shipping_country NOT IN ('US', 'CA', 'GB', 'AU') THEN 15 ELSE 0 END
        ))::NUMERIC AS risk_score,
        fs.risk_flags,
        CASE
            WHEN LEAST(100, (
                CASE WHEN fs.customer_orders_24h > 5 THEN 25 ELSE 0 END +
                CASE WHEN fs.ip_orders_1h > 3 THEN 30 ELSE 0 END +
                CASE WHEN fs.total_cents > 50000 THEN 15 ELSE 0 END +
                CASE WHEN ch.lifetime_orders IS NULL OR ch.lifetime_orders = 0 THEN 10 ELSE 0 END
            )) >= 70 THEN 'BLOCK'
            WHEN LEAST(100, (
                CASE WHEN fs.customer_orders_24h > 5 THEN 25 ELSE 0 END +
                CASE WHEN fs.ip_orders_1h > 3 THEN 30 ELSE 0 END +
                CASE WHEN fs.total_cents > 50000 THEN 15 ELSE 0 END
            )) >= 40 THEN 'REVIEW'
            ELSE 'ALLOW'
        END AS recommendation
    FROM fraud_scored fs
    LEFT JOIN customer_history ch ON ch.customer_id = fs.customer_id
    WHERE array_length(fs.risk_flags, 1) > 0
    ORDER BY risk_score DESC;
END;
$$;

COMMENT ON FUNCTION sp_detect_fraud IS
    'Rule-based fraud detection returning risk-scored orders. '
    'In production, supplement with ML inference via a sidecar service.';

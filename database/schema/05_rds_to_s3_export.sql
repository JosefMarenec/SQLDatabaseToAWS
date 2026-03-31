-- =============================================================================
-- Real-Time E-Commerce Analytics Platform
-- 05_rds_to_s3_export.sql — RDS to S3 Export via aws_s3 Extension
-- =============================================================================
-- Requires the aws_s3 extension available on RDS PostgreSQL 10+.
-- Grant the RDS IAM role s3:PutObject permission on the target bucket.
-- Run after: 01_init_rds.sql
-- =============================================================================

-- Install the aws_s3 extension (requires RDS PostgreSQL with IAM role attached)
CREATE EXTENSION IF NOT EXISTS aws_s3 CASCADE;
CREATE EXTENSION IF NOT EXISTS aws_commons CASCADE;

-- =============================================================================
-- TABLE: export_job_log
-- Tracks all S3 export executions for audit and idempotency.
-- =============================================================================

CREATE TABLE IF NOT EXISTS export_job_log (
    job_id              BIGSERIAL       PRIMARY KEY,
    job_name            VARCHAR(200)    NOT NULL,
    table_name          VARCHAR(200)    NOT NULL,
    s3_bucket           VARCHAR(200)    NOT NULL,
    s3_key              TEXT            NOT NULL,
    rows_exported       BIGINT,
    bytes_exported      BIGINT,
    started_at          TIMESTAMPTZ     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at        TIMESTAMPTZ,
    status              VARCHAR(20)     NOT NULL DEFAULT 'running'
                            CHECK (status IN ('running', 'completed', 'failed')),
    error_message       TEXT,
    watermark_from      TIMESTAMPTZ,
    watermark_to        TIMESTAMPTZ
);

CREATE INDEX idx_export_log_job_name ON export_job_log(job_name, started_at DESC);
CREATE INDEX idx_export_log_status ON export_job_log(status) WHERE status = 'running';

-- =============================================================================
-- FUNCTION: fn_export_table_to_s3
-- Generic incremental table export to S3 as CSV.
-- Tracks watermark via export_job_log for idempotent incremental loads.
-- =============================================================================

CREATE OR REPLACE FUNCTION fn_export_table_to_s3(
    p_table_name        TEXT,
    p_s3_bucket         TEXT,
    p_s3_prefix         TEXT,
    p_aws_region        TEXT DEFAULT 'us-east-1',
    p_incremental_col   TEXT DEFAULT 'updated_at',
    p_incremental_hours INT  DEFAULT 1
)
RETURNS BIGINT      -- Returns number of rows exported
LANGUAGE plpgsql
AS $$
DECLARE
    v_job_id            BIGINT;
    v_watermark_from    TIMESTAMPTZ;
    v_watermark_to      TIMESTAMPTZ;
    v_s3_key            TEXT;
    v_export_result     RECORD;
    v_query             TEXT;
    v_rows_exported     BIGINT;
BEGIN
    -- Determine watermark window
    v_watermark_to   := CURRENT_TIMESTAMP;
    v_watermark_from := v_watermark_to - make_interval(hours => p_incremental_hours);

    -- Build S3 key: prefix/table_name/year=YYYY/month=MM/day=DD/table_YYYYMMDD_HHMMSS.csv
    v_s3_key := format(
        '%s/%s/year=%s/month=%s/day=%s/%s_%s.csv',
        p_s3_prefix,
        p_table_name,
        TO_CHAR(v_watermark_to, 'YYYY'),
        TO_CHAR(v_watermark_to, 'MM'),
        TO_CHAR(v_watermark_to, 'DD'),
        p_table_name,
        TO_CHAR(v_watermark_to, 'YYYYMMDD_HH24MISS')
    );

    -- Log job start
    INSERT INTO export_job_log (
        job_name, table_name, s3_bucket, s3_key,
        watermark_from, watermark_to
    )
    VALUES (
        format('%s_incremental', p_table_name),
        p_table_name, p_s3_bucket, v_s3_key,
        v_watermark_from, v_watermark_to
    )
    RETURNING job_id INTO v_job_id;

    -- Build incremental query
    v_query := format(
        'SELECT * FROM %I WHERE %I >= %L AND %I < %L',
        p_table_name,
        p_incremental_col, v_watermark_from,
        p_incremental_col, v_watermark_to
    );

    -- Execute S3 export
    BEGIN
        SELECT * INTO v_export_result
        FROM aws_s3.query_export_to_s3(
            v_query,
            aws_commons.create_s3_uri(p_s3_bucket, v_s3_key, p_aws_region),
            options := 'FORMAT CSV, HEADER true, FORCE_QUOTE *'
        );

        v_rows_exported := v_export_result.rows_uploaded;

        -- Update log with success
        UPDATE export_job_log
        SET
            status         = 'completed',
            rows_exported  = v_rows_exported,
            bytes_exported = v_export_result.bytes_uploaded,
            completed_at   = CURRENT_TIMESTAMP
        WHERE job_id = v_job_id;

        RETURN v_rows_exported;

    EXCEPTION WHEN OTHERS THEN
        UPDATE export_job_log
        SET status = 'failed', error_message = SQLERRM, completed_at = CURRENT_TIMESTAMP
        WHERE job_id = v_job_id;
        RAISE;
    END;
END;
$$;

-- =============================================================================
-- FUNCTION: fn_export_orders_to_s3
-- Exports orders with joined items (denormalized) for the data lake.
-- Produces one row per order item — ideal for Parquet conversion by Glue.
-- =============================================================================

CREATE OR REPLACE FUNCTION fn_export_orders_to_s3(
    p_s3_bucket         TEXT,
    p_start_date        DATE DEFAULT CURRENT_DATE - 1,
    p_end_date          DATE DEFAULT CURRENT_DATE,
    p_aws_region        TEXT DEFAULT 'us-east-1'
)
RETURNS BIGINT
LANGUAGE plpgsql
AS $$
DECLARE
    v_job_id        BIGINT;
    v_s3_key        TEXT;
    v_export_result RECORD;
BEGIN
    v_s3_key := format(
        'raw/orders/year=%s/month=%s/day=%s/orders_%s.csv',
        TO_CHAR(p_start_date, 'YYYY'),
        TO_CHAR(p_start_date, 'MM'),
        TO_CHAR(p_start_date, 'DD'),
        TO_CHAR(p_start_date, 'YYYYMMDD')
    );

    INSERT INTO export_job_log (
        job_name, table_name, s3_bucket, s3_key,
        watermark_from, watermark_to
    )
    VALUES (
        'orders_daily_export', 'orders', p_s3_bucket, v_s3_key,
        p_start_date::TIMESTAMPTZ, p_end_date::TIMESTAMPTZ
    )
    RETURNING job_id INTO v_job_id;

    -- Denormalized order export (one row per item)
    SELECT * INTO v_export_result
    FROM aws_s3.query_export_to_s3(
        format($q$
            SELECT
                o.order_id::TEXT        AS order_id,
                o.order_number,
                o.customer_id::TEXT     AS customer_id,
                o.status::TEXT          AS order_status,
                o.payment_status::TEXT  AS payment_status,
                o.payment_method::TEXT  AS payment_method,
                o.subtotal_cents,
                o.discount_cents,
                o.shipping_cents,
                o.tax_cents,
                o.total_cents,
                o.discount_code,
                o.shipping_country,
                o.shipping_city,
                o.shipping_state,
                o.created_at            AS order_created_at,
                o.confirmed_at,
                o.shipped_at,
                o.delivered_at,
                oi.item_id,
                oi.product_id::TEXT     AS product_id,
                oi.product_sku,
                oi.product_name,
                oi.quantity,
                oi.unit_price_cents,
                oi.discount_cents       AS item_discount_cents,
                oi.total_cents          AS item_total_cents,
                p.category_id,
                pc.name                 AS category_name,
                p.brand
            FROM orders o
            JOIN order_items oi ON oi.order_id = o.order_id
                AND oi.order_created_at = o.created_at
            JOIN products p ON p.product_id = oi.product_id
            JOIN product_categories pc ON pc.category_id = p.category_id
            WHERE o.created_at >= %L AND o.created_at < %L
            ORDER BY o.created_at, o.order_id
        $q$, p_start_date, p_end_date),
        aws_commons.create_s3_uri(p_s3_bucket, v_s3_key, p_aws_region),
        options := 'FORMAT CSV, HEADER true'
    );

    UPDATE export_job_log
    SET
        status        = 'completed',
        rows_exported = v_export_result.rows_uploaded,
        bytes_exported = v_export_result.bytes_uploaded,
        completed_at  = CURRENT_TIMESTAMP
    WHERE job_id = v_job_id;

    RETURN v_export_result.rows_uploaded;
END;
$$;

-- =============================================================================
-- FUNCTION: fn_export_events_to_s3
-- Exports a single day's events to S3.
-- Called daily by the partition manager for each retention-nearing partition.
-- =============================================================================

CREATE OR REPLACE FUNCTION fn_export_events_to_s3(
    p_s3_bucket     TEXT,
    p_date          DATE DEFAULT CURRENT_DATE - 1,
    p_aws_region    TEXT DEFAULT 'us-east-1'
)
RETURNS BIGINT
LANGUAGE plpgsql
AS $$
DECLARE
    v_s3_key        TEXT;
    v_export_result RECORD;
    v_job_id        BIGINT;
BEGIN
    v_s3_key := format(
        'raw/events/year=%s/month=%s/day=%s/events_%s.csv',
        TO_CHAR(p_date, 'YYYY'),
        TO_CHAR(p_date, 'MM'),
        TO_CHAR(p_date, 'DD'),
        TO_CHAR(p_date, 'YYYYMMDD')
    );

    INSERT INTO export_job_log (
        job_name, table_name, s3_bucket, s3_key,
        watermark_from, watermark_to
    )
    VALUES (
        'events_daily_export', 'events', p_s3_bucket, v_s3_key,
        p_date::TIMESTAMPTZ, (p_date + 1)::TIMESTAMPTZ
    )
    RETURNING job_id INTO v_job_id;

    SELECT * INTO v_export_result
    FROM aws_s3.query_export_to_s3(
        format($q$
            SELECT
                event_id::TEXT          AS event_id,
                session_id::TEXT        AS session_id,
                customer_id::TEXT       AS customer_id,
                anonymous_id::TEXT      AS anonymous_id,
                event_type::TEXT        AS event_type,
                occurred_at,
                page_url,
                page_title,
                referrer_url,
                product_id::TEXT        AS product_id,
                order_id::TEXT          AS order_id,
                search_query,
                properties::TEXT        AS properties_json
            FROM events
            WHERE occurred_at >= %L AND occurred_at < %L
            ORDER BY occurred_at, session_id
        $q$, p_date, p_date + 1),
        aws_commons.create_s3_uri(p_s3_bucket, v_s3_key, p_aws_region),
        options := 'FORMAT CSV, HEADER true'
    );

    UPDATE export_job_log
    SET
        status         = 'completed',
        rows_exported  = v_export_result.rows_uploaded,
        bytes_exported = v_export_result.bytes_uploaded,
        completed_at   = CURRENT_TIMESTAMP
    WHERE job_id = v_job_id;

    RETURN v_export_result.rows_uploaded;
END;
$$;

-- =============================================================================
-- FUNCTION: fn_scheduled_export
-- Master export function — exports all tables for a given date window.
-- Called by pg_cron daily (scheduled via RDS pg_cron extension).
-- =============================================================================

CREATE OR REPLACE FUNCTION fn_scheduled_export(
    p_s3_bucket     TEXT,
    p_date          DATE DEFAULT CURRENT_DATE - 1,
    p_aws_region    TEXT DEFAULT 'us-east-1'
)
RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_results   JSONB := '{}';
    v_rows      BIGINT;
BEGIN
    -- Export orders (denormalized with items)
    v_rows := fn_export_orders_to_s3(p_s3_bucket, p_date, p_date + 1, p_aws_region);
    v_results := v_results || jsonb_build_object('orders_rows', v_rows);

    -- Export events
    v_rows := fn_export_events_to_s3(p_s3_bucket, p_date, p_aws_region);
    v_results := v_results || jsonb_build_object('events_rows', v_rows);

    -- Export customers (full snapshot — SCD Type 2 will handle changes in Redshift)
    v_rows := fn_export_table_to_s3(
        'customers', p_s3_bucket, 'raw', p_aws_region, 'updated_at', 25
    );
    v_results := v_results || jsonb_build_object('customers_rows', v_rows);

    -- Export products
    v_rows := fn_export_table_to_s3(
        'products', p_s3_bucket, 'raw', p_aws_region, 'updated_at', 25
    );
    v_results := v_results || jsonb_build_object('products_rows', v_rows);

    -- Export inventory
    v_rows := fn_export_table_to_s3(
        'inventory', p_s3_bucket, 'raw', p_aws_region, 'updated_at', 25
    );
    v_results := v_results || jsonb_build_object('inventory_rows', v_rows);

    RETURN v_results;
END;
$$;

-- =============================================================================
-- SCHEDULE via pg_cron (requires pg_cron extension and superuser)
-- =============================================================================

-- SELECT cron.schedule(
--     'nightly-s3-export',
--     '0 1 * * *',    -- 1am UTC daily
--     $$SELECT fn_scheduled_export(
--         'ecom-analytics-prod-data-lake-raw',
--         CURRENT_DATE - 1,
--         'us-east-1'
--     )$$
-- );

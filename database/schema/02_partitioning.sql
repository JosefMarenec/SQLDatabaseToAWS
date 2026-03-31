-- =============================================================================
-- Real-Time E-Commerce Analytics Platform
-- 02_partitioning.sql — PostgreSQL Declarative Partitioning
-- =============================================================================
-- Creates initial partitions for orders (monthly) and events (daily),
-- plus automated partition management functions.
-- Run after: 01_init_rds.sql
-- =============================================================================

-- =============================================================================
-- ORDERS TABLE — Monthly Range Partitions
-- Strategy: RANGE on created_at, one partition per calendar month.
-- Rationale: Order queries almost always filter by date range; pruning entire
--            month partitions dramatically reduces I/O for historical queries.
-- =============================================================================

-- Create partitions for the current year + 2 months ahead
-- The partition manager Lambda (lambda/partition_manager/handler.py)
-- runs daily to ensure future partitions always exist.

DO $$
DECLARE
    v_start_date  DATE := DATE_TRUNC('month', CURRENT_DATE - INTERVAL '12 months');
    v_end_date    DATE := DATE_TRUNC('month', CURRENT_DATE + INTERVAL '3 months');
    v_month_start DATE;
    v_month_end   DATE;
    v_part_name   TEXT;
BEGIN
    v_month_start := v_start_date;

    WHILE v_month_start < v_end_date LOOP
        v_month_end  := v_month_start + INTERVAL '1 month';
        v_part_name  := 'orders_' || TO_CHAR(v_month_start, 'YYYY_MM');

        -- Skip if partition already exists
        IF NOT EXISTS (
            SELECT FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE c.relname = v_part_name
              AND n.nspname = 'public'
        ) THEN
            EXECUTE format(
                'CREATE TABLE %I PARTITION OF orders
                 FOR VALUES FROM (%L) TO (%L)',
                v_part_name,
                v_month_start,
                v_month_end
            );

            -- Each partition gets its own indexes
            EXECUTE format(
                'CREATE INDEX %I ON %I (customer_id, created_at DESC)',
                'idx_' || v_part_name || '_customer',
                v_part_name
            );

            EXECUTE format(
                'CREATE INDEX %I ON %I (status) WHERE status NOT IN (''delivered'', ''cancelled'')',
                'idx_' || v_part_name || '_status',
                v_part_name
            );

            RAISE NOTICE 'Created orders partition: % (% to %)',
                v_part_name, v_month_start, v_month_end;
        END IF;

        v_month_start := v_month_end;
    END LOOP;
END;
$$;

-- Default partition — catches out-of-range inserts rather than erroring
-- Useful during partition creation window transitions
CREATE TABLE IF NOT EXISTS orders_default PARTITION OF orders DEFAULT;

-- =============================================================================
-- EVENTS TABLE — Daily Range Partitions
-- Strategy: RANGE on occurred_at, one partition per calendar day.
-- Rationale: Events volume is high (~millions/day); daily pruning enables
--            efficient time-series queries and fast retention cleanup.
-- =============================================================================

DO $$
DECLARE
    v_start_date  DATE := CURRENT_DATE - 90;  -- 90 days history
    v_end_date    DATE := CURRENT_DATE + 7;   -- 7 days ahead
    v_day_start   DATE;
    v_day_end     DATE;
    v_part_name   TEXT;
BEGIN
    v_day_start := v_start_date;

    WHILE v_day_start < v_end_date LOOP
        v_day_end   := v_day_start + 1;
        v_part_name := 'events_' || TO_CHAR(v_day_start, 'YYYY_MM_DD');

        IF NOT EXISTS (
            SELECT FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE c.relname = v_part_name
              AND n.nspname = 'public'
        ) THEN
            EXECUTE format(
                'CREATE TABLE %I PARTITION OF events
                 FOR VALUES FROM (%L) TO (%L)',
                v_part_name,
                v_day_start,
                v_day_end
            );

            EXECUTE format(
                'CREATE INDEX %I ON %I (customer_id, occurred_at DESC)',
                'idx_' || v_part_name || '_customer',
                v_part_name
            );

            EXECUTE format(
                'CREATE INDEX %I ON %I (event_type, occurred_at)',
                'idx_' || v_part_name || '_type',
                v_part_name
            );

            EXECUTE format(
                'CREATE INDEX %I ON %I (session_id)',
                'idx_' || v_part_name || '_session',
                v_part_name
            );
        END IF;

        v_day_start := v_day_end;
    END LOOP;
END;
$$;

CREATE TABLE IF NOT EXISTS events_default PARTITION OF events DEFAULT;

-- =============================================================================
-- FUNCTION: fn_create_orders_partition
-- Creates a single monthly orders partition, idempotent.
-- =============================================================================

CREATE OR REPLACE FUNCTION fn_create_orders_partition(p_year INT, p_month INT)
RETURNS TEXT
LANGUAGE plpgsql
AS $$
DECLARE
    v_start_date  DATE;
    v_end_date    DATE;
    v_part_name   TEXT;
BEGIN
    v_start_date := make_date(p_year, p_month, 1);
    v_end_date   := v_start_date + INTERVAL '1 month';
    v_part_name  := 'orders_' || TO_CHAR(v_start_date, 'YYYY_MM');

    IF EXISTS (
        SELECT FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relname = v_part_name AND n.nspname = 'public'
    ) THEN
        RETURN format('Partition %s already exists', v_part_name);
    END IF;

    EXECUTE format(
        'CREATE TABLE %I PARTITION OF orders
         FOR VALUES FROM (%L) TO (%L)',
        v_part_name, v_start_date, v_end_date
    );

    EXECUTE format(
        'CREATE INDEX %I ON %I (customer_id, created_at DESC)',
        'idx_' || v_part_name || '_customer', v_part_name
    );
    EXECUTE format(
        'CREATE INDEX %I ON %I (status) WHERE status NOT IN (''delivered'', ''cancelled'')',
        'idx_' || v_part_name || '_status', v_part_name
    );
    EXECUTE format(
        'CREATE INDEX %I ON %I USING BRIN (created_at)',
        'idx_' || v_part_name || '_brin', v_part_name
    );

    RETURN format('Created partition: %s (%s to %s)',
        v_part_name, v_start_date, v_end_date);
END;
$$;

-- =============================================================================
-- FUNCTION: fn_create_events_partition
-- Creates a single daily events partition, idempotent.
-- =============================================================================

CREATE OR REPLACE FUNCTION fn_create_events_partition(p_date DATE)
RETURNS TEXT
LANGUAGE plpgsql
AS $$
DECLARE
    v_start_date  DATE := p_date;
    v_end_date    DATE := p_date + 1;
    v_part_name   TEXT;
BEGIN
    v_part_name := 'events_' || TO_CHAR(v_start_date, 'YYYY_MM_DD');

    IF EXISTS (
        SELECT FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relname = v_part_name AND n.nspname = 'public'
    ) THEN
        RETURN format('Partition %s already exists', v_part_name);
    END IF;

    EXECUTE format(
        'CREATE TABLE %I PARTITION OF events
         FOR VALUES FROM (%L) TO (%L)',
        v_part_name, v_start_date, v_end_date
    );

    EXECUTE format(
        'CREATE INDEX %I ON %I (customer_id, occurred_at DESC)',
        'idx_' || v_part_name || '_customer', v_part_name
    );
    EXECUTE format(
        'CREATE INDEX %I ON %I (event_type, occurred_at)',
        'idx_' || v_part_name || '_type', v_part_name
    );
    EXECUTE format(
        'CREATE INDEX %I ON %I (session_id)',
        'idx_' || v_part_name || '_session', v_part_name
    );
    EXECUTE format(
        'CREATE INDEX %I ON %I USING BRIN (occurred_at)',
        'idx_' || v_part_name || '_brin', v_part_name
    );

    RETURN format('Created partition: %s', v_part_name);
END;
$$;

-- =============================================================================
-- FUNCTION: fn_drop_old_partitions
-- Drops partitions older than the retention period.
-- For orders: 24 months. For events: 90 days.
-- Drops via DETACH + DROP to allow controlled removal.
-- =============================================================================

CREATE OR REPLACE FUNCTION fn_drop_old_partitions(
    p_table_name         TEXT,
    p_retention_months   INT DEFAULT 24
)
RETURNS TABLE (partition_name TEXT, action TEXT)
LANGUAGE plpgsql
AS $$
DECLARE
    v_rec           RECORD;
    v_cutoff        TIMESTAMPTZ;
    v_part_date     DATE;
    v_part_pattern  TEXT;
BEGIN
    v_cutoff := CURRENT_TIMESTAMP - make_interval(months => p_retention_months);

    FOR v_rec IN
        SELECT c.relname AS partition_name
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        JOIN pg_inherits i ON i.inhrelid = c.oid
        JOIN pg_class parent ON parent.oid = i.inhparent
        WHERE parent.relname = p_table_name
          AND n.nspname = 'public'
          AND c.relname != p_table_name || '_default'
        ORDER BY c.relname
    LOOP
        -- Extract date from partition name (orders_2022_01 or events_2022_01_15)
        BEGIN
            IF p_table_name = 'orders' THEN
                v_part_date := TO_DATE(
                    RIGHT(v_rec.partition_name, 7), 'YYYY_MM'
                );
            ELSIF p_table_name = 'events' THEN
                v_part_date := TO_DATE(
                    RIGHT(v_rec.partition_name, 10), 'YYYY_MM_DD'
                );
            ELSE
                CONTINUE;
            END IF;
        EXCEPTION WHEN OTHERS THEN
            CONTINUE;  -- Skip partitions whose names don't match expected format
        END;

        IF v_part_date < v_cutoff::DATE THEN
            -- Detach first (makes the partition a standalone table)
            EXECUTE format(
                'ALTER TABLE %I DETACH PARTITION %I',
                p_table_name, v_rec.partition_name
            );
            -- Drop the detached table
            EXECUTE format('DROP TABLE IF EXISTS %I', v_rec.partition_name);

            partition_name := v_rec.partition_name;
            action         := 'DROPPED';
            RETURN NEXT;

            RAISE NOTICE 'Dropped old partition: %', v_rec.partition_name;
        END IF;
    END LOOP;
END;
$$;

-- =============================================================================
-- FUNCTION: fn_partition_maintenance
-- Master function called by the partition manager Lambda.
-- Creates upcoming partitions + drops old ones.
-- =============================================================================

CREATE OR REPLACE FUNCTION fn_partition_maintenance(
    p_months_ahead          INT DEFAULT 2,
    p_order_retention_months INT DEFAULT 24,
    p_event_retention_days  INT DEFAULT 90
)
RETURNS JSONB
LANGUAGE plpgsql
AS $$
DECLARE
    v_results       JSONB := '{"created": [], "dropped": []}';
    v_result_text   TEXT;
    v_i             INT;
    v_future_date   DATE;
BEGIN
    -- Create future orders partitions
    FOR v_i IN 0..p_months_ahead LOOP
        v_future_date := DATE_TRUNC('month', CURRENT_DATE + make_interval(months => v_i));
        v_result_text := fn_create_orders_partition(
            EXTRACT(YEAR FROM v_future_date)::INT,
            EXTRACT(MONTH FROM v_future_date)::INT
        );
        v_results := jsonb_set(
            v_results, '{created}',
            (v_results->'created') || to_jsonb(v_result_text)
        );
    END LOOP;

    -- Create future events partitions (7 days ahead)
    FOR v_i IN 0..6 LOOP
        v_future_date := CURRENT_DATE + v_i;
        v_result_text := fn_create_events_partition(v_future_date);
        v_results := jsonb_set(
            v_results, '{created}',
            (v_results->'created') || to_jsonb(v_result_text)
        );
    END LOOP;

    -- Drop old orders partitions
    INSERT INTO partition_maintenance_log (table_name, partition_name, action, executed_at)
    SELECT 'orders', partition_name, action, CURRENT_TIMESTAMP
    FROM fn_drop_old_partitions('orders', p_order_retention_months);

    -- Drop old events partitions (convert days to months approximation)
    INSERT INTO partition_maintenance_log (table_name, partition_name, action, executed_at)
    SELECT 'events', partition_name, action, CURRENT_TIMESTAMP
    FROM fn_drop_old_partitions('events', CEIL(p_event_retention_days / 30.0)::INT);

    RETURN v_results;
END;
$$;

-- =============================================================================
-- TABLE: partition_maintenance_log
-- Audit log for partition creation and deletion operations.
-- =============================================================================

CREATE TABLE IF NOT EXISTS partition_maintenance_log (
    log_id          BIGSERIAL       PRIMARY KEY,
    table_name      VARCHAR(100)    NOT NULL,
    partition_name  VARCHAR(200)    NOT NULL,
    action          VARCHAR(50)     NOT NULL,  -- CREATED | DROPPED
    executed_at     TIMESTAMPTZ     NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_part_maint_log_table ON partition_maintenance_log(table_name, executed_at DESC);

-- =============================================================================
-- VIEW: v_partition_info
-- Shows current partition boundaries and row counts.
-- =============================================================================

CREATE OR REPLACE VIEW v_partition_info AS
SELECT
    p.relname                                               AS partition_name,
    parent.relname                                          AS parent_table,
    pg_get_expr(p.relpartbound, p.oid, TRUE)                AS partition_range,
    pg_size_pretty(pg_relation_size(p.oid))                 AS partition_size,
    pg_size_pretty(pg_total_relation_size(p.oid))           AS total_size_with_indexes,
    (SELECT reltuples::BIGINT FROM pg_class WHERE oid = p.oid) AS estimated_row_count
FROM pg_class p
JOIN pg_namespace n ON n.oid = p.relnamespace
JOIN pg_inherits i ON i.inhrelid = p.oid
JOIN pg_class parent ON parent.oid = i.inhparent
WHERE n.nspname = 'public'
  AND parent.relname IN ('orders', 'events')
ORDER BY parent.relname, p.relname;

COMMENT ON VIEW v_partition_info IS
    'Shows all active partitions for the orders and events tables with size and row estimates.';

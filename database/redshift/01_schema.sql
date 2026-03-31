-- =============================================================================
-- Real-Time E-Commerce Analytics Platform
-- Redshift 01_schema.sql — Data Warehouse Star Schema
-- =============================================================================
-- Creates dimension and fact tables optimized for analytical workloads.
-- Distribution and sort key strategy documented inline.
-- Run as: rsadmin or superuser
-- =============================================================================

-- =============================================================================
-- SCHEMAS
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS dim;      -- Dimension tables
CREATE SCHEMA IF NOT EXISTS fact;     -- Fact tables
CREATE SCHEMA IF NOT EXISTS staging;  -- Staging area for COPY loads
CREATE SCHEMA IF NOT EXISTS mart;     -- Pre-computed data marts

-- Grant usage to analytics users
CREATE GROUP analytics_users;
GRANT USAGE ON SCHEMA dim, fact, mart TO GROUP analytics_users;
GRANT SELECT ON ALL TABLES IN SCHEMA dim TO GROUP analytics_users;
GRANT SELECT ON ALL TABLES IN SCHEMA fact TO GROUP analytics_users;
GRANT SELECT ON ALL TABLES IN SCHEMA mart TO GROUP analytics_users;

-- ETL role — used by Glue jobs
CREATE USER etl_user PASSWORD DISABLE;
GRANT USAGE ON SCHEMA dim, fact, staging TO etl_user;
GRANT ALL ON ALL TABLES IN SCHEMA staging TO etl_user;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA dim TO etl_user;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA fact TO etl_user;

-- =============================================================================
-- DIMENSION: dim_date
-- Pre-populated date dimension (2020-01-01 through 2030-12-31).
-- DISTSTYLE ALL — small table, broadcast to every node to avoid redistribution.
-- =============================================================================

CREATE TABLE IF NOT EXISTS dim.dim_date (
    date_key            INT             NOT NULL     ENCODE az64,
    full_date           DATE            NOT NULL     ENCODE delta32k,
    day_of_week         SMALLINT        NOT NULL     ENCODE bytedict,    -- 0=Sun ... 6=Sat
    day_name            VARCHAR(10)     NOT NULL     ENCODE bytedict,
    day_of_month        SMALLINT        NOT NULL     ENCODE bytedict,
    day_of_year         SMALLINT        NOT NULL     ENCODE az64,
    week_of_year        SMALLINT        NOT NULL     ENCODE bytedict,
    month_num           SMALLINT        NOT NULL     ENCODE bytedict,
    month_name          VARCHAR(10)     NOT NULL     ENCODE bytedict,
    month_short         CHAR(3)         NOT NULL     ENCODE bytedict,
    quarter             SMALLINT        NOT NULL     ENCODE bytedict,
    quarter_name        CHAR(2)         NOT NULL     ENCODE bytedict,
    year                SMALLINT        NOT NULL     ENCODE az64,
    year_month          CHAR(7)         NOT NULL     ENCODE bytedict,    -- 2024-01
    fiscal_year         SMALLINT        NOT NULL     ENCODE az64,        -- April fiscal year start
    fiscal_quarter      SMALLINT        NOT NULL     ENCODE bytedict,
    is_weekend          BOOLEAN         NOT NULL     ENCODE bytedict,
    is_holiday          BOOLEAN         NOT NULL DEFAULT FALSE ENCODE bytedict,
    holiday_name        VARCHAR(50)                  ENCODE bytedict,

    PRIMARY KEY (date_key)
)
DISTSTYLE ALL
SORTKEY (full_date);

-- Populate date dimension
INSERT INTO dim.dim_date
SELECT
    TO_NUMBER(TO_CHAR(d::DATE, 'YYYYMMDD'), '99999999')::INT        AS date_key,
    d::DATE                                                          AS full_date,
    EXTRACT(DOW FROM d)::SMALLINT                                    AS day_of_week,
    TO_CHAR(d, 'Day')                                                AS day_name,
    EXTRACT(DAY FROM d)::SMALLINT                                    AS day_of_month,
    EXTRACT(DOY FROM d)::SMALLINT                                    AS day_of_year,
    EXTRACT(WEEK FROM d)::SMALLINT                                   AS week_of_year,
    EXTRACT(MONTH FROM d)::SMALLINT                                  AS month_num,
    TO_CHAR(d, 'Month')                                              AS month_name,
    TO_CHAR(d, 'Mon')                                                AS month_short,
    EXTRACT(QUARTER FROM d)::SMALLINT                                AS quarter,
    'Q' || EXTRACT(QUARTER FROM d)::TEXT                             AS quarter_name,
    EXTRACT(YEAR FROM d)::SMALLINT                                   AS year,
    TO_CHAR(d, 'YYYY-MM')                                            AS year_month,
    -- Fiscal year: April start
    CASE WHEN EXTRACT(MONTH FROM d) >= 4
         THEN EXTRACT(YEAR FROM d)::SMALLINT
         ELSE (EXTRACT(YEAR FROM d) - 1)::SMALLINT
    END                                                              AS fiscal_year,
    CASE
        WHEN EXTRACT(MONTH FROM d) IN (4,5,6)  THEN 1
        WHEN EXTRACT(MONTH FROM d) IN (7,8,9)  THEN 2
        WHEN EXTRACT(MONTH FROM d) IN (10,11,12) THEN 3
        ELSE 4
    END::SMALLINT                                                    AS fiscal_quarter,
    (EXTRACT(DOW FROM d) IN (0,6))::BOOLEAN                         AS is_weekend,
    FALSE                                                            AS is_holiday,
    NULL                                                             AS holiday_name
FROM (
    SELECT DATEADD('day', n, '2020-01-01'::DATE) AS d
    FROM (
        SELECT ROW_NUMBER() OVER () - 1 AS n
        FROM stl_scan
        LIMIT 4018  -- 2020-01-01 through 2030-12-31
    )
) dates
WHERE d <= '2030-12-31'
  AND NOT EXISTS (SELECT 1 FROM dim.dim_date WHERE full_date = d::DATE);

-- =============================================================================
-- DIMENSION: dim_customers
-- SCD Type 2 — tracks customer attribute changes over time.
-- DISTKEY on customer_id — co-located with fact_orders.
-- =============================================================================

CREATE TABLE IF NOT EXISTS dim.dim_customers (
    customer_key        BIGINT IDENTITY(1,1)  NOT NULL  ENCODE az64,
    customer_id         VARCHAR(36)           NOT NULL  ENCODE zstd,    -- UUID from RDS
    email               VARCHAR(320)          NOT NULL  ENCODE zstd,
    first_name          VARCHAR(100)                    ENCODE zstd,
    last_name           VARCHAR(100)                    ENCODE zstd,
    full_name           VARCHAR(201)                    ENCODE zstd,
    country_code        CHAR(2)                         ENCODE bytedict,
    city                VARCHAR(100)                    ENCODE zstd,
    state_province      VARCHAR(100)                    ENCODE zstd,
    loyalty_tier        VARCHAR(20)                     ENCODE bytedict,
    acquisition_source  VARCHAR(100)                    ENCODE bytedict,
    acquisition_medium  VARCHAR(100)                    ENCODE bytedict,
    gender              CHAR(1)                         ENCODE bytedict,
    marketing_opt_in    BOOLEAN                         ENCODE bytedict,
    customer_since_date DATE                            ENCODE delta32k,
    -- SCD Type 2 columns
    effective_from      DATE            NOT NULL         ENCODE delta32k,
    effective_to        DATE                             ENCODE delta32k,
    is_current          BOOLEAN         NOT NULL DEFAULT TRUE ENCODE bytedict,
    -- Audit
    dw_created_at       TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP ENCODE az64,
    dw_updated_at       TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP ENCODE az64,

    PRIMARY KEY (customer_key)
)
DISTKEY (customer_id)
SORTKEY (customer_id, effective_from);

-- =============================================================================
-- DIMENSION: dim_products
-- SCD Type 2 — tracks product attribute and price changes.
-- DISTKEY on product_id — co-located with fact_orders.
-- =============================================================================

CREATE TABLE IF NOT EXISTS dim.dim_products (
    product_key         BIGINT IDENTITY(1,1)  NOT NULL  ENCODE az64,
    product_id          VARCHAR(36)           NOT NULL  ENCODE zstd,
    sku                 VARCHAR(100)          NOT NULL  ENCODE zstd,
    product_name        VARCHAR(500)          NOT NULL  ENCODE zstd,
    category_id         INT                             ENCODE az64,
    category_name       VARCHAR(150)                    ENCODE bytedict,
    parent_category     VARCHAR(150)                    ENCODE bytedict,
    brand               VARCHAR(200)                    ENCODE bytedict,
    base_price_cents    INT                             ENCODE az64,
    is_digital          BOOLEAN                         ENCODE bytedict,
    is_taxable          BOOLEAN                         ENCODE bytedict,
    -- SCD Type 2
    effective_from      DATE            NOT NULL         ENCODE delta32k,
    effective_to        DATE                             ENCODE delta32k,
    is_current          BOOLEAN         NOT NULL DEFAULT TRUE ENCODE bytedict,
    dw_created_at       TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP ENCODE az64,

    PRIMARY KEY (product_key)
)
DISTKEY (product_id)
SORTKEY (product_id, effective_from);

-- =============================================================================
-- DIMENSION: dim_geography
-- Denormalized geography for spatial analytics.
-- DISTSTYLE ALL — small reference table.
-- =============================================================================

CREATE TABLE IF NOT EXISTS dim.dim_geography (
    geography_key       INT IDENTITY(1,1)   NOT NULL  ENCODE az64,
    country_code        CHAR(2)             NOT NULL  ENCODE bytedict,
    country_name        VARCHAR(100)        NOT NULL  ENCODE zstd,
    region              VARCHAR(100)                  ENCODE bytedict,
    sub_region          VARCHAR(100)                  ENCODE bytedict,
    state_province      VARCHAR(100)                  ENCODE zstd,
    city                VARCHAR(100)                  ENCODE zstd,
    postal_code         VARCHAR(20)                   ENCODE zstd,
    latitude            DECIMAL(9,6)                  ENCODE raw,
    longitude           DECIMAL(9,6)                  ENCODE raw,
    timezone            VARCHAR(50)                   ENCODE bytedict,
    currency_code       CHAR(3)                       ENCODE bytedict,

    PRIMARY KEY (geography_key)
)
DISTSTYLE ALL
SORTKEY (country_code, state_province, city);

-- =============================================================================
-- FACT: fact_orders
-- One row per order LINE ITEM. Grain: order_item.
-- DISTKEY on customer_key — joins to dim_customers without redistribution.
-- SORTKEY on order_date_key — range scans by date are the primary access pattern.
-- =============================================================================

CREATE TABLE IF NOT EXISTS fact.fact_orders (
    order_item_key      BIGINT IDENTITY(1,1) NOT NULL  ENCODE az64,
    order_id            VARCHAR(36)          NOT NULL  ENCODE zstd,
    order_item_id       BIGINT               NOT NULL  ENCODE az64,

    -- Foreign keys to dimensions
    order_date_key      INT                  NOT NULL  ENCODE az64,
    customer_key        BIGINT               NOT NULL  ENCODE az64,
    product_key         BIGINT               NOT NULL  ENCODE az64,
    ship_geography_key  INT                            ENCODE az64,

    -- Order attributes (degenerate dimensions)
    order_number        VARCHAR(50)                    ENCODE zstd,
    order_status        VARCHAR(30)                    ENCODE bytedict,
    payment_method      VARCHAR(30)                    ENCODE bytedict,
    discount_code       VARCHAR(50)                    ENCODE zstd,
    shipping_country    CHAR(2)                        ENCODE bytedict,

    -- Measures (all in cents)
    quantity            INT                  NOT NULL  ENCODE az64,
    unit_price_cents    INT                  NOT NULL  ENCODE az64,
    subtotal_cents      INT                  NOT NULL  ENCODE az64,
    discount_cents      INT                  NOT NULL DEFAULT 0 ENCODE az64,
    shipping_cents      INT                  NOT NULL DEFAULT 0 ENCODE az64,
    tax_cents           INT                  NOT NULL DEFAULT 0 ENCODE az64,
    total_cents         INT                  NOT NULL ENCODE az64,
    gross_margin_cents  INT                            ENCODE az64,

    -- Timestamps
    ordered_at          TIMESTAMP            NOT NULL  ENCODE az64,
    confirmed_at        TIMESTAMP                      ENCODE az64,
    shipped_at          TIMESTAMP                      ENCODE az64,
    delivered_at        TIMESTAMP                      ENCODE az64,

    -- Audit
    dw_loaded_at        TIMESTAMP            NOT NULL DEFAULT CURRENT_TIMESTAMP ENCODE az64,

    PRIMARY KEY (order_item_key),
    FOREIGN KEY (order_date_key) REFERENCES dim.dim_date(date_key),
    FOREIGN KEY (customer_key)   REFERENCES dim.dim_customers(customer_key),
    FOREIGN KEY (product_key)    REFERENCES dim.dim_products(product_key)
)
DISTKEY (customer_key)
COMPOUND SORTKEY (order_date_key, customer_key, product_key);

-- =============================================================================
-- FACT: fact_events
-- One row per clickstream event. Very high volume.
-- DISTKEY on customer_key — sessions are primarily analyzed per customer.
-- =============================================================================

CREATE TABLE IF NOT EXISTS fact.fact_events (
    event_key           BIGINT IDENTITY(1,1) NOT NULL  ENCODE az64,
    event_id            VARCHAR(36)          NOT NULL  ENCODE zstd,
    session_id          VARCHAR(36)                    ENCODE zstd,

    -- Foreign keys
    event_date_key      INT                  NOT NULL  ENCODE az64,
    customer_key        BIGINT                         ENCODE az64,   -- NULL for anonymous
    product_key         BIGINT                         ENCODE az64,   -- NULL if not product event

    -- Degenerate dimensions
    event_type          VARCHAR(30)          NOT NULL  ENCODE bytedict,
    device_type         VARCHAR(20)                    ENCODE bytedict,
    country_code        CHAR(2)                        ENCODE bytedict,
    utm_source          VARCHAR(100)                   ENCODE zstd,
    utm_medium          VARCHAR(100)                   ENCODE bytedict,
    utm_campaign        VARCHAR(100)                   ENCODE zstd,

    -- Measures
    occurred_at         TIMESTAMP            NOT NULL  ENCODE az64,

    -- Audit
    dw_loaded_at        TIMESTAMP            NOT NULL DEFAULT CURRENT_TIMESTAMP ENCODE az64,

    PRIMARY KEY (event_key),
    FOREIGN KEY (event_date_key) REFERENCES dim.dim_date(date_key)
)
DISTKEY (customer_key)
COMPOUND SORTKEY (event_date_key, event_type);

-- =============================================================================
-- FACT: fact_inventory_snapshots
-- Daily inventory snapshot — point-in-time stock levels.
-- DISTSTYLE EVEN — no obvious distkey; scans are typically full table.
-- =============================================================================

CREATE TABLE IF NOT EXISTS fact.fact_inventory_snapshots (
    snapshot_key        BIGINT IDENTITY(1,1) NOT NULL  ENCODE az64,
    snapshot_date_key   INT                  NOT NULL  ENCODE az64,
    product_key         BIGINT               NOT NULL  ENCODE az64,

    warehouse_id        VARCHAR(50)          NOT NULL  ENCODE bytedict,
    quantity_on_hand    INT                  NOT NULL  ENCODE az64,
    quantity_reserved   INT                  NOT NULL  ENCODE az64,
    quantity_available  INT                  NOT NULL  ENCODE az64,
    unit_cost_cents     INT                            ENCODE az64,
    inventory_value_cents BIGINT                       ENCODE az64,  -- qty * cost

    dw_loaded_at        TIMESTAMP            NOT NULL DEFAULT CURRENT_TIMESTAMP ENCODE az64,

    PRIMARY KEY (snapshot_key),
    FOREIGN KEY (snapshot_date_key) REFERENCES dim.dim_date(date_key),
    FOREIGN KEY (product_key)       REFERENCES dim.dim_products(product_key)
)
DISTSTYLE EVEN
SORTKEY (snapshot_date_key, product_key);

-- =============================================================================
-- STAGING TABLES
-- Temporary landing zone for COPY command loads.
-- Truncated before each batch load.
-- =============================================================================

CREATE TABLE IF NOT EXISTS staging.stg_orders (LIKE fact.fact_orders);
CREATE TABLE IF NOT EXISTS staging.stg_events (LIKE fact.fact_events);
CREATE TABLE IF NOT EXISTS staging.stg_customers (LIKE dim.dim_customers);
CREATE TABLE IF NOT EXISTS staging.stg_products (LIKE dim.dim_products);

-- =============================================================================
-- DATA MART: mart.customer_rfm
-- Pre-computed RFM scores — refreshed with each ETL load.
-- =============================================================================

CREATE TABLE IF NOT EXISTS mart.customer_rfm (
    customer_key        BIGINT          NOT NULL  ENCODE az64,
    customer_id         VARCHAR(36)     NOT NULL  ENCODE zstd,
    recency_days        INT                       ENCODE az64,
    frequency           INT                       ENCODE az64,
    monetary_cents      BIGINT                    ENCODE az64,
    r_score             SMALLINT                  ENCODE bytedict,   -- 1-5
    f_score             SMALLINT                  ENCODE bytedict,
    m_score             SMALLINT                  ENCODE bytedict,
    rfm_score           SMALLINT                  ENCODE az64,       -- r+f+m
    rfm_segment         VARCHAR(30)               ENCODE bytedict,
    calculated_at       TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP ENCODE az64,

    PRIMARY KEY (customer_key)
)
DISTSTYLE EVEN
SORTKEY (rfm_score);

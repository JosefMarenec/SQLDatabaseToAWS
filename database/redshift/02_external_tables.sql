-- =============================================================================
-- Real-Time E-Commerce Analytics Platform
-- Redshift 02_external_tables.sql — Redshift Spectrum External Tables
-- =============================================================================
-- Creates external schema and tables pointing to the S3 data lake via
-- AWS Glue catalog. Allows querying raw/processed S3 data directly from
-- Redshift without loading into the warehouse.
--
-- Prerequisites:
--   1. Replace <ACCOUNT_ID> and <IAM_ROLE_ARN> with actual values.
--   2. Glue crawlers must have run against the S3 paths first.
-- =============================================================================

-- =============================================================================
-- EXTERNAL SCHEMAS
-- Each schema maps to a Glue database (and therefore an S3 zone).
-- =============================================================================

-- External schema for processed zone (validated Parquet files)
CREATE EXTERNAL SCHEMA IF NOT EXISTS spectrum_processed
FROM DATA CATALOG
DATABASE 'ecom_analytics_prod'          -- Glue database name
IAM_ROLE 'arn:aws:iam::<ACCOUNT_ID>:role/ecom-analytics-prod-redshift-s3-role'
REGION 'us-east-1'
CREATE EXTERNAL DATABASE IF NOT EXISTS;

-- External schema for raw zone (events, logs, exports)
CREATE EXTERNAL SCHEMA IF NOT EXISTS spectrum_raw
FROM DATA CATALOG
DATABASE 'ecom_analytics_prod_raw'
IAM_ROLE 'arn:aws:iam::<ACCOUNT_ID>:role/ecom-analytics-prod-redshift-s3-role'
REGION 'us-east-1'
CREATE EXTERNAL DATABASE IF NOT EXISTS;

-- =============================================================================
-- EXTERNAL TABLES — Processed Zone (Parquet)
-- These tables are typically maintained by the Glue crawler.
-- Defined here explicitly for documentation and manual override purposes.
-- =============================================================================

-- Processed orders (Parquet, partitioned by year/month/day)
CREATE EXTERNAL TABLE IF NOT EXISTS spectrum_processed.orders (
    order_id            VARCHAR(36),
    order_number        VARCHAR(50),
    customer_id         VARCHAR(36),
    order_status        VARCHAR(30),
    payment_status      VARCHAR(30),
    payment_method      VARCHAR(30),
    subtotal_cents      BIGINT,
    discount_cents      BIGINT,
    shipping_cents      BIGINT,
    tax_cents           BIGINT,
    total_cents         BIGINT,
    discount_code       VARCHAR(50),
    shipping_country    CHAR(2),
    shipping_city       VARCHAR(100),
    shipping_state      VARCHAR(100),
    order_created_at    TIMESTAMP,
    confirmed_at        TIMESTAMP,
    shipped_at          TIMESTAMP,
    delivered_at        TIMESTAMP,
    item_id             BIGINT,
    product_id          VARCHAR(36),
    product_sku         VARCHAR(100),
    product_name        VARCHAR(500),
    quantity            INT,
    unit_price_cents    BIGINT,
    item_discount_cents BIGINT,
    item_total_cents    BIGINT,
    category_id         INT,
    category_name       VARCHAR(150),
    brand               VARCHAR(200)
)
PARTITIONED BY (year VARCHAR(4), month VARCHAR(2), day VARCHAR(2))
STORED AS PARQUET
LOCATION 's3://ecom-analytics-prod-data-lake-processed/orders/'
TABLE PROPERTIES ('classification'='parquet', 'compressionType'='snappy');

-- Processed events (Parquet, partitioned by year/month/day)
CREATE EXTERNAL TABLE IF NOT EXISTS spectrum_processed.events (
    event_id            VARCHAR(36),
    session_id          VARCHAR(36),
    customer_id         VARCHAR(36),
    anonymous_id        VARCHAR(36),
    event_type          VARCHAR(30),
    occurred_at         TIMESTAMP,
    page_url            VARCHAR(2048),
    page_title          VARCHAR(500),
    referrer_url        VARCHAR(2048),
    product_id          VARCHAR(36),
    order_id            VARCHAR(36),
    search_query        VARCHAR(500),
    properties_json     VARCHAR(65535)
)
PARTITIONED BY (year VARCHAR(4), month VARCHAR(2), day VARCHAR(2))
STORED AS PARQUET
LOCATION 's3://ecom-analytics-prod-data-lake-processed/events/'
TABLE PROPERTIES ('classification'='parquet', 'compressionType'='snappy');

-- Processed customers
CREATE EXTERNAL TABLE IF NOT EXISTS spectrum_processed.customers (
    customer_id         VARCHAR(36),
    email               VARCHAR(320),
    email_verified      BOOLEAN,
    first_name          VARCHAR(100),
    last_name           VARCHAR(100),
    phone               VARCHAR(30),
    country_code        CHAR(2),
    city                VARCHAR(100),
    state_province      VARCHAR(100),
    postal_code         VARCHAR(20),
    is_active           BOOLEAN,
    is_guest            BOOLEAN,
    marketing_opt_in    BOOLEAN,
    acquisition_source  VARCHAR(100),
    acquisition_medium  VARCHAR(100),
    loyalty_points      INT,
    loyalty_tier        VARCHAR(20),
    created_at          TIMESTAMP,
    updated_at          TIMESTAMP,
    last_login_at       TIMESTAMP
)
PARTITIONED BY (year VARCHAR(4), month VARCHAR(2), day VARCHAR(2))
STORED AS PARQUET
LOCATION 's3://ecom-analytics-prod-data-lake-processed/customers/'
TABLE PROPERTIES ('classification'='parquet', 'compressionType'='snappy');

-- Processed products
CREATE EXTERNAL TABLE IF NOT EXISTS spectrum_processed.products (
    product_id          VARCHAR(36),
    sku                 VARCHAR(100),
    name                VARCHAR(500),
    category_id         INT,
    category_name       VARCHAR(150),
    brand               VARCHAR(200),
    base_price_cents    INT,
    sale_price_cents    INT,
    cost_price_cents    INT,
    is_active           BOOLEAN,
    is_digital          BOOLEAN,
    review_count        INT,
    avg_rating          DOUBLE PRECISION,
    created_at          TIMESTAMP,
    updated_at          TIMESTAMP
)
PARTITIONED BY (year VARCHAR(4), month VARCHAR(2), day VARCHAR(2))
STORED AS PARQUET
LOCATION 's3://ecom-analytics-prod-data-lake-processed/products/'
TABLE PROPERTIES ('classification'='parquet', 'compressionType'='snappy');

-- =============================================================================
-- EXTERNAL TABLES — Raw Zone (CSV from RDS exports)
-- Lower-quality, not yet cleaned — used for recovery/backfill only.
-- =============================================================================

CREATE EXTERNAL TABLE IF NOT EXISTS spectrum_raw.raw_events (
    event_id            VARCHAR(36),
    session_id          VARCHAR(36),
    customer_id         VARCHAR(36),
    anonymous_id        VARCHAR(36),
    event_type          VARCHAR(30),
    occurred_at         VARCHAR(30),    -- STRING in raw CSV, parsed downstream
    page_url            VARCHAR(2048),
    page_title          VARCHAR(500),
    referrer_url        VARCHAR(2048),
    product_id          VARCHAR(36),
    order_id            VARCHAR(36),
    search_query        VARCHAR(500),
    properties_json     VARCHAR(65535)
)
PARTITIONED BY (year VARCHAR(4), month VARCHAR(2), day VARCHAR(2))
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 's3://ecom-analytics-prod-data-lake-raw/events/'
TABLE PROPERTIES (
    'classification'='csv',
    'skip.header.line.count'='1',
    'field.delim'=',',
    'quoteChar'='"'
);

-- =============================================================================
-- ADD PARTITIONS PROCEDURE
-- In production, the Glue crawler handles partition discovery automatically.
-- Use this for manual backfill or when the crawler hasn't run yet.
-- =============================================================================

-- Example: Add partition for a specific date
-- ALTER TABLE spectrum_processed.orders
-- ADD PARTITION (year='2024', month='01', day='15')
-- LOCATION 's3://ecom-analytics-prod-data-lake-processed/orders/year=2024/month=01/day=15/';

-- =============================================================================
-- VERIFICATION QUERIES
-- =============================================================================

-- Check external table definitions
-- SELECT schemaname, tablename, location, input_format
-- FROM SVV_EXTERNAL_TABLES
-- WHERE schemaname IN ('spectrum_processed', 'spectrum_raw')
-- ORDER BY schemaname, tablename;

-- Verify partition count
-- SELECT schemaname, tablename, COUNT(*) AS partition_count
-- FROM SVV_EXTERNAL_PARTITIONS
-- GROUP BY schemaname, tablename
-- ORDER BY schemaname, tablename;

-- Sample query combining Redshift internal and Spectrum external tables
-- (Demonstrates joining local warehouse data with S3 data lake)
/*
SELECT
    dc.country_code,
    dc.loyalty_tier,
    COUNT(DISTINCT sp.customer_id)  AS active_customers,
    SUM(sp.total_cents) / 100.0     AS total_revenue
FROM spectrum_processed.orders sp
JOIN dim.dim_customers dc
    ON dc.customer_id = sp.customer_id
    AND dc.is_current = TRUE
WHERE sp.year = '2024'
  AND sp.month = '01'
GROUP BY dc.country_code, dc.loyalty_tier
ORDER BY total_revenue DESC;
*/

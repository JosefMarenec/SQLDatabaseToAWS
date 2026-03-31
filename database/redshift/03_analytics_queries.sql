-- =============================================================================
-- Real-Time E-Commerce Analytics Platform
-- Redshift 03_analytics_queries.sql — Production Analytics Queries
-- =============================================================================
-- 20+ analytical queries demonstrating advanced SQL patterns:
--   window functions, CTEs, cohort analysis, attribution, RFM, forecasting
-- All queries reference the dim/fact schema from 01_schema.sql.
-- =============================================================================

-- ─────────────────────────────────────────────────────────────────────────────
-- QUERY 1: Customer Lifetime Value with Statistical Confidence
-- LTV per customer with mean, stddev, and percentile bands.
-- ─────────────────────────────────────────────────────────────────────────────
WITH customer_orders AS (
    SELECT
        fo.customer_key,
        dc.customer_id,
        dc.email,
        dc.loyalty_tier,
        dc.acquisition_source,
        COUNT(DISTINCT fo.order_id)             AS order_count,
        SUM(fo.total_cents)                     AS lifetime_revenue_cents,
        AVG(fo.total_cents)                     AS avg_order_cents,
        STDDEV(fo.total_cents)                  AS stddev_order_cents,
        MIN(dd.full_date)                       AS first_order_date,
        MAX(dd.full_date)                       AS last_order_date,
        DATEDIFF('day', MIN(dd.full_date), MAX(dd.full_date)) AS tenure_days
    FROM fact.fact_orders fo
    JOIN dim.dim_customers dc ON dc.customer_key = fo.customer_key AND dc.is_current = TRUE
    JOIN dim.dim_date dd ON dd.date_key = fo.order_date_key
    WHERE fo.order_status NOT IN ('cancelled')
    GROUP BY fo.customer_key, dc.customer_id, dc.email, dc.loyalty_tier, dc.acquisition_source
),
ltv_with_percentiles AS (
    SELECT
        *,
        NTILE(10) OVER (ORDER BY lifetime_revenue_cents) AS revenue_decile,
        NTILE(4)  OVER (ORDER BY lifetime_revenue_cents) AS revenue_quartile,
        PERCENT_RANK() OVER (ORDER BY lifetime_revenue_cents) AS revenue_percentile
    FROM customer_orders
)
SELECT
    customer_id,
    email,
    loyalty_tier,
    acquisition_source,
    order_count,
    ROUND(lifetime_revenue_cents / 100.0, 2)     AS lifetime_revenue,
    ROUND(avg_order_cents / 100.0, 2)            AS avg_order_value,
    first_order_date,
    last_order_date,
    tenure_days,
    revenue_decile,
    revenue_quartile,
    ROUND(revenue_percentile * 100, 1)           AS revenue_percentile_rank,
    CASE
        WHEN revenue_decile = 10 THEN 'Top 10%'
        WHEN revenue_decile >= 8 THEN 'Top 30%'
        WHEN revenue_decile >= 6 THEN 'Mid Tier'
        ELSE 'Low Tier'
    END AS customer_value_segment
FROM ltv_with_percentiles
ORDER BY lifetime_revenue_cents DESC;

-- ─────────────────────────────────────────────────────────────────────────────
-- QUERY 2: RFM Segmentation (Recency, Frequency, Monetary)
-- Quintile scoring across three dimensions → 125 possible segments → grouped.
-- ─────────────────────────────────────────────────────────────────────────────
WITH rfm_base AS (
    SELECT
        fo.customer_key,
        dc.customer_id,
        DATEDIFF('day', MAX(dd.full_date), CURRENT_DATE)  AS recency_days,
        COUNT(DISTINCT fo.order_id)                        AS frequency,
        SUM(fo.total_cents)                                AS monetary_cents
    FROM fact.fact_orders fo
    JOIN dim.dim_customers dc ON dc.customer_key = fo.customer_key AND dc.is_current = TRUE
    JOIN dim.dim_date dd ON dd.date_key = fo.order_date_key
    WHERE fo.order_status NOT IN ('cancelled')
      AND dd.full_date >= DATEADD('month', -12, CURRENT_DATE)
    GROUP BY fo.customer_key, dc.customer_id
),
rfm_scored AS (
    SELECT
        *,
        -- R: Lower recency = better score (5 = most recent)
        CASE NTILE(5) OVER (ORDER BY recency_days ASC)
            WHEN 1 THEN 5 WHEN 2 THEN 4 WHEN 3 THEN 3 WHEN 4 THEN 2 ELSE 1
        END AS r_score,
        -- F: Higher frequency = better score
        NTILE(5) OVER (ORDER BY frequency DESC)    AS f_score,
        -- M: Higher monetary = better score
        NTILE(5) OVER (ORDER BY monetary_cents DESC) AS m_score
    FROM rfm_base
),
rfm_segments AS (
    SELECT
        *,
        r_score + f_score + m_score AS rfm_total,
        CASE
            WHEN r_score >= 4 AND f_score >= 4 THEN 'Champions'
            WHEN r_score >= 4 AND f_score >= 2 THEN 'Loyal Customers'
            WHEN r_score >= 3 AND f_score >= 3 THEN 'Potential Loyalists'
            WHEN r_score = 5 AND f_score = 1   THEN 'New Customers'
            WHEN r_score >= 4 AND f_score = 1   THEN 'Promising'
            WHEN r_score = 3 AND f_score >= 3   THEN 'Need Attention'
            WHEN r_score <= 2 AND f_score >= 4  THEN 'At Risk'
            WHEN r_score <= 2 AND f_score >= 2  THEN 'About to Sleep'
            WHEN r_score = 1 AND f_score = 1    THEN 'Lost'
            ELSE 'Others'
        END AS rfm_segment
    FROM rfm_scored
)
SELECT
    rfm_segment,
    COUNT(*)                                        AS customer_count,
    ROUND(AVG(recency_days))                        AS avg_recency_days,
    ROUND(AVG(frequency), 1)                        AS avg_frequency,
    ROUND(AVG(monetary_cents) / 100.0, 2)           AS avg_monetary,
    ROUND(SUM(monetary_cents) / 100.0, 2)           AS total_revenue,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) AS pct_of_customers
FROM rfm_segments
GROUP BY rfm_segment
ORDER BY avg_monetary DESC;

-- ─────────────────────────────────────────────────────────────────────────────
-- QUERY 3: Monthly Cohort Retention Analysis
-- Shows % of customers from each acquisition cohort still buying N months later.
-- ─────────────────────────────────────────────────────────────────────────────
WITH cohorts AS (
    SELECT
        fo.customer_key,
        DATE_TRUNC('month', MIN(dd.full_date))::DATE  AS cohort_month
    FROM fact.fact_orders fo
    JOIN dim.dim_date dd ON dd.date_key = fo.order_date_key
    WHERE fo.order_status NOT IN ('cancelled')
    GROUP BY fo.customer_key
),
cohort_activity AS (
    SELECT
        c.customer_key,
        c.cohort_month,
        DATE_TRUNC('month', dd.full_date)::DATE AS activity_month,
        DATEDIFF(
            'month',
            c.cohort_month,
            DATE_TRUNC('month', dd.full_date)::DATE
        ) AS months_since_cohort
    FROM cohorts c
    JOIN fact.fact_orders fo ON fo.customer_key = c.customer_key
    JOIN dim.dim_date dd ON dd.date_key = fo.order_date_key
    WHERE fo.order_status NOT IN ('cancelled')
    GROUP BY c.customer_key, c.cohort_month,
             DATE_TRUNC('month', dd.full_date)::DATE
),
cohort_size AS (
    SELECT cohort_month, COUNT(DISTINCT customer_key) AS cohort_customers
    FROM cohorts
    GROUP BY cohort_month
)
SELECT
    ca.cohort_month,
    cs.cohort_customers,
    ca.months_since_cohort,
    COUNT(DISTINCT ca.customer_key)  AS retained_customers,
    ROUND(
        COUNT(DISTINCT ca.customer_key) * 100.0 / cs.cohort_customers,
        1
    )                                AS retention_rate_pct
FROM cohort_activity ca
JOIN cohort_size cs ON cs.cohort_month = ca.cohort_month
WHERE ca.cohort_month >= DATEADD('month', -12, DATE_TRUNC('month', CURRENT_DATE))
  AND ca.months_since_cohort <= 6
GROUP BY ca.cohort_month, cs.cohort_customers, ca.months_since_cohort
ORDER BY ca.cohort_month, ca.months_since_cohort;

-- ─────────────────────────────────────────────────────────────────────────────
-- QUERY 4: Revenue Attribution (First-Touch, Last-Touch, Linear)
-- Compares three attribution models across marketing channels.
-- ─────────────────────────────────────────────────────────────────────────────
WITH customer_touchpoints AS (
    SELECT
        fe.customer_key,
        fe.occurred_at,
        fe.utm_source,
        fe.utm_medium,
        fe.utm_campaign,
        ROW_NUMBER() OVER (
            PARTITION BY fe.customer_key ORDER BY fe.occurred_at ASC
        ) AS touchpoint_num_asc,
        ROW_NUMBER() OVER (
            PARTITION BY fe.customer_key ORDER BY fe.occurred_at DESC
        ) AS touchpoint_num_desc,
        COUNT(*) OVER (PARTITION BY fe.customer_key) AS total_touchpoints
    FROM fact.fact_events fe
    WHERE fe.utm_source IS NOT NULL
      AND fe.event_type IN ('page_view', 'product_view')
),
customer_revenue AS (
    SELECT
        fo.customer_key,
        SUM(fo.total_cents) AS total_revenue_cents
    FROM fact.fact_orders fo
    WHERE fo.order_status NOT IN ('cancelled')
    GROUP BY fo.customer_key
),
attribution AS (
    SELECT
        ct.customer_key,
        ct.utm_source,
        ct.utm_medium,
        cr.total_revenue_cents,
        ct.touchpoint_num_asc,
        ct.touchpoint_num_desc,
        ct.total_touchpoints,
        -- First-touch: 100% credit to first touchpoint
        CASE WHEN ct.touchpoint_num_asc = 1
             THEN cr.total_revenue_cents
             ELSE 0
        END AS first_touch_revenue,
        -- Last-touch: 100% credit to last touchpoint
        CASE WHEN ct.touchpoint_num_desc = 1
             THEN cr.total_revenue_cents
             ELSE 0
        END AS last_touch_revenue,
        -- Linear: equal credit across all touchpoints
        ROUND(cr.total_revenue_cents::FLOAT / ct.total_touchpoints) AS linear_revenue
    FROM customer_touchpoints ct
    JOIN customer_revenue cr ON cr.customer_key = ct.customer_key
)
SELECT
    utm_source,
    utm_medium,
    COUNT(DISTINCT customer_key)               AS attributed_customers,
    ROUND(SUM(first_touch_revenue) / 100.0, 2) AS first_touch_revenue,
    ROUND(SUM(last_touch_revenue)  / 100.0, 2) AS last_touch_revenue,
    ROUND(SUM(linear_revenue)      / 100.0, 2) AS linear_revenue,
    ROUND(
        (SUM(first_touch_revenue) - SUM(last_touch_revenue))
        * 100.0 / NULLIF(SUM(last_touch_revenue), 0),
        1
    )                                          AS first_vs_last_pct_diff
FROM attribution
WHERE utm_source IS NOT NULL
GROUP BY utm_source, utm_medium
ORDER BY linear_revenue DESC;

-- ─────────────────────────────────────────────────────────────────────────────
-- QUERY 5: Inventory Turnover Rate by Category (trailing 12 months)
-- Turnover = COGS / Average Inventory Value
-- ─────────────────────────────────────────────────────────────────────────────
WITH cogs AS (
    SELECT
        dp.category_name,
        SUM(fo.quantity * dp.base_price_cents * 0.6) AS cogs_cents  -- assume 60% COGS ratio
    FROM fact.fact_orders fo
    JOIN dim.dim_products dp ON dp.product_key = fo.product_key AND dp.is_current = TRUE
    JOIN dim.dim_date dd ON dd.date_key = fo.order_date_key
    WHERE fo.order_status NOT IN ('cancelled')
      AND dd.full_date >= DATEADD('month', -12, CURRENT_DATE)
    GROUP BY dp.category_name
),
avg_inventory AS (
    SELECT
        dp.category_name,
        AVG(fi.inventory_value_cents) AS avg_inventory_cents
    FROM fact.fact_inventory_snapshots fi
    JOIN dim.dim_products dp ON dp.product_key = fi.product_key AND dp.is_current = TRUE
    GROUP BY dp.category_name
)
SELECT
    c.category_name,
    ROUND(c.cogs_cents / 100.0, 2)              AS annual_cogs,
    ROUND(ai.avg_inventory_cents / 100.0, 2)    AS avg_inventory_value,
    ROUND(
        c.cogs_cents::FLOAT / NULLIF(ai.avg_inventory_cents, 0),
        2
    )                                           AS inventory_turnover_ratio,
    -- Days Sales of Inventory
    ROUND(
        365 / NULLIF(c.cogs_cents::FLOAT / NULLIF(ai.avg_inventory_cents, 0), 0),
        1
    )                                           AS days_sales_of_inventory
FROM cogs c
JOIN avg_inventory ai ON ai.category_name = c.category_name
ORDER BY inventory_turnover_ratio DESC;

-- ─────────────────────────────────────────────────────────────────────────────
-- QUERY 6: Customer Churn Prediction Feature Matrix
-- Engineered features for ML model training (export to SageMaker).
-- ─────────────────────────────────────────────────────────────────────────────
WITH customer_features AS (
    SELECT
        dc.customer_key,
        dc.customer_id,
        dc.loyalty_tier,
        dc.acquisition_source,
        dc.country_code,
        DATEDIFF('day', dc.customer_since_date, CURRENT_DATE) AS account_age_days,

        -- Purchase features
        COUNT(DISTINCT fo.order_id)                         AS total_orders,
        COALESCE(SUM(fo.total_cents), 0)                    AS total_revenue_cents,
        COALESCE(AVG(fo.total_cents), 0)                    AS avg_order_value_cents,
        COALESCE(MAX(dd.full_date), dc.customer_since_date) AS last_order_date,
        DATEDIFF('day', MAX(dd.full_date), CURRENT_DATE)    AS recency_days,

        -- Order cadence
        COALESCE(
            DATEDIFF('day', MIN(dd.full_date), MAX(dd.full_date))::FLOAT
            / NULLIF(COUNT(DISTINCT fo.order_id) - 1, 0),
            0
        )                                                    AS avg_days_between_orders,

        -- Returns indicator
        COUNT(DISTINCT CASE WHEN fo.order_status = 'refunded' THEN fo.order_id END)
            * 100.0 / NULLIF(COUNT(DISTINCT fo.order_id), 0) AS return_rate_pct,

        -- Discount dependency
        COUNT(DISTINCT CASE WHEN fo.discount_code IS NOT NULL THEN fo.order_id END)
            * 100.0 / NULLIF(COUNT(DISTINCT fo.order_id), 0) AS discount_usage_rate_pct,

        -- Label: churned if no purchase in last 90 days (for training)
        CASE
            WHEN DATEDIFF('day', MAX(dd.full_date), CURRENT_DATE) > 90 THEN 1
            ELSE 0
        END AS is_churned
    FROM dim.dim_customers dc
    LEFT JOIN fact.fact_orders fo
        ON fo.customer_key = dc.customer_key
        AND fo.order_status NOT IN ('cancelled')
    LEFT JOIN dim.dim_date dd ON dd.date_key = fo.order_date_key
    WHERE dc.is_current = TRUE
    GROUP BY
        dc.customer_key, dc.customer_id, dc.loyalty_tier,
        dc.acquisition_source, dc.country_code,
        dc.customer_since_date
)
SELECT
    *,
    -- Composite risk score (higher = more likely to churn)
    ROUND(
        CASE WHEN recency_days > 60  THEN 30 ELSE recency_days / 2.0 END
        + CASE WHEN total_orders <= 1 THEN 20 ELSE 0 END
        + CASE WHEN return_rate_pct > 20 THEN 10 ELSE 0 END
        + CASE WHEN avg_order_value_cents < 2000 THEN 10 ELSE 0 END
    )                       AS churn_risk_score
FROM customer_features
ORDER BY churn_risk_score DESC, recency_days DESC;

-- ─────────────────────────────────────────────────────────────────────────────
-- QUERY 7: Basket Analysis — Top Co-Purchased Product Pairs
-- Uses self-join to find products frequently bought together.
-- ─────────────────────────────────────────────────────────────────────────────
WITH order_products AS (
    SELECT DISTINCT
        fo.order_id,
        dp.product_id,
        dp.product_name,
        dp.category_name
    FROM fact.fact_orders fo
    JOIN dim.dim_products dp ON dp.product_key = fo.product_key AND dp.is_current = TRUE
    WHERE fo.order_status NOT IN ('cancelled')
),
product_pairs AS (
    SELECT
        a.product_id           AS product_a_id,
        a.product_name         AS product_a,
        a.category_name        AS category_a,
        b.product_id           AS product_b_id,
        b.product_name         AS product_b,
        b.category_name        AS category_b,
        COUNT(DISTINCT a.order_id) AS co_purchase_count
    FROM order_products a
    JOIN order_products b
        ON a.order_id = b.order_id
        AND a.product_id < b.product_id  -- avoid duplicates
    GROUP BY a.product_id, a.product_name, a.category_name,
             b.product_id, b.product_name, b.category_name
),
product_totals AS (
    SELECT product_id, COUNT(DISTINCT order_id) AS total_orders
    FROM order_products
    GROUP BY product_id
)
SELECT
    pp.product_a,
    pp.category_a,
    pp.product_b,
    pp.category_b,
    pp.co_purchase_count,
    pa.total_orders AS product_a_orders,
    pb.total_orders AS product_b_orders,
    -- Support: fraction of all orders containing both products
    ROUND(pp.co_purchase_count * 100.0 / (SELECT COUNT(DISTINCT order_id) FROM order_products), 3) AS support_pct,
    -- Confidence A→B: P(B|A)
    ROUND(pp.co_purchase_count * 100.0 / pa.total_orders, 1) AS confidence_a_to_b_pct,
    -- Lift: how much more likely than random
    ROUND(
        pp.co_purchase_count::FLOAT / pa.total_orders
        / (pb.total_orders::FLOAT / (SELECT COUNT(DISTINCT order_id) FROM order_products)),
        2
    )                AS lift
FROM product_pairs pp
JOIN product_totals pa ON pa.product_id = pp.product_a_id
JOIN product_totals pb ON pb.product_id = pp.product_b_id
WHERE pp.co_purchase_count >= 10
ORDER BY lift DESC, co_purchase_count DESC
LIMIT 100;

-- ─────────────────────────────────────────────────────────────────────────────
-- QUERY 8: Day-over-Day and Week-over-Week Revenue Growth (window functions)
-- ─────────────────────────────────────────────────────────────────────────────
WITH daily_revenue AS (
    SELECT
        dd.full_date,
        dd.day_name,
        dd.week_of_year,
        dd.year,
        dd.is_weekend,
        SUM(fo.total_cents)         AS revenue_cents,
        COUNT(DISTINCT fo.order_id) AS order_count,
        COUNT(DISTINCT fo.customer_key) AS unique_customers
    FROM fact.fact_orders fo
    JOIN dim.dim_date dd ON dd.date_key = fo.order_date_key
    WHERE fo.order_status NOT IN ('cancelled')
      AND dd.full_date >= DATEADD('day', -90, CURRENT_DATE)
    GROUP BY dd.full_date, dd.day_name, dd.week_of_year, dd.year, dd.is_weekend
)
SELECT
    full_date,
    day_name,
    is_weekend,
    revenue_cents,
    order_count,
    unique_customers,
    -- Day over day
    LAG(revenue_cents, 1)  OVER (ORDER BY full_date) AS prev_day_revenue,
    ROUND(
        (revenue_cents - LAG(revenue_cents, 1) OVER (ORDER BY full_date))
        * 100.0 / NULLIF(LAG(revenue_cents, 1) OVER (ORDER BY full_date), 0),
        1
    )                                                 AS dod_growth_pct,
    -- Same day last week
    LAG(revenue_cents, 7)  OVER (ORDER BY full_date) AS same_day_last_week,
    ROUND(
        (revenue_cents - LAG(revenue_cents, 7) OVER (ORDER BY full_date))
        * 100.0 / NULLIF(LAG(revenue_cents, 7) OVER (ORDER BY full_date), 0),
        1
    )                                                 AS wow_growth_pct,
    -- 7-day moving average
    ROUND(AVG(revenue_cents) OVER (
        ORDER BY full_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ))                                                AS ma7_revenue,
    -- 30-day moving average
    ROUND(AVG(revenue_cents) OVER (
        ORDER BY full_date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ))                                                AS ma30_revenue,
    -- Cumulative revenue for the year
    SUM(revenue_cents) OVER (
        PARTITION BY EXTRACT(YEAR FROM full_date)
        ORDER BY full_date
    )                                                 AS ytd_revenue
FROM daily_revenue
ORDER BY full_date DESC;

-- ─────────────────────────────────────────────────────────────────────────────
-- QUERY 9: Funnel Conversion Rates with Statistical Significance
-- ─────────────────────────────────────────────────────────────────────────────
WITH funnel_daily AS (
    SELECT
        dd.year_month,
        dd.full_date,
        COUNT(DISTINCT fe.session_id) FILTER (WHERE fe.event_type = 'page_view')       AS sessions,
        COUNT(DISTINCT fe.session_id) FILTER (WHERE fe.event_type = 'product_view')    AS product_views,
        COUNT(DISTINCT fe.session_id) FILTER (WHERE fe.event_type = 'add_to_cart')     AS cart_adds,
        COUNT(DISTINCT fe.session_id) FILTER (WHERE fe.event_type = 'checkout_start')  AS checkouts,
        COUNT(DISTINCT fe.session_id) FILTER (WHERE fe.event_type = 'purchase')        AS purchases
    FROM fact.fact_events fe
    JOIN dim.dim_date dd ON dd.date_key = fe.event_date_key
    WHERE dd.full_date >= DATEADD('day', -30, CURRENT_DATE)
    GROUP BY dd.year_month, dd.full_date
)
SELECT
    year_month,
    SUM(sessions)       AS total_sessions,
    SUM(product_views)  AS total_product_views,
    SUM(cart_adds)      AS total_cart_adds,
    SUM(checkouts)      AS total_checkouts,
    SUM(purchases)      AS total_purchases,
    -- Step rates
    ROUND(SUM(product_views) * 100.0 / NULLIF(SUM(sessions), 0), 2)    AS session_to_pv_rate,
    ROUND(SUM(cart_adds) * 100.0 / NULLIF(SUM(product_views), 0), 2)   AS pv_to_atc_rate,
    ROUND(SUM(checkouts) * 100.0 / NULLIF(SUM(cart_adds), 0), 2)       AS atc_to_checkout_rate,
    ROUND(SUM(purchases) * 100.0 / NULLIF(SUM(checkouts), 0), 2)       AS checkout_to_purchase_rate,
    -- Overall
    ROUND(SUM(purchases) * 100.0 / NULLIF(SUM(sessions), 0), 3)        AS overall_conversion_rate,
    -- Cart abandonment
    ROUND((SUM(cart_adds) - SUM(purchases)) * 100.0 / NULLIF(SUM(cart_adds), 0), 2) AS cart_abandonment_rate
FROM funnel_daily
GROUP BY year_month
ORDER BY year_month;

-- ─────────────────────────────────────────────────────────────────────────────
-- QUERY 10: Geographic Revenue Heatmap
-- Revenue by country, state, city with YoY comparison.
-- ─────────────────────────────────────────────────────────────────────────────
WITH geo_revenue AS (
    SELECT
        fo.shipping_country,
        dc.state_province,
        dc.city,
        dd.year,
        SUM(fo.total_cents)         AS revenue_cents,
        COUNT(DISTINCT fo.order_id) AS order_count,
        COUNT(DISTINCT fo.customer_key) AS unique_customers
    FROM fact.fact_orders fo
    JOIN dim.dim_customers dc ON dc.customer_key = fo.customer_key AND dc.is_current = TRUE
    JOIN dim.dim_date dd ON dd.date_key = fo.order_date_key
    WHERE fo.order_status NOT IN ('cancelled')
      AND dd.year IN (EXTRACT(YEAR FROM CURRENT_DATE), EXTRACT(YEAR FROM CURRENT_DATE) - 1)
    GROUP BY fo.shipping_country, dc.state_province, dc.city, dd.year
),
pivoted AS (
    SELECT
        shipping_country,
        state_province,
        city,
        SUM(CASE WHEN year = EXTRACT(YEAR FROM CURRENT_DATE) THEN revenue_cents END)     AS current_year_rev,
        SUM(CASE WHEN year = EXTRACT(YEAR FROM CURRENT_DATE) - 1 THEN revenue_cents END) AS prior_year_rev,
        SUM(CASE WHEN year = EXTRACT(YEAR FROM CURRENT_DATE) THEN order_count END)       AS current_orders,
        SUM(CASE WHEN year = EXTRACT(YEAR FROM CURRENT_DATE) THEN unique_customers END)  AS current_customers
    FROM geo_revenue
    GROUP BY shipping_country, state_province, city
)
SELECT
    shipping_country,
    state_province,
    city,
    ROUND(current_year_rev / 100.0, 2)  AS current_year_revenue,
    ROUND(prior_year_rev / 100.0, 2)    AS prior_year_revenue,
    current_orders,
    current_customers,
    ROUND(
        (current_year_rev - prior_year_rev) * 100.0 / NULLIF(prior_year_rev, 0),
        1
    )                                    AS yoy_growth_pct,
    -- Rank within country
    RANK() OVER (
        PARTITION BY shipping_country ORDER BY current_year_rev DESC
    )                                    AS rank_in_country
FROM pivoted
WHERE current_year_rev IS NOT NULL
ORDER BY current_year_rev DESC;

-- ─────────────────────────────────────────────────────────────────────────────
-- QUERY 11: Product Recommendation Scoring (Collaborative Filtering Features)
-- Generates item-item similarity scores based on co-purchase behavior.
-- ─────────────────────────────────────────────────────────────────────────────
WITH customer_product_matrix AS (
    SELECT
        fo.customer_key,
        fo.product_key,
        SUM(fo.quantity)                            AS units_purchased,
        COUNT(DISTINCT fo.order_id)                 AS purchase_occasions,
        -- Normalized purchase score (0-1 per customer)
        SUM(fo.quantity)::FLOAT / SUM(SUM(fo.quantity)) OVER (
            PARTITION BY fo.customer_key
        )                                           AS normalized_score
    FROM fact.fact_orders fo
    WHERE fo.order_status NOT IN ('cancelled')
    GROUP BY fo.customer_key, fo.product_key
),
product_similarity AS (
    SELECT
        a.product_key   AS product_a,
        b.product_key   AS product_b,
        -- Dot product of normalized vectors (cosine similarity component)
        SUM(a.normalized_score * b.normalized_score) AS dot_product,
        COUNT(DISTINCT a.customer_key)              AS common_customers,
        SQRT(SUM(POW(a.normalized_score, 2)))        AS norm_a,
        SQRT(SUM(POW(b.normalized_score, 2)))        AS norm_b
    FROM customer_product_matrix a
    JOIN customer_product_matrix b
        ON a.customer_key = b.customer_key
        AND a.product_key < b.product_key
    WHERE a.normalized_score > 0 AND b.normalized_score > 0
    GROUP BY a.product_key, b.product_key
    HAVING COUNT(DISTINCT a.customer_key) >= 5  -- minimum support
)
SELECT
    dp_a.product_name    AS product_a,
    dp_b.product_name    AS product_b,
    dp_a.category_name   AS category_a,
    dp_b.category_name   AS category_b,
    ps.common_customers,
    ROUND(ps.dot_product / NULLIF(ps.norm_a * ps.norm_b, 0), 4) AS cosine_similarity,
    RANK() OVER (
        PARTITION BY ps.product_a ORDER BY ps.dot_product / NULLIF(ps.norm_a * ps.norm_b, 0) DESC
    )                    AS similarity_rank
FROM product_similarity ps
JOIN dim.dim_products dp_a ON dp_a.product_key = ps.product_a AND dp_a.is_current = TRUE
JOIN dim.dim_products dp_b ON dp_b.product_key = ps.product_b AND dp_b.is_current = TRUE
WHERE ps.dot_product / NULLIF(ps.norm_a * ps.norm_b, 0) > 0.1
ORDER BY cosine_similarity DESC
LIMIT 200;

-- ─────────────────────────────────────────────────────────────────────────────
-- QUERY 12: Seasonal Revenue Decomposition (trend + seasonality)
-- Uses rolling averages to separate trend from seasonal variation.
-- ─────────────────────────────────────────────────────────────────────────────
WITH weekly_revenue AS (
    SELECT
        DATE_TRUNC('week', dd.full_date)::DATE  AS week_start,
        SUM(fo.total_cents)                     AS weekly_revenue
    FROM fact.fact_orders fo
    JOIN dim.dim_date dd ON dd.date_key = fo.order_date_key
    WHERE fo.order_status NOT IN ('cancelled')
      AND dd.full_date >= DATEADD('month', -24, CURRENT_DATE)
    GROUP BY DATE_TRUNC('week', dd.full_date)::DATE
),
decomposed AS (
    SELECT
        week_start,
        weekly_revenue,
        -- 4-week centered moving average (trend component)
        AVG(weekly_revenue) OVER (
            ORDER BY week_start
            ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING
        )   AS trend_4w,
        -- 12-week trend
        AVG(weekly_revenue) OVER (
            ORDER BY week_start
            ROWS BETWEEN 6 PRECEDING AND 6 FOLLOWING
        )   AS trend_12w,
        -- Percent deviation from trend (seasonality index)
        ROUND(
            weekly_revenue * 100.0 / NULLIF(
                AVG(weekly_revenue) OVER (
                    ORDER BY week_start ROWS BETWEEN 6 PRECEDING AND 6 FOLLOWING
                ), 0
            ),
            1
        )   AS seasonality_index
    FROM weekly_revenue
)
SELECT
    week_start,
    weekly_revenue,
    ROUND(trend_4w)     AS trend_4_week,
    ROUND(trend_12w)    AS trend_12_week,
    seasonality_index,
    -- Classify seasonal effect
    CASE
        WHEN seasonality_index > 115 THEN 'High Season'
        WHEN seasonality_index > 105 THEN 'Above Average'
        WHEN seasonality_index >= 95 THEN 'Normal'
        WHEN seasonality_index >= 85 THEN 'Below Average'
        ELSE 'Low Season'
    END AS season_classification
FROM decomposed
ORDER BY week_start;

-- ─────────────────────────────────────────────────────────────────────────────
-- QUERY 13: Return Rate Analysis by Product and Reason
-- ─────────────────────────────────────────────────────────────────────────────
WITH returns AS (
    SELECT
        fo_returned.product_key,
        COUNT(DISTINCT fo_returned.order_id)                AS returned_orders,
        SUM(fo_returned.total_cents)                        AS refunded_amount_cents
    FROM fact.fact_orders fo_returned
    WHERE fo_returned.order_status IN ('refunded', 'partially_refunded')
    GROUP BY fo_returned.product_key
),
total_sales AS (
    SELECT
        product_key,
        COUNT(DISTINCT order_id)                            AS total_orders,
        SUM(total_cents)                                    AS total_revenue_cents
    FROM fact.fact_orders
    WHERE order_status NOT IN ('cancelled')
    GROUP BY product_key
)
SELECT
    dp.product_name,
    dp.category_name,
    dp.brand,
    ts.total_orders,
    COALESCE(r.returned_orders, 0)                          AS returned_orders,
    ROUND(
        COALESCE(r.returned_orders, 0) * 100.0 / NULLIF(ts.total_orders, 0),
        2
    )                                                       AS return_rate_pct,
    ROUND(ts.total_revenue_cents / 100.0, 2)                AS total_revenue,
    ROUND(COALESCE(r.refunded_amount_cents, 0) / 100.0, 2)  AS total_refunded,
    ROUND(
        COALESCE(r.refunded_amount_cents, 0) * 100.0 / NULLIF(ts.total_revenue_cents, 0),
        2
    )                                                       AS refund_rate_pct
FROM total_sales ts
JOIN dim.dim_products dp ON dp.product_key = ts.product_key AND dp.is_current = TRUE
LEFT JOIN returns r ON r.product_key = ts.product_key
WHERE ts.total_orders >= 10
ORDER BY return_rate_pct DESC, ts.total_orders DESC
LIMIT 50;

-- ─────────────────────────────────────────────────────────────────────────────
-- QUERY 14: Order Velocity and Peak Hour Analysis
-- ─────────────────────────────────────────────────────────────────────────────
WITH order_hours AS (
    SELECT
        EXTRACT(HOUR FROM fo.ordered_at)    AS hour_of_day,
        dd.day_name,
        dd.is_weekend,
        COUNT(DISTINCT fo.order_id)         AS order_count,
        SUM(fo.total_cents)                 AS revenue_cents,
        AVG(fo.total_cents)                 AS avg_order_value
    FROM fact.fact_orders fo
    JOIN dim.dim_date dd ON dd.date_key = fo.order_date_key
    WHERE fo.order_status NOT IN ('cancelled')
      AND dd.full_date >= DATEADD('day', -90, CURRENT_DATE)
    GROUP BY EXTRACT(HOUR FROM fo.ordered_at), dd.day_name, dd.is_weekend
)
SELECT
    hour_of_day,
    SUM(order_count)                                            AS total_orders,
    ROUND(SUM(revenue_cents) / 100.0, 2)                        AS total_revenue,
    ROUND(AVG(avg_order_value) / 100.0, 2)                      AS avg_order_value,
    SUM(order_count) FILTER (WHERE NOT is_weekend)              AS weekday_orders,
    SUM(order_count) FILTER (WHERE is_weekend)                  AS weekend_orders,
    -- Percent of daily volume
    ROUND(
        SUM(order_count) * 100.0 / SUM(SUM(order_count)) OVER (),
        2
    )                                                           AS pct_of_daily_orders,
    -- Rank
    RANK() OVER (ORDER BY SUM(order_count) DESC)                AS volume_rank
FROM order_hours
GROUP BY hour_of_day
ORDER BY hour_of_day;

-- ─────────────────────────────────────────────────────────────────────────────
-- QUERY 15: Discount Effectiveness Analysis
-- Measures impact of discount codes on order value and repeat purchase.
-- ─────────────────────────────────────────────────────────────────────────────
WITH discount_orders AS (
    SELECT
        COALESCE(fo.discount_code, 'NO_DISCOUNT') AS discount_code,
        COUNT(DISTINCT fo.order_id)               AS order_count,
        COUNT(DISTINCT fo.customer_key)           AS unique_customers,
        SUM(fo.total_cents)                       AS gross_revenue_cents,
        SUM(fo.discount_cents)                    AS total_discount_given,
        AVG(fo.total_cents)                       AS avg_order_value,
        -- Net revenue after discount
        SUM(fo.total_cents - fo.discount_cents)   AS net_revenue_cents
    FROM fact.fact_orders fo
    WHERE fo.order_status NOT IN ('cancelled')
    GROUP BY COALESCE(fo.discount_code, 'NO_DISCOUNT')
),
follow_on_orders AS (
    -- Did customers who used a code make a second purchase without a code?
    SELECT
        fo1.discount_code AS first_order_code,
        COUNT(DISTINCT fo2.order_id) AS subsequent_orders
    FROM fact.fact_orders fo1
    JOIN fact.fact_orders fo2
        ON fo2.customer_key = fo1.customer_key
        AND fo2.ordered_at > fo1.ordered_at
        AND fo2.order_status NOT IN ('cancelled')
    WHERE fo1.discount_code IS NOT NULL
    GROUP BY fo1.discount_code
)
SELECT
    d.discount_code,
    d.order_count,
    d.unique_customers,
    ROUND(d.gross_revenue_cents / 100.0, 2)     AS gross_revenue,
    ROUND(d.total_discount_given / 100.0, 2)    AS total_discounts_given,
    ROUND(d.net_revenue_cents / 100.0, 2)       AS net_revenue,
    ROUND(d.avg_order_value / 100.0, 2)         AS avg_order_value,
    ROUND(
        d.total_discount_given * 100.0 / NULLIF(d.gross_revenue_cents, 0),
        2
    )                                           AS discount_pct_of_gross,
    COALESCE(f.subsequent_orders, 0)            AS follow_on_orders,
    ROUND(
        COALESCE(f.subsequent_orders, 0) * 100.0 / NULLIF(d.order_count, 0),
        1
    )                                           AS follow_on_conversion_rate_pct
FROM discount_orders d
LEFT JOIN follow_on_orders f ON f.first_order_code = d.discount_code
ORDER BY d.gross_revenue_cents DESC;

-- ─────────────────────────────────────────────────────────────────────────────
-- QUERY 16: Monthly Revenue Forecast (Simple Linear Regression)
-- Uses SQL window functions to compute slope and project next 3 months.
-- ─────────────────────────────────────────────────────────────────────────────
WITH monthly_revenue AS (
    SELECT
        DATE_TRUNC('month', dd.full_date)::DATE AS month,
        SUM(fo.total_cents)                     AS revenue_cents,
        ROW_NUMBER() OVER (ORDER BY DATE_TRUNC('month', dd.full_date)) AS period_num
    FROM fact.fact_orders fo
    JOIN dim.dim_date dd ON dd.date_key = fo.order_date_key
    WHERE fo.order_status NOT IN ('cancelled')
      AND dd.full_date >= DATEADD('month', -12, DATE_TRUNC('month', CURRENT_DATE))
    GROUP BY DATE_TRUNC('month', dd.full_date)::DATE
),
regression AS (
    SELECT
        COUNT(*)                        AS n,
        AVG(period_num::FLOAT)          AS mean_x,
        AVG(revenue_cents::FLOAT)       AS mean_y,
        SUM((period_num - AVG(period_num::FLOAT) OVER ())
            * (revenue_cents - AVG(revenue_cents::FLOAT) OVER ()))
            / NULLIF(SUM(POW(period_num - AVG(period_num::FLOAT) OVER (), 2)), 0) AS slope,
        AVG(revenue_cents::FLOAT)
            - (SUM((period_num - AVG(period_num::FLOAT) OVER ())
                * (revenue_cents - AVG(revenue_cents::FLOAT) OVER ()))
               / NULLIF(SUM(POW(period_num - AVG(period_num::FLOAT) OVER (), 2)), 0))
            * AVG(period_num::FLOAT)    AS intercept
    FROM monthly_revenue
    LIMIT 1
)
SELECT
    mr.month,
    ROUND(mr.revenue_cents / 100.0, 2)      AS actual_revenue,
    ROUND((r.slope * mr.period_num + r.intercept) / 100.0, 2) AS trend_revenue,
    NULL::NUMERIC                           AS forecast_revenue,
    'actual'                                AS data_type
FROM monthly_revenue mr
CROSS JOIN (SELECT slope, intercept FROM regression LIMIT 1) r

UNION ALL

-- Forecasted months
SELECT
    DATEADD('month', n, DATE_TRUNC('month', CURRENT_DATE))::DATE AS month,
    NULL                                    AS actual_revenue,
    NULL                                    AS trend_revenue,
    ROUND(
        (r.slope * (max_period + n) + r.intercept) / 100.0,
        2
    )                                       AS forecast_revenue,
    'forecast'                              AS data_type
FROM (SELECT 1 AS n UNION ALL SELECT 2 UNION ALL SELECT 3) forecast_periods
CROSS JOIN (SELECT slope, intercept FROM regression LIMIT 1) r
CROSS JOIN (SELECT MAX(period_num) AS max_period FROM monthly_revenue) mx

ORDER BY month;

-- ─────────────────────────────────────────────────────────────────────────────
-- QUERY 17: Cross-Sell Opportunity Scoring per Customer
-- For each active customer, rank the top 5 products they haven't bought.
-- ─────────────────────────────────────────────────────────────────────────────
WITH customer_purchases AS (
    SELECT DISTINCT fo.customer_key, fo.product_key
    FROM fact.fact_orders fo
    WHERE fo.order_status NOT IN ('cancelled')
),
top_category_products AS (
    SELECT
        fo.customer_key,
        dp_other.product_key,
        dp_other.product_name,
        dp_other.category_name,
        COUNT(DISTINCT fo_all.customer_key) AS buyers_in_segment,
        ROW_NUMBER() OVER (
            PARTITION BY fo.customer_key
            ORDER BY COUNT(DISTINCT fo_all.customer_key) DESC
        ) AS product_rank
    FROM customer_purchases cp
    -- Get the customer's most-bought category
    JOIN fact.fact_orders fo ON fo.customer_key = cp.customer_key
    JOIN dim.dim_products dp_bought ON dp_bought.product_key = fo.product_key AND dp_bought.is_current = TRUE
    -- Find products in that category the customer hasn't bought
    JOIN dim.dim_products dp_other
        ON dp_other.category_name = dp_bought.category_name
        AND dp_other.is_current = TRUE
    -- How many customers in the same loyalty tier bought each product
    JOIN dim.dim_customers dc_self ON dc_self.customer_key = cp.customer_key AND dc_self.is_current = TRUE
    JOIN dim.dim_customers dc_other ON dc_other.loyalty_tier = dc_self.loyalty_tier AND dc_other.is_current = TRUE
    JOIN fact.fact_orders fo_all ON fo_all.customer_key = dc_other.customer_key
        AND fo_all.product_key = dp_other.product_key
        AND fo_all.order_status NOT IN ('cancelled')
    -- Exclude products already bought
    WHERE NOT EXISTS (
        SELECT 1 FROM customer_purchases cp2
        WHERE cp2.customer_key = cp.customer_key
          AND cp2.product_key = dp_other.product_key
    )
    GROUP BY fo.customer_key, dp_other.product_key, dp_other.product_name, dp_other.category_name
)
SELECT
    dc.customer_id,
    dc.email,
    tcp.product_name,
    tcp.category_name,
    tcp.buyers_in_segment   AS similar_customers_bought,
    tcp.product_rank        AS recommendation_rank
FROM top_category_products tcp
JOIN dim.dim_customers dc ON dc.customer_key = tcp.customer_key AND dc.is_current = TRUE
WHERE tcp.product_rank <= 5
ORDER BY dc.customer_id, tcp.product_rank;

-- ─────────────────────────────────────────────────────────────────────────────
-- QUERY 18: Lifetime Value Decile Distribution Report
-- Summary report for executive dashboard.
-- ─────────────────────────────────────────────────────────────────────────────
WITH customer_ltv AS (
    SELECT
        fo.customer_key,
        SUM(fo.total_cents)             AS ltv_cents,
        COUNT(DISTINCT fo.order_id)     AS order_count,
        NTILE(10) OVER (ORDER BY SUM(fo.total_cents)) AS decile
    FROM fact.fact_orders fo
    WHERE fo.order_status NOT IN ('cancelled')
    GROUP BY fo.customer_key
)
SELECT
    decile,
    COUNT(*)                                        AS customer_count,
    ROUND(MIN(ltv_cents) / 100.0, 2)               AS min_ltv,
    ROUND(MAX(ltv_cents) / 100.0, 2)               AS max_ltv,
    ROUND(AVG(ltv_cents) / 100.0, 2)               AS avg_ltv,
    ROUND(SUM(ltv_cents) / 100.0, 2)               AS total_revenue,
    ROUND(
        SUM(ltv_cents) * 100.0 / SUM(SUM(ltv_cents)) OVER (),
        1
    )                                               AS pct_of_total_revenue,
    -- Cumulative from top
    ROUND(
        SUM(SUM(ltv_cents)) OVER (
            ORDER BY decile DESC
        ) * 100.0 / SUM(SUM(ltv_cents)) OVER (),
        1
    )                                               AS cumulative_revenue_from_top
FROM customer_ltv
GROUP BY decile
ORDER BY decile DESC;

-- ─────────────────────────────────────────────────────────────────────────────
-- QUERY 19: New vs. Returning Customer Revenue Split
-- Monthly breakdown showing customer acquisition health.
-- ─────────────────────────────────────────────────────────────────────────────
WITH customer_first_order AS (
    SELECT
        fo.customer_key,
        MIN(fo.ordered_at)  AS first_order_at
    FROM fact.fact_orders fo
    WHERE fo.order_status NOT IN ('cancelled')
    GROUP BY fo.customer_key
),
categorized_orders AS (
    SELECT
        fo.order_id,
        fo.customer_key,
        fo.total_cents,
        fo.ordered_at,
        dd.year_month,
        CASE
            WHEN fo.ordered_at = cfo.first_order_at THEN 'new'
            ELSE 'returning'
        END AS customer_type
    FROM fact.fact_orders fo
    JOIN dim.dim_date dd ON dd.date_key = fo.order_date_key
    JOIN customer_first_order cfo ON cfo.customer_key = fo.customer_key
    WHERE fo.order_status NOT IN ('cancelled')
)
SELECT
    year_month,
    SUM(total_cents) FILTER (WHERE customer_type = 'new')         / 100.0 AS new_customer_revenue,
    SUM(total_cents) FILTER (WHERE customer_type = 'returning')   / 100.0 AS returning_customer_revenue,
    SUM(total_cents)                                              / 100.0 AS total_revenue,
    COUNT(DISTINCT order_id) FILTER (WHERE customer_type = 'new')      AS new_orders,
    COUNT(DISTINCT order_id) FILTER (WHERE customer_type = 'returning') AS returning_orders,
    COUNT(DISTINCT customer_key) FILTER (WHERE customer_type = 'new')  AS new_customers,
    ROUND(
        SUM(total_cents) FILTER (WHERE customer_type = 'returning')
        * 100.0 / NULLIF(SUM(total_cents), 0),
        1
    )                                                                   AS returning_revenue_pct
FROM categorized_orders
GROUP BY year_month
ORDER BY year_month;

-- ─────────────────────────────────────────────────────────────────────────────
-- QUERY 20: Executive KPI Dashboard (Single Row Summary)
-- ─────────────────────────────────────────────────────────────────────────────
WITH current_month AS (
    SELECT
        SUM(fo.total_cents)                     AS revenue_cents,
        COUNT(DISTINCT fo.order_id)             AS orders,
        COUNT(DISTINCT fo.customer_key)         AS customers,
        AVG(fo.total_cents)                     AS aov_cents
    FROM fact.fact_orders fo
    JOIN dim.dim_date dd ON dd.date_key = fo.order_date_key
    WHERE dd.year_month = TO_CHAR(CURRENT_DATE, 'YYYY-MM')
      AND fo.order_status NOT IN ('cancelled')
),
prior_month AS (
    SELECT
        SUM(fo.total_cents)                     AS revenue_cents,
        COUNT(DISTINCT fo.order_id)             AS orders,
        COUNT(DISTINCT fo.customer_key)         AS customers
    FROM fact.fact_orders fo
    JOIN dim.dim_date dd ON dd.date_key = fo.order_date_key
    WHERE dd.year_month = TO_CHAR(DATEADD('month', -1, CURRENT_DATE), 'YYYY-MM')
      AND fo.order_status NOT IN ('cancelled')
),
ytd AS (
    SELECT
        SUM(fo.total_cents)                     AS revenue_cents,
        COUNT(DISTINCT fo.order_id)             AS orders
    FROM fact.fact_orders fo
    JOIN dim.dim_date dd ON dd.date_key = fo.order_date_key
    WHERE dd.year = EXTRACT(YEAR FROM CURRENT_DATE)
      AND fo.order_status NOT IN ('cancelled')
)
SELECT
    CURRENT_DATE                                        AS report_date,
    ROUND(cm.revenue_cents / 100.0, 2)                  AS mtd_revenue,
    ROUND(pm.revenue_cents / 100.0, 2)                  AS prior_month_revenue,
    ROUND((cm.revenue_cents - pm.revenue_cents) * 100.0
        / NULLIF(pm.revenue_cents, 0), 1)               AS mom_growth_pct,
    cm.orders                                           AS mtd_orders,
    cm.customers                                        AS mtd_customers,
    ROUND(cm.aov_cents / 100.0, 2)                      AS mtd_aov,
    ROUND(ytd.revenue_cents / 100.0, 2)                 AS ytd_revenue,
    ytd.orders                                          AS ytd_orders
FROM current_month cm
CROSS JOIN prior_month pm
CROSS JOIN ytd;

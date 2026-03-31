#!/usr/bin/env python3
"""
run_analytics.py — Connects to Redshift, runs key analytics queries, and prints results.

Usage:
    python scripts/run_analytics.py --host <REDSHIFT_HOST> --password <PASSWORD>
    python scripts/run_analytics.py --host <REDSHIFT_HOST> --password <PASSWORD> --export

Requires: psycopg2-binary, tabulate
"""

import argparse
import csv
import os
import textwrap
from datetime import datetime

import psycopg2
import psycopg2.extras
from tabulate import tabulate

# ── Query Library ──────────────────────────────────────────────────────────────

QUERIES = [
    {
        "name": "Monthly Revenue (Last 12 Months)",
        "description": "Total revenue, orders, and AOV by month",
        "sql": """
            SELECT
                d.year,
                d.month_name,
                COUNT(DISTINCT fo.order_id)            AS total_orders,
                SUM(fo.revenue)                        AS total_revenue,
                ROUND(SUM(fo.revenue) /
                      NULLIF(COUNT(DISTINCT fo.order_id), 0), 2) AS avg_order_value,
                LAG(SUM(fo.revenue)) OVER (ORDER BY d.year, d.month_num) AS prev_month_revenue,
                ROUND(
                    (SUM(fo.revenue) - LAG(SUM(fo.revenue)) OVER (ORDER BY d.year, d.month_num))
                    / NULLIF(LAG(SUM(fo.revenue)) OVER (ORDER BY d.year, d.month_num), 0) * 100,
                2) AS mom_growth_pct
            FROM fact_orders fo
            JOIN dim_date d ON fo.order_date_key = d.date_key
            WHERE d.full_date >= DATEADD(month, -12, CURRENT_DATE)
              AND fo.status NOT IN ('cancelled', 'refunded')
            GROUP BY d.year, d.month_name, d.month_num
            ORDER BY d.year, d.month_num
        """,
    },
    {
        "name": "RFM Customer Segmentation",
        "description": "Recency / Frequency / Monetary segmentation",
        "sql": """
            WITH rfm_raw AS (
                SELECT
                    fo.customer_key,
                    DATEDIFF('day', MAX(d.full_date), CURRENT_DATE)  AS recency_days,
                    COUNT(DISTINCT fo.order_id)                       AS frequency,
                    SUM(fo.revenue)                                   AS monetary
                FROM fact_orders fo
                JOIN dim_date d ON fo.order_date_key = d.date_key
                WHERE fo.status NOT IN ('cancelled', 'refunded')
                GROUP BY fo.customer_key
            ),
            rfm_scored AS (
                SELECT *,
                    NTILE(5) OVER (ORDER BY recency_days DESC)  AS r_score,
                    NTILE(5) OVER (ORDER BY frequency ASC)      AS f_score,
                    NTILE(5) OVER (ORDER BY monetary ASC)       AS m_score
                FROM rfm_raw
            )
            SELECT
                CASE
                    WHEN r_score >= 4 AND f_score >= 4 AND m_score >= 4 THEN 'Champions'
                    WHEN r_score >= 3 AND f_score >= 3                  THEN 'Loyal Customers'
                    WHEN r_score >= 4 AND f_score <= 2                  THEN 'Recent Customers'
                    WHEN r_score <= 2 AND f_score >= 3 AND m_score >= 3 THEN 'At Risk'
                    WHEN r_score = 1 AND f_score <= 2                   THEN 'Lost'
                    ELSE 'Potential Loyalists'
                END                        AS segment,
                COUNT(*)                   AS customer_count,
                ROUND(AVG(recency_days), 1) AS avg_recency_days,
                ROUND(AVG(frequency), 2)   AS avg_orders,
                ROUND(AVG(monetary), 2)    AS avg_ltv
            FROM rfm_scored
            GROUP BY 1
            ORDER BY avg_ltv DESC
        """,
    },
    {
        "name": "Top 10 Products by Revenue",
        "description": "Best-performing SKUs with margin analysis",
        "sql": """
            SELECT
                dp.sku,
                dp.product_name,
                dp.category,
                SUM(fo.quantity)               AS units_sold,
                SUM(fo.revenue)                AS total_revenue,
                SUM(fo.cost)                   AS total_cost,
                ROUND(
                    (SUM(fo.revenue) - SUM(fo.cost))
                    / NULLIF(SUM(fo.revenue), 0) * 100,
                1)                             AS gross_margin_pct,
                COUNT(DISTINCT fo.customer_key) AS unique_customers
            FROM fact_orders fo
            JOIN dim_products dp ON fo.product_key = dp.product_key
            WHERE fo.status NOT IN ('cancelled', 'refunded')
            GROUP BY dp.sku, dp.product_name, dp.category
            ORDER BY total_revenue DESC
            LIMIT 10
        """,
    },
    {
        "name": "Customer Cohort Retention",
        "description": "Month-1 to Month-6 retention by acquisition cohort",
        "sql": """
            WITH first_orders AS (
                SELECT
                    fo.customer_key,
                    MIN(d.year * 100 + d.month_num) AS cohort_ym
                FROM fact_orders fo
                JOIN dim_date d ON fo.order_date_key = d.date_key
                WHERE fo.status NOT IN ('cancelled', 'refunded')
                GROUP BY fo.customer_key
            ),
            cohort_activity AS (
                SELECT
                    f.cohort_ym,
                    (d.year * 100 + d.month_num) - f.cohort_ym AS months_since_first,
                    COUNT(DISTINCT fo.customer_key) AS active_customers
                FROM fact_orders fo
                JOIN dim_date d ON fo.order_date_key = d.date_key
                JOIN first_orders f ON fo.customer_key = f.customer_key
                WHERE fo.status NOT IN ('cancelled', 'refunded')
                  AND (d.year * 100 + d.month_num) - f.cohort_ym BETWEEN 0 AND 6
                GROUP BY f.cohort_ym, months_since_first
            ),
            cohort_sizes AS (
                SELECT cohort_ym, active_customers AS cohort_size
                FROM cohort_activity WHERE months_since_first = 0
            )
            SELECT
                ca.cohort_ym,
                cs.cohort_size,
                MAX(CASE WHEN months_since_first = 1 THEN
                    ROUND(active_customers::FLOAT / cs.cohort_size * 100, 1) END) AS "M1%",
                MAX(CASE WHEN months_since_first = 2 THEN
                    ROUND(active_customers::FLOAT / cs.cohort_size * 100, 1) END) AS "M2%",
                MAX(CASE WHEN months_since_first = 3 THEN
                    ROUND(active_customers::FLOAT / cs.cohort_size * 100, 1) END) AS "M3%",
                MAX(CASE WHEN months_since_first = 6 THEN
                    ROUND(active_customers::FLOAT / cs.cohort_size * 100, 1) END) AS "M6%"
            FROM cohort_activity ca
            JOIN cohort_sizes cs ON ca.cohort_ym = cs.cohort_ym
            GROUP BY ca.cohort_ym, cs.cohort_size
            ORDER BY ca.cohort_ym DESC
            LIMIT 12
        """,
    },
    {
        "name": "Revenue by Geography (Top 15 Countries)",
        "description": "Revenue, order count and AOV per country",
        "sql": """
            SELECT
                dg.country_name,
                dg.region,
                COUNT(DISTINCT fo.order_id)   AS orders,
                SUM(fo.revenue)               AS revenue,
                ROUND(SUM(fo.revenue) /
                      NULLIF(COUNT(DISTINCT fo.order_id), 0), 2) AS aov
            FROM fact_orders fo
            JOIN dim_geography dg ON fo.geography_key = dg.geography_key
            WHERE fo.status NOT IN ('cancelled', 'refunded')
            GROUP BY dg.country_name, dg.region
            ORDER BY revenue DESC
            LIMIT 15
        """,
    },
    {
        "name": "Conversion Funnel",
        "description": "Step-by-step funnel with drop-off rates",
        "sql": """
            SELECT
                event_type,
                COUNT(DISTINCT session_id)   AS sessions,
                LAG(COUNT(DISTINCT session_id))
                    OVER (ORDER BY step_order) AS prev_step_sessions,
                ROUND(
                    COUNT(DISTINCT session_id)::FLOAT
                    / NULLIF(FIRST_VALUE(COUNT(DISTINCT session_id))
                        OVER (ORDER BY step_order
                              ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),
                      0) * 100,
                1)                           AS pct_of_top
            FROM (
                SELECT
                    session_id, event_type,
                    CASE event_type
                        WHEN 'page_view'       THEN 1
                        WHEN 'product_view'    THEN 2
                        WHEN 'add_to_cart'     THEN 3
                        WHEN 'checkout_start'  THEN 4
                        WHEN 'purchase'        THEN 5
                    END AS step_order
                FROM fact_events
                WHERE event_type IN ('page_view','product_view','add_to_cart',
                                     'checkout_start','purchase')
                  AND event_date >= DATEADD(day, -30, CURRENT_DATE)
            ) funnel_steps
            GROUP BY event_type, step_order
            ORDER BY step_order
        """,
    },
    {
        "name": "Inventory Health",
        "description": "Stock-out risk and overstock by category",
        "sql": """
            SELECT
                dp.category,
                COUNT(*)                                     AS sku_count,
                SUM(CASE WHEN fi.quantity_on_hand = 0 THEN 1 ELSE 0 END)  AS out_of_stock,
                SUM(CASE WHEN fi.quantity_on_hand <= fi.reorder_point
                          AND fi.quantity_on_hand > 0 THEN 1 ELSE 0 END)   AS low_stock,
                SUM(CASE WHEN fi.quantity_on_hand > fi.reorder_point * 10
                          THEN 1 ELSE 0 END)                               AS overstocked,
                ROUND(AVG(fi.quantity_on_hand), 0)           AS avg_qty_on_hand
            FROM fact_inventory_snapshots fi
            JOIN dim_products dp ON fi.product_key = dp.product_key
            WHERE fi.snapshot_date = (SELECT MAX(snapshot_date) FROM fact_inventory_snapshots)
            GROUP BY dp.category
            ORDER BY out_of_stock DESC
        """,
    },
]


# ── Runner ─────────────────────────────────────────────────────────────────────

def run_query(conn, query: dict, export_dir: str | None = None) -> None:
    print(f"\n{'─' * 70}")
    print(f"  {query['name']}")
    print(f"  {query['description']}")
    print(f"{'─' * 70}")

    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(query["sql"])
            rows = cur.fetchall()

        if not rows:
            print("  (no rows returned)")
            return

        headers = list(rows[0].keys())
        table_data = [[row[h] for h in headers] for row in rows]
        print(tabulate(table_data, headers=headers, tablefmt="rounded_outline",
                       floatfmt=".2f", numalign="right"))
        print(f"  {len(rows)} row(s)")

        if export_dir:
            safe_name = query["name"].lower().replace(" ", "_").replace("(", "").replace(")", "")
            path = os.path.join(export_dir, f"{safe_name}.csv")
            with open(path, "w", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=headers)
                writer.writeheader()
                writer.writerows(rows)
            print(f"  Exported → {path}")

    except psycopg2.Error as e:
        print(f"  ERROR: {e.pgerror or e}")


def main():
    parser = argparse.ArgumentParser(description="Run Redshift analytics queries")
    parser.add_argument("--host",     required=True,        help="Redshift cluster endpoint")
    parser.add_argument("--port",     default=5439,         type=int)
    parser.add_argument("--dbname",   default="analytics",  help="Redshift database name")
    parser.add_argument("--user",     default="dwadmin",    help="Redshift username")
    parser.add_argument("--password", required=True)
    parser.add_argument("--export",   action="store_true",  help="Export results to CSV files")
    parser.add_argument("--query",    default=None,         help="Run only this query name (partial match)")
    args = parser.parse_args()

    export_dir = None
    if args.export:
        export_dir = f"analytics_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        os.makedirs(export_dir)
        print(f"  Exporting results to: {export_dir}/")

    conn = psycopg2.connect(
        host=args.host, port=args.port, dbname=args.dbname,
        user=args.user, password=args.password,
    )
    conn.autocommit = True

    print("\n" + "=" * 70)
    print("  E-Commerce Analytics Dashboard")
    print(f"  Redshift: {args.host}:{args.port}/{args.dbname}")
    print(f"  Run time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)

    queries_to_run = QUERIES
    if args.query:
        term = args.query.lower()
        queries_to_run = [q for q in QUERIES if term in q["name"].lower()]
        if not queries_to_run:
            print(f"  No queries matched '{args.query}'")
            return

    for query in queries_to_run:
        run_query(conn, query, export_dir)

    conn.close()
    print(f"\n{'=' * 70}")
    print(f"  Completed {len(queries_to_run)} analytic queries.")
    print("=" * 70 + "\n")


if __name__ == "__main__":
    main()

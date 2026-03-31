"""
Real-Time E-Commerce Analytics Platform
ETL Job: rds_to_s3.py — Incremental extract from RDS PostgreSQL to S3 (Parquet)

Extracts data incrementally from RDS using Glue job bookmarks (watermark tracking),
applies light transformations and type coercions, then writes Parquet files to the
S3 processed zone partitioned by year/month/day.

AWS Glue version: 4.0 (Spark 3.3, Python 3.10)
Worker type: G.1X
Recommended workers: 5
"""

import sys
import json
import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional

import boto3
from botocore.exceptions import ClientError

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

from pyspark.context import SparkContext
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, LongType,
    TimestampType, BooleanType, DoubleType,
    DecimalType
)

# ─────────────────────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────────────────────
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter(
    '%(asctime)s %(levelname)s [%(name)s] %(message)s',
    datefmt='%Y-%m-%dT%H:%M:%S'
))
logger.addHandler(handler)


# ─────────────────────────────────────────────────────────────
# Table extraction configurations
# ─────────────────────────────────────────────────────────────
TABLE_CONFIGS: Dict[str, Dict] = {
    "orders": {
        "query": """
            SELECT
                o.order_id::TEXT         AS order_id,
                o.order_number,
                o.customer_id::TEXT      AS customer_id,
                o.status::TEXT           AS order_status,
                o.payment_status::TEXT   AS payment_status,
                o.payment_method::TEXT   AS payment_method,
                o.subtotal_cents,
                o.discount_cents,
                o.shipping_cents,
                o.tax_cents,
                o.total_cents,
                o.refunded_cents,
                o.discount_code,
                o.shipping_country,
                o.shipping_city,
                o.shipping_state,
                o.created_at             AS order_created_at,
                o.updated_at,
                o.confirmed_at,
                o.shipped_at,
                o.delivered_at,
                o.cancelled_at,
                oi.item_id,
                oi.product_id::TEXT      AS product_id,
                oi.product_sku,
                oi.product_name,
                oi.quantity,
                oi.unit_price_cents,
                oi.discount_cents        AS item_discount_cents,
                oi.total_cents           AS item_total_cents,
                p.category_id,
                pc.name                  AS category_name,
                p.brand
            FROM orders o
            JOIN order_items oi
                ON oi.order_id = o.order_id
                AND oi.order_created_at = o.created_at
            JOIN products p ON p.product_id = oi.product_id
            JOIN product_categories pc ON pc.category_id = p.category_id
            WHERE o.updated_at > '{watermark_from}'
              AND o.updated_at <= '{watermark_to}'
        """,
        "watermark_col": "updated_at",
        "partition_cols": ["year", "month", "day"],
        "output_path": "orders",
    },
    "customers": {
        "query": """
            SELECT
                customer_id::TEXT        AS customer_id,
                email,
                email_verified,
                first_name,
                last_name,
                phone,
                country_code,
                city,
                state_province,
                postal_code,
                is_active,
                is_guest,
                marketing_opt_in,
                acquisition_source,
                acquisition_medium,
                acquisition_campaign,
                loyalty_points,
                loyalty_tier,
                created_at,
                updated_at,
                last_login_at
            FROM customers
            WHERE updated_at > '{watermark_from}'
              AND updated_at <= '{watermark_to}'
              AND deleted_at IS NULL
        """,
        "watermark_col": "updated_at",
        "partition_cols": ["year", "month", "day"],
        "output_path": "customers",
    },
    "products": {
        "query": """
            SELECT
                product_id::TEXT         AS product_id,
                sku,
                name                     AS product_name,
                category_id,
                pc.name                  AS category_name,
                brand,
                base_price_cents,
                sale_price_cents,
                cost_price_cents,
                is_active,
                is_digital,
                is_taxable,
                review_count,
                avg_rating,
                created_at,
                updated_at
            FROM products p
            JOIN product_categories pc ON pc.category_id = p.category_id
            WHERE p.updated_at > '{watermark_from}'
              AND p.updated_at <= '{watermark_to}'
              AND p.deleted_at IS NULL
        """,
        "watermark_col": "updated_at",
        "partition_cols": ["year", "month", "day"],
        "output_path": "products",
    },
    "events": {
        "query": """
            SELECT
                event_id::TEXT           AS event_id,
                session_id::TEXT         AS session_id,
                customer_id::TEXT        AS customer_id,
                anonymous_id::TEXT       AS anonymous_id,
                event_type::TEXT         AS event_type,
                occurred_at,
                page_url,
                page_title,
                product_id::TEXT         AS product_id,
                order_id::TEXT           AS order_id,
                search_query,
                properties::TEXT         AS properties_json
            FROM events
            WHERE occurred_at > '{watermark_from}'
              AND occurred_at <= '{watermark_to}'
        """,
        "watermark_col": "occurred_at",
        "partition_cols": ["year", "month", "day"],
        "output_path": "events",
    },
    "inventory": {
        "query": """
            SELECT
                i.inventory_id,
                i.product_id::TEXT       AS product_id,
                p.sku,
                i.warehouse_id,
                i.quantity_on_hand,
                i.quantity_reserved,
                i.quantity_available,
                i.reorder_point,
                i.unit_cost_cents,
                i.updated_at
            FROM inventory i
            JOIN products p ON p.product_id = i.product_id
            WHERE i.updated_at > '{watermark_from}'
              AND i.updated_at <= '{watermark_to}'
        """,
        "watermark_col": "updated_at",
        "partition_cols": ["year", "month", "day"],
        "output_path": "inventory",
    },
}


def get_rds_credentials(secret_name: str, region: str = "us-east-1") -> Dict[str, str]:
    """Retrieve RDS credentials from AWS Secrets Manager."""
    client = boto3.client("secretsmanager", region_name=region)
    try:
        response = client.get_secret_value(SecretId=secret_name)
        return json.loads(response["SecretString"])
    except ClientError as e:
        logger.error(f"Failed to retrieve secret '{secret_name}': {e}")
        raise


def get_jdbc_url(host: str, port: int, db_name: str) -> str:
    """Build JDBC connection URL for PostgreSQL."""
    return f"jdbc:postgresql://{host}:{port}/{db_name}?ssl=true&sslmode=require"


def get_last_watermark(
    glue_client,
    job_name: str,
    table_name: str,
    default_hours_back: int = 2
) -> tuple[datetime, datetime]:
    """
    Returns (watermark_from, watermark_to) using Glue job bookmark state.
    Falls back to 'current_time - default_hours_back' if no prior run.
    """
    now = datetime.now(timezone.utc)

    try:
        response = glue_client.get_job_bookmark(JobName=job_name)
        bookmark = response.get("JobBookmarkEntry", {}).get("JobBookmark", {})
        last_run_at = bookmark.get(f"watermark_{table_name}")

        if last_run_at:
            watermark_from = datetime.fromisoformat(last_run_at)
            logger.info(
                f"[{table_name}] Using bookmarked watermark: {watermark_from}"
            )
        else:
            from datetime import timedelta
            watermark_from = now - timedelta(hours=default_hours_back)
            logger.info(
                f"[{table_name}] No prior bookmark; using {default_hours_back}h lookback: {watermark_from}"
            )
    except Exception as e:
        logger.warning(f"Could not retrieve job bookmark: {e}. Using default lookback.")
        from datetime import timedelta
        watermark_from = now - timedelta(hours=default_hours_back)

    return watermark_from, now


def add_partition_columns(df: DataFrame, ts_col: str = "order_created_at") -> DataFrame:
    """Add year/month/day partition columns derived from a timestamp column."""
    # Prefer order_created_at; fall back to first available timestamp column
    available_cols = df.columns
    source_col = ts_col if ts_col in available_cols else "updated_at"
    if source_col not in available_cols:
        # Try common timestamp columns
        for candidate in ["occurred_at", "created_at", "ordered_at"]:
            if candidate in available_cols:
                source_col = candidate
                break

    return (
        df
        .withColumn("year",  F.date_format(F.col(source_col), "yyyy"))
        .withColumn("month", F.date_format(F.col(source_col), "MM"))
        .withColumn("day",   F.date_format(F.col(source_col), "dd"))
    )


def clean_dataframe(df: DataFrame) -> DataFrame:
    """
    Apply common data cleaning rules:
    - Trim string columns
    - Normalize NULL-like strings to actual NULLs
    - Ensure timestamps are UTC
    """
    null_strings = ["", "NULL", "None", "null", "none", "N/A", "n/a"]

    for field in df.schema.fields:
        if isinstance(field.dataType, StringType):
            df = df.withColumn(
                field.name,
                F.when(
                    F.trim(F.col(field.name)).isin(null_strings),
                    F.lit(None)
                ).otherwise(F.trim(F.col(field.name)))
            )
        elif isinstance(field.dataType, TimestampType):
            # Ensure timestamp is in UTC (Spark reads JDBC timestamps as local by default)
            df = df.withColumn(
                field.name,
                F.to_utc_timestamp(F.col(field.name), "UTC")
            )

    return df


def extract_table(
    glue_context: GlueContext,
    spark: SparkSession,
    table_name: str,
    config: Dict,
    jdbc_url: str,
    jdbc_user: str,
    jdbc_password: str,
    watermark_from: datetime,
    watermark_to: datetime,
) -> Optional[DataFrame]:
    """Extract a single table from RDS using JDBC with watermark filtering."""
    query = config["query"].format(
        watermark_from=watermark_from.strftime("%Y-%m-%d %H:%M:%S"),
        watermark_to=watermark_to.strftime("%Y-%m-%d %H:%M:%S"),
    )

    # Wrap in subquery for JDBC
    jdbc_query = f"({query}) AS t"

    logger.info(f"[{table_name}] Extracting data from {watermark_from} to {watermark_to}")

    try:
        df = (
            spark.read
            .format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", jdbc_query)
            .option("user", jdbc_user)
            .option("password", jdbc_password)
            .option("driver", "org.postgresql.Driver")
            # Performance options
            .option("fetchsize", "10000")
            .option("numPartitions", "8")
            # Use SSL
            .option("ssl", "true")
            .option("sslfactory", "org.postgresql.ssl.NonValidatingFactory")
            .load()
        )

        count = df.count()
        logger.info(f"[{table_name}] Extracted {count:,} rows")

        if count == 0:
            logger.info(f"[{table_name}] No new data in window — skipping write")
            return None

        return df

    except Exception as e:
        logger.error(f"[{table_name}] JDBC extraction failed: {e}")
        raise


def write_parquet(
    df: DataFrame,
    s3_bucket: str,
    output_path: str,
    partition_cols: List[str],
) -> None:
    """Write DataFrame to S3 as Parquet with partition pruning."""
    s3_uri = f"s3://{s3_bucket}/{output_path}/"

    logger.info(f"Writing to {s3_uri} partitioned by {partition_cols}")

    (
        df.write
        .mode("append")
        .partitionBy(*partition_cols)
        .option("compression", "snappy")
        # Merge small files (Glue-specific)
        .option("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
        .parquet(s3_uri)
    )


def main():
    """Entry point — called by AWS Glue job runner."""

    # ── Parse job arguments ───────────────────────────────────
    args = getResolvedOptions(sys.argv, [
        "JOB_NAME",
        "rds_secret_name",
        "s3_output_bucket",
        "environment",
        "project_prefix",
    ])

    job_name       = args["JOB_NAME"]
    secret_name    = args["rds_secret_name"]
    output_bucket  = args["s3_output_bucket"]
    environment    = args["environment"]

    # ── Initialize Glue context ───────────────────────────────
    sc           = SparkContext()
    glue_context = GlueContext(sc)
    spark        = glue_context.spark_session
    job          = Job(glue_context)
    job.init(job_name, args)

    # Tune Spark for JDBC performance
    spark.conf.set("spark.sql.shuffle.partitions", "200")
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")

    # ── Retrieve credentials ──────────────────────────────────
    logger.info("Retrieving RDS credentials from Secrets Manager")
    credentials = get_rds_credentials(secret_name)

    jdbc_url = get_jdbc_url(
        host     = credentials["host"],
        port     = int(credentials.get("port", 5432)),
        db_name  = credentials.get("dbname", "ecommerce"),
    )
    jdbc_user     = credentials["username"]
    jdbc_password = credentials["password"]

    glue_client = boto3.client("glue")

    # ── Extract and write each table ──────────────────────────
    results = {}
    errors  = []

    for table_name, config in TABLE_CONFIGS.items():
        try:
            watermark_from, watermark_to = get_last_watermark(
                glue_client, job_name, table_name
            )

            df = extract_table(
                glue_context   = glue_context,
                spark          = spark,
                table_name     = table_name,
                config         = config,
                jdbc_url       = jdbc_url,
                jdbc_user      = jdbc_user,
                jdbc_password  = jdbc_password,
                watermark_from = watermark_from,
                watermark_to   = watermark_to,
            )

            if df is None:
                results[table_name] = {"status": "skipped", "rows": 0}
                continue

            # Clean and enrich
            df_clean = clean_dataframe(df)

            # Determine timestamp column for partition derivation
            ts_col_map = {
                "orders": "order_created_at",
                "events": "occurred_at",
                "customers": "updated_at",
                "products": "updated_at",
                "inventory": "updated_at",
            }
            df_partitioned = add_partition_columns(df_clean, ts_col_map.get(table_name, "updated_at"))

            # Write to S3
            write_parquet(
                df             = df_partitioned,
                s3_bucket      = output_bucket,
                output_path    = config["output_path"],
                partition_cols = config["partition_cols"],
            )

            row_count = df_partitioned.count()
            results[table_name] = {
                "status": "success",
                "rows": row_count,
                "watermark_from": watermark_from.isoformat(),
                "watermark_to": watermark_to.isoformat(),
            }
            logger.info(f"[{table_name}] Successfully wrote {row_count:,} rows to S3")

        except Exception as e:
            logger.error(f"[{table_name}] Failed: {e}", exc_info=True)
            errors.append({"table": table_name, "error": str(e)})
            results[table_name] = {"status": "failed", "error": str(e)}

    # ── Summary ───────────────────────────────────────────────
    logger.info("=" * 60)
    logger.info("ETL Job Summary:")
    for table, result in results.items():
        logger.info(f"  {table}: {result}")

    if errors:
        error_summary = "; ".join([f"{e['table']}: {e['error']}" for e in errors])
        logger.error(f"Job completed with {len(errors)} error(s): {error_summary}")
        # Commit bookmark for successful tables before raising
        job.commit()
        raise RuntimeError(f"Extraction failed for tables: {[e['table'] for e in errors]}")

    logger.info("All tables extracted successfully")
    job.commit()


if __name__ == "__main__":
    main()

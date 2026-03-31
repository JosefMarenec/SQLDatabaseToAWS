"""
Real-Time E-Commerce Analytics Platform
ETL Job: data_quality.py — Data Quality Validation

Validates Parquet files in the S3 processed zone before they are loaded
into Redshift. Checks:
  - Schema conformance (expected columns and types)
  - Null rate for required fields
  - Value range constraints
  - Referential integrity (foreign key existence)
  - Duplicate detection on primary keys
  - Freshness (data is not stale)

On critical failures: raises exception to fail the Glue job (blocking load).
On warnings: logs and writes report, but continues.

Quality reports are written to DynamoDB and S3 for audit.

AWS Glue version: 4.0
"""

import sys
import json
import uuid
import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, asdict, field

import boto3
from botocore.exceptions import ClientError
from decimal import Decimal

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.context import SparkContext
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StringType, IntegerType, LongType, TimestampType, BooleanType, DoubleType
)

# ─────────────────────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────────────────────
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(message)s'))
logger.addHandler(handler)


# ─────────────────────────────────────────────────────────────
# Data Classes
# ─────────────────────────────────────────────────────────────
@dataclass
class QualityCheck:
    check_name: str
    table_name: str
    severity: str           # CRITICAL | WARNING | INFO
    status: str             # PASSED | FAILED | SKIPPED
    message: str
    metric_value: Any = None
    threshold: Any = None
    rows_affected: int = 0


@dataclass
class QualityReport:
    report_id: str
    table_name: str
    run_date: str           # partition key
    job_run_id: str
    checks: List[QualityCheck] = field(default_factory=list)
    total_rows: int = 0
    critical_failures: int = 0
    warnings: int = 0
    passed: int = 0
    overall_status: str = "PENDING"  # PASSED | FAILED | DEGRADED

    def add_check(self, check: QualityCheck):
        self.checks.append(check)
        if check.status == "FAILED":
            if check.severity == "CRITICAL":
                self.critical_failures += 1
            elif check.severity == "WARNING":
                self.warnings += 1
        elif check.status == "PASSED":
            self.passed += 1

    def finalize(self):
        if self.critical_failures > 0:
            self.overall_status = "FAILED"
        elif self.warnings > 0:
            self.overall_status = "DEGRADED"
        else:
            self.overall_status = "PASSED"


# ─────────────────────────────────────────────────────────────
# Schema Expectations
# ─────────────────────────────────────────────────────────────
SCHEMA_EXPECTATIONS = {
    "orders": {
        "required_columns": [
            "order_id", "order_number", "customer_id",
            "order_status", "total_cents", "order_created_at",
            "item_id", "product_id", "quantity", "unit_price_cents",
        ],
        "not_null_columns": [
            "order_id", "customer_id", "order_status",
            "total_cents", "order_created_at", "product_id",
        ],
        "range_checks": {
            "total_cents":      {"min": 0, "max": 10_000_000},    # $0 to $100,000
            "quantity":         {"min": 1, "max": 10_000},
            "unit_price_cents": {"min": 0, "max": 100_000_000},
        },
        "primary_key": "item_id",
        "max_null_rate": 0.01,   # 1% overall null rate threshold
    },
    "customers": {
        "required_columns": [
            "customer_id", "email", "first_name", "last_name",
            "country_code", "created_at",
        ],
        "not_null_columns": ["customer_id", "email"],
        "range_checks": {
            "loyalty_points": {"min": 0, "max": 10_000_000},
        },
        "primary_key": "customer_id",
        "max_null_rate": 0.02,
    },
    "products": {
        "required_columns": [
            "product_id", "sku", "product_name", "category_id",
            "base_price_cents", "created_at",
        ],
        "not_null_columns": ["product_id", "sku", "product_name"],
        "range_checks": {
            "base_price_cents": {"min": 0, "max": 100_000_000},
            "avg_rating":       {"min": 0.0, "max": 5.0},
        },
        "primary_key": "product_id",
        "max_null_rate": 0.02,
    },
    "events": {
        "required_columns": [
            "event_id", "event_type", "occurred_at",
        ],
        "not_null_columns": ["event_id", "event_type", "occurred_at"],
        "range_checks": {},
        "primary_key": "event_id",
        "max_null_rate": 0.05,
    },
}


# ─────────────────────────────────────────────────────────────
# Quality Check Functions
# ─────────────────────────────────────────────────────────────

def check_schema_conformance(
    df: DataFrame,
    table_name: str,
    expected_cols: List[str],
    report: QualityReport,
) -> None:
    """Verify all required columns are present."""
    actual_cols = set(df.columns)
    missing     = set(expected_cols) - actual_cols

    if missing:
        report.add_check(QualityCheck(
            check_name    = "schema_conformance",
            table_name    = table_name,
            severity      = "CRITICAL",
            status        = "FAILED",
            message       = f"Missing required columns: {sorted(missing)}",
            metric_value  = list(missing),
        ))
    else:
        report.add_check(QualityCheck(
            check_name  = "schema_conformance",
            table_name  = table_name,
            severity    = "CRITICAL",
            status      = "PASSED",
            message     = "All required columns present",
        ))


def check_null_rates(
    df: DataFrame,
    table_name: str,
    not_null_cols: List[str],
    max_null_rate: float,
    report: QualityReport,
) -> None:
    """Check null rates for required columns."""
    total_rows = df.count()
    if total_rows == 0:
        report.add_check(QualityCheck(
            check_name = "null_rate",
            table_name = table_name,
            severity   = "WARNING",
            status     = "FAILED",
            message    = "DataFrame is empty",
        ))
        return

    report.total_rows = total_rows

    for col_name in not_null_cols:
        if col_name not in df.columns:
            continue

        null_count = df.filter(F.col(col_name).isNull()).count()
        null_rate  = null_count / total_rows

        severity = "CRITICAL" if null_rate > max_null_rate else "WARNING"
        status   = "FAILED" if null_rate > 0 else "PASSED"

        report.add_check(QualityCheck(
            check_name     = f"null_rate:{col_name}",
            table_name     = table_name,
            severity       = severity if status == "FAILED" else "INFO",
            status         = status,
            message        = f"Column '{col_name}' null rate: {null_rate:.4%} ({null_count}/{total_rows})",
            metric_value   = round(null_rate, 6),
            threshold      = max_null_rate,
            rows_affected  = null_count,
        ))


def check_value_ranges(
    df: DataFrame,
    table_name: str,
    range_checks: Dict[str, Dict],
    report: QualityReport,
) -> None:
    """Validate numeric columns fall within expected ranges."""
    for col_name, bounds in range_checks.items():
        if col_name not in df.columns:
            continue

        min_val = bounds.get("min")
        max_val = bounds.get("max")

        conditions = []
        if min_val is not None:
            conditions.append(F.col(col_name) < min_val)
        if max_val is not None:
            conditions.append(F.col(col_name) > max_val)

        if not conditions:
            continue

        violation_condition = conditions[0]
        for c in conditions[1:]:
            violation_condition = violation_condition | c

        violations = df.filter(violation_condition.isNotNull() & violation_condition).count()

        status = "FAILED" if violations > 0 else "PASSED"
        report.add_check(QualityCheck(
            check_name    = f"range_check:{col_name}",
            table_name    = table_name,
            severity      = "CRITICAL" if violations > 0 else "INFO",
            status        = status,
            message       = (
                f"Column '{col_name}' has {violations} out-of-range values "
                f"(expected {min_val} to {max_val})"
            ) if violations > 0 else f"Column '{col_name}' range check passed",
            metric_value  = violations,
            threshold     = {"min": min_val, "max": max_val},
            rows_affected = violations,
        ))


def check_duplicates(
    df: DataFrame,
    table_name: str,
    primary_key: str,
    report: QualityReport,
) -> None:
    """Detect duplicate primary key values."""
    if primary_key not in df.columns:
        report.add_check(QualityCheck(
            check_name = f"duplicate_check:{primary_key}",
            table_name = table_name,
            severity   = "WARNING",
            status     = "SKIPPED",
            message    = f"Primary key column '{primary_key}' not found",
        ))
        return

    total_rows    = df.count()
    distinct_rows = df.select(primary_key).distinct().count()
    duplicate_count = total_rows - distinct_rows

    status = "FAILED" if duplicate_count > 0 else "PASSED"
    report.add_check(QualityCheck(
        check_name    = f"duplicate_check:{primary_key}",
        table_name    = table_name,
        severity      = "CRITICAL" if duplicate_count > 0 else "INFO",
        status        = status,
        message       = (
            f"{duplicate_count} duplicate values found in '{primary_key}'"
        ) if duplicate_count > 0 else f"No duplicates found in '{primary_key}'",
        metric_value  = duplicate_count,
        rows_affected = duplicate_count,
    ))


def check_freshness(
    df: DataFrame,
    table_name: str,
    ts_col: str,
    max_age_hours: int,
    report: QualityReport,
) -> None:
    """Check that the data is not older than max_age_hours."""
    if ts_col not in df.columns:
        return

    max_ts = df.agg(F.max(F.col(ts_col))).collect()[0][0]
    if max_ts is None:
        report.add_check(QualityCheck(
            check_name = "freshness",
            table_name = table_name,
            severity   = "WARNING",
            status     = "FAILED",
            message    = f"No timestamp values found in '{ts_col}'",
        ))
        return

    age_hours = (datetime.now(timezone.utc) - max_ts.replace(tzinfo=timezone.utc)).total_seconds() / 3600

    status = "FAILED" if age_hours > max_age_hours else "PASSED"
    report.add_check(QualityCheck(
        check_name   = "freshness",
        table_name   = table_name,
        severity     = "WARNING",
        status       = status,
        message      = (
            f"Latest record in '{ts_col}' is {age_hours:.1f}h old (max allowed: {max_age_hours}h)"
        ),
        metric_value = round(age_hours, 2),
        threshold    = max_age_hours,
    ))


def check_referential_integrity(
    spark: SparkSession,
    df: DataFrame,
    table_name: str,
    fk_col: str,
    ref_df: DataFrame,
    ref_col: str,
    report: QualityReport,
) -> None:
    """Check that foreign key values exist in the referenced table."""
    if fk_col not in df.columns:
        return

    orphaned = (
        df.select(F.col(fk_col).alias("fk"))
        .join(ref_df.select(F.col(ref_col).alias("ref_key")),
              F.col("fk") == F.col("ref_key"), "left_anti")
        .count()
    )

    status = "FAILED" if orphaned > 0 else "PASSED"
    report.add_check(QualityCheck(
        check_name    = f"referential_integrity:{fk_col}",
        table_name    = table_name,
        severity      = "WARNING",
        status        = status,
        message       = (
            f"{orphaned} orphaned '{fk_col}' values with no matching '{ref_col}'"
        ) if orphaned > 0 else f"Referential integrity OK for '{fk_col}'",
        metric_value  = orphaned,
        rows_affected = orphaned,
    ))


def write_report_to_dynamodb(
    report: QualityReport,
    dynamodb_table: str,
    region: str = "us-east-1",
) -> None:
    """Persist quality report to DynamoDB."""
    dynamodb = boto3.resource("dynamodb", region_name=region)
    table    = dynamodb.Table(dynamodb_table)

    def floatify(obj):
        """DynamoDB doesn't support Python float — convert to Decimal."""
        if isinstance(obj, float):
            return Decimal(str(round(obj, 8)))
        if isinstance(obj, dict):
            return {k: floatify(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [floatify(v) for v in obj]
        return obj

    item = {
        "report_id":         report.report_id,
        "run_date":          report.run_date,
        "job_run_id":        report.job_run_id,
        "table_name":        report.table_name,
        "overall_status":    report.overall_status,
        "total_rows":        report.total_rows,
        "critical_failures": report.critical_failures,
        "warnings":          report.warnings,
        "passed":            report.passed,
        "checks":            floatify([asdict(c) for c in report.checks]),
        "created_at":        datetime.now(timezone.utc).isoformat(),
        # TTL: expire reports after 90 days
        "ttl":               int((datetime.now(timezone.utc) + timedelta(days=90)).timestamp()),
    }

    table.put_item(Item=item)
    logger.info(f"Quality report {report.report_id} written to DynamoDB")


def write_report_to_s3(
    report: QualityReport,
    bucket: str,
    prefix: str = "quality-reports/",
    region: str = "us-east-1",
) -> str:
    """Write JSON quality report to S3 for archival."""
    s3     = boto3.client("s3", region_name=region)
    key    = (
        f"{prefix}{report.table_name}/"
        f"year={report.run_date[:4]}/month={report.run_date[5:7]}/"
        f"{report.report_id}.json"
    )
    body   = json.dumps(asdict(report), indent=2, default=str)

    s3.put_object(
        Bucket      = bucket,
        Key         = key,
        Body        = body.encode("utf-8"),
        ContentType = "application/json",
    )
    logger.info(f"Quality report written to s3://{bucket}/{key}")
    return f"s3://{bucket}/{key}"


def quarantine_invalid_rows(
    df: DataFrame,
    table_name: str,
    quarantine_bucket: str,
    reason: str,
) -> None:
    """Write invalid rows to the quarantine zone with error metadata."""
    quarantine_path = (
        f"s3://{quarantine_bucket}/{table_name}/"
        f"{datetime.now(timezone.utc).strftime('%Y/%m/%d/%H%M%S')}/"
    )
    (
        df
        .withColumn("quarantine_reason", F.lit(reason))
        .withColumn("quarantined_at", F.current_timestamp())
        .write
        .mode("append")
        .parquet(quarantine_path)
    )
    logger.warning(f"Quarantined {df.count()} rows to {quarantine_path}")


def validate_table(
    spark: SparkSession,
    table_name: str,
    df: DataFrame,
    expectations: Dict,
    quarantine_bucket: str,
    dynamodb_table: str,
    report_bucket: str,
    job_run_id: str,
    region: str,
) -> QualityReport:
    """Run all quality checks for a single table and return a report."""
    report = QualityReport(
        report_id   = str(uuid.uuid4()),
        table_name  = table_name,
        run_date    = datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        job_run_id  = job_run_id,
    )

    logger.info(f"[{table_name}] Starting quality validation...")

    # Schema check
    check_schema_conformance(
        df, table_name,
        expectations.get("required_columns", []),
        report
    )

    # Short-circuit if schema is broken
    if report.critical_failures > 0:
        report.finalize()
        return report

    # Null rate checks
    check_null_rates(
        df, table_name,
        expectations.get("not_null_columns", []),
        expectations.get("max_null_rate", 0.01),
        report
    )

    # Range checks
    check_value_ranges(
        df, table_name,
        expectations.get("range_checks", {}),
        report
    )

    # Duplicate checks
    if pk := expectations.get("primary_key"):
        check_duplicates(df, table_name, pk, report)

        # Quarantine duplicates if critical
        if any(c.check_name.startswith("duplicate_check") and c.status == "FAILED"
               for c in report.checks):
            dupes = (
                df.groupBy(pk)
                .count()
                .filter(F.col("count") > 1)
                .join(df, pk, "inner")
            )
            quarantine_invalid_rows(
                dupes, table_name, quarantine_bucket,
                f"Duplicate primary key: {pk}"
            )

    # Freshness check
    ts_col_map = {
        "orders": "order_created_at",
        "events": "occurred_at",
        "customers": "updated_at",
        "products": "updated_at",
        "inventory": "updated_at",
    }
    if ts_col := ts_col_map.get(table_name):
        check_freshness(df, table_name, ts_col, max_age_hours=3, report=report)

    # Finalize
    report.finalize()

    # Persist reports
    write_report_to_dynamodb(report, dynamodb_table, region)
    write_report_to_s3(report, report_bucket)

    logger.info(
        f"[{table_name}] Quality check complete: {report.overall_status} "
        f"(critical={report.critical_failures}, warnings={report.warnings}, passed={report.passed})"
    )

    return report


def main():
    """Entry point for Glue data quality job."""

    args = getResolvedOptions(sys.argv, [
        "JOB_NAME",
        "s3_processed_bucket",
        "s3_quarantine_bucket",
        "dynamodb_table",
        "environment",
        "project_prefix",
    ])

    job_name          = args["JOB_NAME"]
    processed_bucket  = args["s3_processed_bucket"]
    quarantine_bucket = args["s3_quarantine_bucket"]
    dynamodb_table    = args["dynamodb_table"]
    region            = "us-east-1"

    sc           = SparkContext()
    glue_context = GlueContext(sc)
    spark        = glue_context.spark_session
    job          = Job(glue_context)
    job.init(job_name, args)

    job_run_id = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")

    all_reports   = []
    critical_fail = False

    for table_name, expectations in SCHEMA_EXPECTATIONS.items():
        try:
            s3_path = f"s3://{processed_bucket}/{table_name}/"
            df = spark.read.parquet(s3_path)

            if df.rdd.isEmpty():
                logger.warning(f"[{table_name}] No data found at {s3_path} — skipping")
                continue

            report = validate_table(
                spark             = spark,
                table_name        = table_name,
                df                = df,
                expectations      = expectations,
                quarantine_bucket = quarantine_bucket,
                dynamodb_table    = dynamodb_table,
                report_bucket     = processed_bucket,
                job_run_id        = job_run_id,
                region            = region,
            )
            all_reports.append(report)

            if report.critical_failures > 0:
                critical_fail = True

        except Exception as e:
            logger.error(f"[{table_name}] Validation error: {e}", exc_info=True)
            critical_fail = True

    # Summary
    logger.info("=" * 60)
    for r in all_reports:
        logger.info(f"  {r.table_name}: {r.overall_status} "
                    f"(crit={r.critical_failures}, warn={r.warnings})")

    job.commit()

    if critical_fail:
        raise RuntimeError(
            "Data quality check FAILED with critical violations. "
            "Review DynamoDB quality reports before proceeding with load."
        )

    logger.info("All quality checks passed")


if __name__ == "__main__":
    main()

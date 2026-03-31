"""
Real-Time E-Commerce Analytics Platform
Lambda Function: data_validator/handler.py

Triggered by S3 PutObject events on the raw data lake bucket.
Validates incoming data files (format, schema, basic content checks),
then routes them:
  - Valid files   → s3://processed-bucket/<table>/<partitioned path>
  - Invalid files → s3://quarantine-bucket/<table>/<timestamp>/

Sends SNS alerts on validation failures for operational visibility.

Runtime: Python 3.11
Memory: 1024 MB
Timeout: 300 seconds
"""

import json
import os
import io
import csv
import gzip
import logging
import hashlib
import time
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, asdict

import boto3
from botocore.exceptions import ClientError

# ─────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────
logger = logging.getLogger(__name__)
logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))

PROCESSED_BUCKET  = os.environ["PROCESSED_BUCKET"]
QUARANTINE_BUCKET = os.environ["QUARANTINE_BUCKET"]
ALERTS_TOPIC_ARN  = os.environ.get("ALERTS_TOPIC_ARN", "")
ENVIRONMENT       = os.environ.get("ENVIRONMENT", "prod")
AWS_REGION        = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")

# Maximum file size to validate in memory (50 MB)
MAX_FILE_SIZE_BYTES = 50 * 1024 * 1024

# Expected schemas for each data type
# These align with the Glue table schemas in 02_external_tables.sql
EXPECTED_SCHEMAS: Dict[str, List[str]] = {
    "orders": [
        "order_id", "order_number", "customer_id", "order_status",
        "total_cents", "order_created_at", "item_id", "product_id",
        "quantity", "unit_price_cents",
    ],
    "events": [
        "event_id", "event_type", "occurred_at",
    ],
    "customers": [
        "customer_id", "email", "first_name", "last_name",
        "country_code", "created_at",
    ],
    "products": [
        "product_id", "sku", "product_name", "base_price_cents",
    ],
    "inventory": [
        "inventory_id", "product_id", "warehouse_id",
        "quantity_on_hand", "updated_at",
    ],
}


@dataclass
class ValidationResult:
    s3_key: str
    table_name: str
    status: str              # VALID | INVALID | UNKNOWN
    row_count: int = 0
    error_count: int = 0
    errors: List[str] = None
    warnings: List[str] = None
    file_size_bytes: int = 0
    content_hash: str = ""
    validated_at: str = ""

    def __post_init__(self):
        if self.errors is None:
            self.errors = []
        if self.warnings is None:
            self.warnings = []
        self.validated_at = datetime.now(timezone.utc).isoformat()


# ─────────────────────────────────────────────────────────────
# AWS Clients (module-level for warm Lambda reuse)
# ─────────────────────────────────────────────────────────────
_s3_client  = boto3.client("s3",  region_name=AWS_REGION)
_sns_client = boto3.client("sns", region_name=AWS_REGION)


def detect_table_name(s3_key: str) -> Optional[str]:
    """
    Infer the data table from the S3 key path.
    Expected path format: raw/<table_name>/year=YYYY/month=MM/day=DD/<file>
    """
    parts = s3_key.strip("/").split("/")
    if len(parts) >= 2 and parts[0] == "raw":
        table = parts[1]
        if table in EXPECTED_SCHEMAS:
            return table
    # Try to match any segment to a known table
    for part in parts:
        if part in EXPECTED_SCHEMAS:
            return part
    return None


def detect_file_format(s3_key: str, content_bytes: bytes) -> str:
    """Detect file format from extension and magic bytes."""
    key_lower = s3_key.lower()

    if key_lower.endswith(".parquet"):
        # Parquet magic bytes: PAR1 at start and end
        if content_bytes[:4] == b"PAR1":
            return "parquet"
        return "parquet_invalid"

    if key_lower.endswith(".gz") or key_lower.endswith(".gzip"):
        if content_bytes[:2] == b"\x1f\x8b":
            return "gzip"

    if key_lower.endswith(".csv") or key_lower.endswith(".tsv"):
        return "csv"

    if key_lower.endswith(".json") or key_lower.endswith(".jsonl"):
        return "json"

    # Try to detect from content
    if content_bytes[:4] == b"PAR1":
        return "parquet"
    if content_bytes[:2] == b"\x1f\x8b":
        return "gzip"

    return "unknown"


def validate_csv(
    content_bytes: bytes,
    table_name: str,
    result: ValidationResult,
    file_format: str = "csv",
) -> None:
    """Validate CSV or gzip-compressed CSV content."""
    try:
        if file_format == "gzip":
            decompressed = gzip.decompress(content_bytes)
            text_content = decompressed.decode("utf-8", errors="replace")
        else:
            text_content = content_bytes.decode("utf-8", errors="replace")

        reader = csv.DictReader(io.StringIO(text_content))
        headers = reader.fieldnames or []

        if not headers:
            result.errors.append("CSV has no headers")
            result.status = "INVALID"
            return

        # Schema check
        expected_cols = EXPECTED_SCHEMAS.get(table_name, [])
        missing_cols  = set(expected_cols) - set(headers)
        if missing_cols:
            result.errors.append(f"Missing required columns: {sorted(missing_cols)}")

        # Row validation
        row_count    = 0
        error_count  = 0
        null_keys    = {col: 0 for col in expected_cols[:3]}  # Check first 3 expected cols

        for i, row in enumerate(reader):
            row_count += 1

            # Check for excessively short rows (indicates parse errors)
            if len(row) < len(headers) * 0.5:
                error_count += 1
                if error_count <= 5:
                    result.warnings.append(f"Row {i+2} appears malformed (only {len(row)}/{len(headers)} fields)")

            # Track nulls in key columns
            for col in list(null_keys.keys()):
                if col in row and (not row[col] or row[col].strip() in ("", "NULL", "null")):
                    null_keys[col] += 1

            if row_count > 1_000_000:
                result.warnings.append("File contains >1M rows — sampling stopped at 1M")
                break

        result.row_count  = row_count
        result.error_count = error_count

        # Check null rates for key columns
        for col, null_count in null_keys.items():
            if row_count > 0:
                null_rate = null_count / row_count
                if null_rate > 0.05:
                    result.errors.append(
                        f"High null rate in '{col}': {null_rate:.1%} ({null_count}/{row_count})"
                    )

        if missing_cols or (error_count / max(row_count, 1)) > 0.1:
            result.status = "INVALID"
        else:
            result.status = "VALID"

    except Exception as e:
        result.errors.append(f"CSV parse error: {str(e)}")
        result.status = "INVALID"


def validate_json_lines(
    content_bytes: bytes,
    table_name: str,
    result: ValidationResult,
) -> None:
    """Validate JSON Lines format."""
    try:
        text = content_bytes.decode("utf-8", errors="replace")
        expected_cols = EXPECTED_SCHEMAS.get(table_name, [])

        row_count   = 0
        error_count = 0

        for i, line in enumerate(text.splitlines()):
            line = line.strip()
            if not line:
                continue

            try:
                obj = json.loads(line)
                missing = set(expected_cols[:3]) - set(obj.keys())
                if missing:
                    error_count += 1
                row_count += 1
            except json.JSONDecodeError as e:
                error_count += 1
                if error_count <= 5:
                    result.errors.append(f"Line {i+1} JSON parse error: {e}")

        result.row_count  = row_count
        result.error_count = error_count

        error_rate = error_count / max(row_count, 1)
        if error_rate > 0.05:
            result.errors.append(
                f"High error rate: {error_rate:.1%} ({error_count}/{row_count} lines)"
            )
            result.status = "INVALID"
        else:
            result.status = "VALID"

    except Exception as e:
        result.errors.append(f"JSON Lines parse error: {str(e)}")
        result.status = "INVALID"


def get_object(bucket: str, key: str) -> Tuple[bytes, int]:
    """Download S3 object and return (content, size_bytes)."""
    try:
        response  = _s3_client.get_object(Bucket=bucket, Key=key)
        size      = response["ContentLength"]
        content   = response["Body"].read()
        return content, size
    except ClientError as e:
        raise RuntimeError(f"Failed to get s3://{bucket}/{key}: {e}") from e


def copy_to_processed(src_bucket: str, src_key: str, dst_bucket: str) -> str:
    """Copy a validated file from raw bucket to processed bucket."""
    # Build destination key — strip 'raw/' prefix, replace with partitioned path
    # Input:  raw/orders/year=2024/month=01/day=15/orders_20240115.csv
    # Output: orders/year=2024/month=01/day=15/orders_20240115.parquet (or .csv)
    parts  = src_key.split("/", 1)  # ["raw", "orders/year=..."]
    dst_key = parts[1] if len(parts) > 1 else src_key

    try:
        _s3_client.copy_object(
            CopySource = {"Bucket": src_bucket, "Key": src_key},
            Bucket     = dst_bucket,
            Key        = dst_key,
        )
        logger.info(f"Copied to processed: s3://{dst_bucket}/{dst_key}")
        return dst_key
    except ClientError as e:
        raise RuntimeError(f"Failed to copy to processed bucket: {e}") from e


def quarantine_file(
    src_bucket: str,
    src_key: str,
    dst_bucket: str,
    result: ValidationResult,
) -> str:
    """Move invalid file to quarantine and write error report alongside it."""
    timestamp = datetime.now(timezone.utc).strftime("%Y/%m/%d/%H%M%S")
    file_name = src_key.split("/")[-1]
    dst_key   = f"{result.table_name or 'unknown'}/{timestamp}/{file_name}"
    report_key = f"{result.table_name or 'unknown'}/{timestamp}/{file_name}.validation_report.json"

    # Copy file to quarantine
    try:
        _s3_client.copy_object(
            CopySource = {"Bucket": src_bucket, "Key": src_key},
            Bucket     = dst_bucket,
            Key        = dst_key,
        )
    except ClientError as e:
        logger.error(f"Failed to copy to quarantine: {e}")

    # Write validation report alongside the quarantined file
    try:
        _s3_client.put_object(
            Bucket      = dst_bucket,
            Key         = report_key,
            Body        = json.dumps(asdict(result), indent=2).encode("utf-8"),
            ContentType = "application/json",
        )
    except ClientError as e:
        logger.warning(f"Failed to write quarantine report: {e}")

    logger.warning(f"Quarantined invalid file to s3://{dst_bucket}/{dst_key}")
    return dst_key


def send_failure_alert(result: ValidationResult) -> None:
    """Publish SNS alert for validation failures."""
    if not ALERTS_TOPIC_ARN:
        return

    message = {
        "source":       "data_validator",
        "environment":  ENVIRONMENT,
        "alert_type":   "validation_failure",
        "s3_key":       result.s3_key,
        "table_name":   result.table_name,
        "row_count":    result.row_count,
        "error_count":  result.error_count,
        "errors":       result.errors[:10],  # Truncate to 10 errors
        "timestamp":    result.validated_at,
    }

    try:
        _sns_client.publish(
            TopicArn = ALERTS_TOPIC_ARN,
            Subject  = f"[{ENVIRONMENT.upper()}] Data Validation Failure: {result.table_name}",
            Message  = json.dumps(message, indent=2),
        )
    except ClientError as e:
        logger.error(f"Failed to send SNS alert: {e}")


# ─────────────────────────────────────────────────────────────
# Lambda Handler
# ─────────────────────────────────────────────────────────────

def process_s3_record(record: Dict) -> ValidationResult:
    """Process a single S3 PutObject event record."""
    bucket = record["s3"]["bucket"]["name"]
    key    = record["s3"]["object"]["key"]

    # URL-decode the key (S3 events URL-encode special characters)
    from urllib.parse import unquote_plus
    key = unquote_plus(key)

    logger.info(f"Validating s3://{bucket}/{key}")

    result = ValidationResult(
        s3_key     = key,
        table_name = detect_table_name(key) or "unknown",
        status     = "UNKNOWN",
    )

    # ── Download file ─────────────────────────────────────────
    try:
        content, file_size = get_object(bucket, key)
    except RuntimeError as e:
        result.errors.append(str(e))
        result.status = "INVALID"
        return result

    result.file_size_bytes = file_size
    result.content_hash    = hashlib.md5(content).hexdigest()

    # Size guard
    if file_size > MAX_FILE_SIZE_BYTES:
        result.warnings.append(
            f"File size {file_size / 1024 / 1024:.1f} MB exceeds validation limit "
            f"({MAX_FILE_SIZE_BYTES / 1024 / 1024:.0f} MB) — performing header-only check"
        )
        content = content[:1024 * 256]  # Check first 256 KB only

    # ── Detect format ─────────────────────────────────────────
    file_format = detect_file_format(key, content)
    logger.info(f"Detected format: {file_format} for {key}")

    # ── Skip Parquet (Glue validates schema on read) ──────────
    if file_format == "parquet":
        result.status   = "VALID"
        result.row_count = -1  # Unknown without Spark
        return result

    if file_format == "parquet_invalid":
        result.errors.append("File has .parquet extension but invalid magic bytes")
        result.status = "INVALID"
        return result

    # ── Validate by format ────────────────────────────────────
    if file_format in ("csv", "gzip"):
        validate_csv(content, result.table_name, result, file_format)
    elif file_format == "json":
        validate_json_lines(content, result.table_name, result)
    else:
        result.warnings.append(f"Unknown file format '{file_format}' — skipping content validation")
        result.status = "VALID"

    # ── Unknown table → treat as warning, not failure ─────────
    if result.table_name == "unknown":
        result.warnings.append(
            f"Could not identify table from path '{key}'. "
            "Routing to processed zone without schema validation."
        )
        if result.status == "UNKNOWN":
            result.status = "VALID"

    return result


def lambda_handler(event: Dict, context) -> Dict:
    """
    Main Lambda handler.
    Processes S3 PutObject notifications from EventBridge or direct S3 trigger.
    """
    records = event.get("Records", [])
    if not records:
        # Could be an EventBridge event
        if "detail" in event:
            records = [{"s3": {"bucket": {"name": event["detail"]["bucket"]["name"]},
                               "object": {"key":    event["detail"]["object"]["key"]}}}]

    if not records:
        logger.warning("No S3 records found in event")
        return {"statusCode": 200, "body": "No records to process"}

    logger.info(f"Processing {len(records)} S3 object(s)")

    results    = []
    failures   = []
    start_time = time.time()

    for record in records:
        try:
            result = process_s3_record(record)
            results.append(result)

            bucket = record["s3"]["bucket"]["name"]
            key    = record["s3"]["object"]["key"]

            if result.status == "VALID":
                copy_to_processed(bucket, key, PROCESSED_BUCKET)
                logger.info(f"Validated and moved to processed: {key}")

            else:  # INVALID or UNKNOWN
                quarantine_file(bucket, key, QUARANTINE_BUCKET, result)
                send_failure_alert(result)
                failures.append({"key": key, "errors": result.errors})
                logger.warning(
                    f"File INVALID: {key} — {len(result.errors)} errors: {result.errors[:3]}"
                )

        except Exception as e:
            logger.error(f"Failed to process record: {e}", exc_info=True)
            failures.append({"key": "unknown", "error": str(e)})

    elapsed = (time.time() - start_time) * 1000
    summary = {
        "processed": len(results),
        "valid":     sum(1 for r in results if r.status == "VALID"),
        "invalid":   sum(1 for r in results if r.status == "INVALID"),
        "duration_ms": round(elapsed),
    }
    logger.info(f"Validation complete: {summary}")

    return {
        "statusCode": 200 if not failures else 207,
        "body": json.dumps(summary),
        "failures": failures,
    }

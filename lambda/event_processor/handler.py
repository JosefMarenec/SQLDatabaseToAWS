"""
Real-Time E-Commerce Analytics Platform
Lambda Function: event_processor/handler.py

Triggered by SQS (batches of up to 100 messages from the event queue).
Each message represents a clickstream event from the front-end or mobile app.

Processing pipeline per batch:
  1. Deserialize and validate each event message
  2. Enrich events (resolve anonymous_id → customer_id if possible)
  3. Batch-insert valid events into RDS PostgreSQL events table
  4. Report partial batch item failures (SQS supports partial failure via messageId)
  5. Route unprocessable messages to DLQ via SQS SendMessage

Uses connection pooling (module-level psycopg2 connection) to reuse connections
across Lambda invocations within the same execution environment.

Runtime: Python 3.11
Memory: 512 MB
Timeout: 60 seconds
"""

import json
import os
import logging
import time
import uuid
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, timezone
from contextlib import contextmanager

import boto3
import psycopg2
import psycopg2.extras
from botocore.exceptions import ClientError

# ─────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────
logger = logging.getLogger(__name__)
logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))

RDS_SECRET_NAME = os.environ["RDS_SECRET_NAME"]
RDS_HOST        = os.environ["RDS_HOST"]
RDS_PORT        = int(os.environ.get("RDS_PORT", "5432"))
DB_NAME         = os.environ.get("DB_NAME", "ecommerce")
DLQ_URL         = os.environ.get("DLQ_URL", "")
ALERTS_TOPIC    = os.environ.get("ALERTS_TOPIC_ARN", "")
ENVIRONMENT     = os.environ.get("ENVIRONMENT", "prod")
AWS_REGION      = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")

# Valid event types (must match ENUM in PostgreSQL)
VALID_EVENT_TYPES = {
    "page_view", "product_view", "add_to_cart", "remove_from_cart",
    "checkout_start", "checkout_step", "purchase", "search",
    "wishlist_add", "coupon_applied", "session_start", "session_end",
}

# ─────────────────────────────────────────────────────────────
# Module-level connection pool (warm Lambda reuse)
# ─────────────────────────────────────────────────────────────
_db_credentials: Optional[Dict] = None
_db_connection: Optional[psycopg2.extensions.connection] = None

_secrets_client = boto3.client("secretsmanager", region_name=AWS_REGION)
_sqs_client     = boto3.client("sqs",            region_name=AWS_REGION)
_sns_client     = boto3.client("sns",            region_name=AWS_REGION)
_cloudwatch     = boto3.client("cloudwatch",     region_name=AWS_REGION)


def get_db_credentials() -> Dict:
    """Fetch DB credentials from Secrets Manager (cached per Lambda container)."""
    global _db_credentials
    if _db_credentials is not None:
        return _db_credentials

    try:
        response = _secrets_client.get_secret_value(SecretId=RDS_SECRET_NAME)
        _db_credentials = json.loads(response["SecretString"])
        logger.info("DB credentials loaded from Secrets Manager")
        return _db_credentials
    except ClientError as e:
        logger.error(f"Failed to get credentials: {e}")
        raise


def get_db_connection() -> psycopg2.extensions.connection:
    """
    Return a module-level database connection, creating or reconnecting if needed.
    This pattern allows connection reuse across warm Lambda invocations.
    """
    global _db_connection

    if _db_connection is not None:
        try:
            # Ping the connection to verify it's still alive
            _db_connection.cursor().execute("SELECT 1")
            return _db_connection
        except Exception:
            logger.warning("Stale DB connection detected — reconnecting")
            try:
                _db_connection.close()
            except Exception:
                pass
            _db_connection = None

    creds = get_db_credentials()
    _db_connection = psycopg2.connect(
        host     = RDS_HOST,
        port     = RDS_PORT,
        dbname   = DB_NAME,
        user     = creds["username"],
        password = creds["password"],
        sslmode  = "require",
        connect_timeout = 10,
        options  = "-c statement_timeout=30000",  # 30s per statement
        application_name = f"lambda-event-processor-{ENVIRONMENT}",
    )
    _db_connection.autocommit = False
    logger.info("New DB connection established")
    return _db_connection


@contextmanager
def db_transaction():
    """Context manager for database transactions with automatic rollback on error."""
    conn = get_db_connection()
    try:
        yield conn
        conn.commit()
    except Exception as e:
        conn.rollback()
        logger.error(f"Transaction rolled back: {e}")
        raise


# ─────────────────────────────────────────────────────────────
# Validation
# ─────────────────────────────────────────────────────────────

class ValidationError(Exception):
    pass


def validate_event(event: Dict) -> Dict:
    """
    Validate and normalize a single event payload.
    Returns the validated event dict or raises ValidationError.
    """
    # Required fields
    if "event_type" not in event:
        raise ValidationError("Missing required field: event_type")

    event_type = event["event_type"].lower().strip()
    if event_type not in VALID_EVENT_TYPES:
        raise ValidationError(
            f"Invalid event_type '{event_type}'. Must be one of: {VALID_EVENT_TYPES}"
        )

    # Normalize occurred_at
    occurred_at = event.get("occurred_at")
    if occurred_at:
        try:
            if isinstance(occurred_at, str):
                # Handle ISO 8601 with or without timezone
                if occurred_at.endswith("Z"):
                    occurred_at = occurred_at.replace("Z", "+00:00")
                event["occurred_at"] = datetime.fromisoformat(occurred_at)
            elif isinstance(occurred_at, (int, float)):
                # Unix timestamp in milliseconds
                event["occurred_at"] = datetime.fromtimestamp(
                    occurred_at / 1000, tz=timezone.utc
                )
        except (ValueError, TypeError) as e:
            raise ValidationError(f"Invalid occurred_at format: {occurred_at}") from e
    else:
        event["occurred_at"] = datetime.now(timezone.utc)

    # Validate UUIDs
    for uuid_field in ["event_id", "session_id", "customer_id", "anonymous_id", "product_id", "order_id"]:
        val = event.get(uuid_field)
        if val:
            try:
                event[uuid_field] = str(uuid.UUID(str(val)))
            except ValueError:
                logger.warning(f"Invalid UUID in {uuid_field}: {val} — setting to None")
                event[uuid_field] = None

    # Ensure event_id exists
    if not event.get("event_id"):
        event["event_id"] = str(uuid.uuid4())

    # Truncate oversized fields to match DB column limits
    if event.get("page_url") and len(event["page_url"]) > 2048:
        event["page_url"] = event["page_url"][:2048]
    if event.get("page_title") and len(event["page_title"]) > 500:
        event["page_title"] = event["page_title"][:500]
    if event.get("search_query") and len(event["search_query"]) > 500:
        event["search_query"] = event["search_query"][:500]

    # Convert properties to JSON string if it's a dict
    if isinstance(event.get("properties"), dict):
        event["properties"] = json.dumps(event["properties"])

    return event


# ─────────────────────────────────────────────────────────────
# Database Writes
# ─────────────────────────────────────────────────────────────

INSERT_EVENT_SQL = """
    INSERT INTO events (
        event_id,
        session_id,
        customer_id,
        anonymous_id,
        event_type,
        occurred_at,
        page_url,
        page_title,
        referrer_url,
        product_id,
        order_id,
        search_query,
        properties
    )
    VALUES %s
    ON CONFLICT DO NOTHING
"""

# Template for psycopg2.extras.execute_values
INSERT_TEMPLATE = (
    "(%(event_id)s, %(session_id)s, %(customer_id)s, %(anonymous_id)s, "
    "%(event_type)s, %(occurred_at)s, %(page_url)s, %(page_title)s, "
    "%(referrer_url)s, %(product_id)s, %(order_id)s, %(search_query)s, "
    "%(properties)s)"
)


def batch_insert_events(events: List[Dict]) -> int:
    """
    Insert a batch of validated events into RDS using execute_values for efficiency.
    Returns the number of rows inserted.
    """
    if not events:
        return 0

    # Normalize events to DB row format
    rows = []
    for event in events:
        rows.append({
            "event_id":      event.get("event_id"),
            "session_id":    event.get("session_id"),
            "customer_id":   event.get("customer_id"),
            "anonymous_id":  event.get("anonymous_id"),
            "event_type":    event["event_type"],
            "occurred_at":   event["occurred_at"],
            "page_url":      event.get("page_url"),
            "page_title":    event.get("page_title"),
            "referrer_url":  event.get("referrer_url"),
            "product_id":    event.get("product_id"),
            "order_id":      event.get("order_id"),
            "search_query":  event.get("search_query"),
            "properties":    event.get("properties"),
        })

    with db_transaction() as conn:
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(
                cur,
                INSERT_EVENT_SQL,
                rows,
                template=INSERT_TEMPLATE,
                page_size=500,   # 500 rows per INSERT statement
            )
            return cur.rowcount


# ─────────────────────────────────────────────────────────────
# DLQ and Alerting
# ─────────────────────────────────────────────────────────────

def send_to_dlq(message_body: str, message_id: str, error: str) -> None:
    """Forward unprocessable message to DLQ with error context."""
    if not DLQ_URL:
        logger.warning("DLQ_URL not configured — cannot forward failed message")
        return

    try:
        _sqs_client.send_message(
            QueueUrl    = DLQ_URL,
            MessageBody = json.dumps({
                "original_message": message_body,
                "original_message_id": message_id,
                "error": error,
                "failed_at": datetime.now(timezone.utc).isoformat(),
                "lambda_environment": ENVIRONMENT,
            }),
            MessageAttributes={
                "ErrorType": {"StringValue": "ValidationError", "DataType": "String"},
            }
        )
    except ClientError as e:
        logger.error(f"Failed to send message to DLQ: {e}")


def publish_metric(metric_name: str, value: float, unit: str = "Count") -> None:
    """Publish a custom CloudWatch metric."""
    try:
        _cloudwatch.put_metric_data(
            Namespace="EcomAnalytics/EventProcessor",
            MetricData=[{
                "MetricName": metric_name,
                "Value": value,
                "Unit": unit,
                "Dimensions": [
                    {"Name": "Environment", "Value": ENVIRONMENT},
                ]
            }]
        )
    except ClientError as e:
        logger.warning(f"Failed to publish metric {metric_name}: {e}")


# ─────────────────────────────────────────────────────────────
# Lambda Handler
# ─────────────────────────────────────────────────────────────

def lambda_handler(event: Dict, context) -> Dict:
    """
    Main Lambda entry point.

    SQS trigger delivers `event["Records"]` — a list of SQS message records.
    Returns `batchItemFailures` list so SQS retries only failed messages.
    """
    records = event.get("Records", [])
    logger.info(f"Received {len(records)} SQS messages")

    start_time = time.time()

    validated_events   = []
    failed_message_ids = []

    # ── Phase 1: Validate all messages ───────────────────────
    for record in records:
        message_id   = record["messageId"]
        message_body = record.get("body", "")

        try:
            payload = json.loads(message_body)

            # Support both single event and array of events per SQS message
            if isinstance(payload, list):
                event_list = payload
            elif isinstance(payload, dict):
                event_list = [payload]
            else:
                raise ValueError(f"Unexpected payload type: {type(payload)}")

            for evt in event_list:
                validated = validate_event(evt)
                validated_events.append(validated)

        except (json.JSONDecodeError, ValueError, ValidationError) as e:
            logger.warning(f"Message {message_id} failed validation: {e}")
            send_to_dlq(message_body, message_id, str(e))
            failed_message_ids.append({"itemIdentifier": message_id})

        except Exception as e:
            logger.error(f"Unexpected error processing message {message_id}: {e}", exc_info=True)
            failed_message_ids.append({"itemIdentifier": message_id})

    logger.info(
        f"Validation complete: {len(validated_events)} valid events, "
        f"{len(failed_message_ids)} failed messages"
    )

    # ── Phase 2: Batch write to RDS ──────────────────────────
    if validated_events:
        try:
            inserted = batch_insert_events(validated_events)
            logger.info(f"Inserted {inserted} events into RDS")
            publish_metric("EventsInserted", inserted)
        except Exception as e:
            logger.error(f"Batch insert failed: {e}", exc_info=True)
            # Mark ALL messages as failed so SQS retries them
            failed_message_ids = [
                {"itemIdentifier": r["messageId"]} for r in records
            ]
            publish_metric("BatchInsertFailures", 1)

    # ── Metrics ───────────────────────────────────────────────
    elapsed_ms = (time.time() - start_time) * 1000
    publish_metric("ProcessingTimeMs", elapsed_ms, "Milliseconds")
    publish_metric("ValidationFailures", len(failed_message_ids))

    logger.info(
        f"Handler complete in {elapsed_ms:.1f}ms. "
        f"batchItemFailures: {len(failed_message_ids)}"
    )

    # Return partial failure response — SQS will retry only the failed messages
    return {"batchItemFailures": failed_message_ids}

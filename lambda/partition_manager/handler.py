"""
Real-Time E-Commerce Analytics Platform
Lambda Function: partition_manager/handler.py

Triggered daily by EventBridge at midnight UTC.
Manages RDS PostgreSQL table partitions to ensure:
  1. Future partitions always exist (orders: 2 months ahead, events: 7 days ahead)
  2. Old partitions beyond retention are detached and dropped
  3. Glue catalog is updated with new partition metadata

EventBridge input payload:
{
    "action": "create_and_cleanup",
    "months_ahead": 2,
    "retention_months": 24
}

Runtime: Python 3.11
Memory: 256 MB
Timeout: 120 seconds
VPC: Yes (needs RDS access)
"""

import json
import os
import logging
from datetime import datetime, timezone, date, timedelta
from typing import Dict, List, Optional, Any
from dateutil.relativedelta import relativedelta

import boto3
import psycopg2
import psycopg2.extras
from botocore.exceptions import ClientError

# ─────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────
logger = logging.getLogger(__name__)
logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))

RDS_SECRET_NAME    = os.environ["RDS_SECRET_NAME"]
RDS_HOST           = os.environ["RDS_HOST"]
RDS_PORT           = int(os.environ.get("RDS_PORT", "5432"))
DB_NAME            = os.environ.get("DB_NAME", "ecommerce")
GLUE_DATABASE      = os.environ.get("GLUE_DATABASE", "")
ALERTS_TOPIC_ARN   = os.environ.get("ALERTS_TOPIC_ARN", "")
ENVIRONMENT        = os.environ.get("ENVIRONMENT", "prod")
AWS_REGION         = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")

# Defaults (can be overridden by EventBridge payload)
DEFAULT_MONTHS_AHEAD       = int(os.environ.get("MONTHS_AHEAD", "2"))
DEFAULT_RETENTION_MONTHS   = int(os.environ.get("PARTITION_RETENTION_MONTHS", "24"))
DEFAULT_EVENTS_DAYS_AHEAD  = 7
DEFAULT_EVENTS_RETENTION_DAYS = 90

# ─────────────────────────────────────────────────────────────
# AWS Clients
# ─────────────────────────────────────────────────────────────
_secrets_client = boto3.client("secretsmanager", region_name=AWS_REGION)
_glue_client    = boto3.client("glue",           region_name=AWS_REGION)
_sns_client     = boto3.client("sns",            region_name=AWS_REGION)

_db_credentials: Optional[Dict] = None
_db_connection  = None


def get_credentials() -> Dict:
    """Fetch DB credentials (cached per Lambda container)."""
    global _db_credentials
    if _db_credentials:
        return _db_credentials
    response = _secrets_client.get_secret_value(SecretId=RDS_SECRET_NAME)
    _db_credentials = json.loads(response["SecretString"])
    return _db_credentials


def get_db_connection():
    """Get or create a PostgreSQL connection."""
    global _db_connection
    if _db_connection:
        try:
            _db_connection.cursor().execute("SELECT 1")
            return _db_connection
        except Exception:
            try:
                _db_connection.close()
            except Exception:
                pass
            _db_connection = None

    creds = get_credentials()
    _db_connection = psycopg2.connect(
        host     = RDS_HOST,
        port     = RDS_PORT,
        dbname   = DB_NAME,
        user     = creds["username"],
        password = creds["password"],
        sslmode  = "require",
        connect_timeout = 15,
        application_name = f"lambda-partition-manager-{ENVIRONMENT}",
    )
    _db_connection.autocommit = True   # DDL statements need autocommit
    return _db_connection


def send_alert(subject: str, message: str) -> None:
    """Send SNS alert."""
    if not ALERTS_TOPIC_ARN:
        return
    try:
        _sns_client.publish(
            TopicArn = ALERTS_TOPIC_ARN,
            Subject  = f"[{ENVIRONMENT.upper()}] Partition Manager: {subject}",
            Message  = message,
        )
    except ClientError as e:
        logger.error(f"Failed to send alert: {e}")


# ─────────────────────────────────────────────────────────────
# Partition Management Functions
# ─────────────────────────────────────────────────────────────

def create_orders_partition(year: int, month: int) -> Dict[str, Any]:
    """Create a monthly orders partition if it doesn't exist."""
    conn      = get_db_connection()
    part_name = f"orders_{year:04d}_{month:02d}"
    start_dt  = date(year, month, 1)
    end_dt    = start_dt + relativedelta(months=1)

    try:
        with conn.cursor() as cur:
            # Check if partition already exists
            cur.execute(
                """
                SELECT 1 FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE c.relname = %s AND n.nspname = 'public'
                """,
                (part_name,)
            )
            if cur.fetchone():
                return {"partition": part_name, "action": "skipped", "reason": "already_exists"}

            # Create partition
            cur.execute(
                f"""
                CREATE TABLE {part_name} PARTITION OF orders
                FOR VALUES FROM (%s) TO (%s)
                """,
                (start_dt.isoformat(), end_dt.isoformat())
            )

            # Create indexes
            cur.execute(
                f"CREATE INDEX idx_{part_name}_customer ON {part_name} (customer_id, created_at DESC)"
            )
            cur.execute(
                f"CREATE INDEX idx_{part_name}_status ON {part_name} (status) "
                f"WHERE status NOT IN ('delivered', 'cancelled')"
            )
            cur.execute(
                f"CREATE INDEX idx_{part_name}_brin ON {part_name} USING BRIN (created_at)"
            )

            logger.info(
                f"Created orders partition: {part_name} "
                f"({start_dt.isoformat()} to {end_dt.isoformat()})"
            )
            return {
                "partition": part_name,
                "action": "created",
                "range_start": start_dt.isoformat(),
                "range_end": end_dt.isoformat(),
            }

    except psycopg2.Error as e:
        logger.error(f"Failed to create partition {part_name}: {e}")
        return {"partition": part_name, "action": "failed", "error": str(e)}


def create_events_partition(dt: date) -> Dict[str, Any]:
    """Create a daily events partition if it doesn't exist."""
    conn      = get_db_connection()
    part_name = f"events_{dt.strftime('%Y_%m_%d')}"
    next_day  = dt + timedelta(days=1)

    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT 1 FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE c.relname = %s AND n.nspname = 'public'
                """,
                (part_name,)
            )
            if cur.fetchone():
                return {"partition": part_name, "action": "skipped", "reason": "already_exists"}

            cur.execute(
                f"""
                CREATE TABLE {part_name} PARTITION OF events
                FOR VALUES FROM (%s) TO (%s)
                """,
                (dt.isoformat(), next_day.isoformat())
            )

            cur.execute(
                f"CREATE INDEX idx_{part_name}_customer ON {part_name} (customer_id, occurred_at DESC)"
            )
            cur.execute(
                f"CREATE INDEX idx_{part_name}_type ON {part_name} (event_type, occurred_at)"
            )
            cur.execute(
                f"CREATE INDEX idx_{part_name}_session ON {part_name} (session_id)"
            )

            logger.info(f"Created events partition: {part_name} ({dt.isoformat()})")
            return {
                "partition": part_name,
                "action": "created",
                "date": dt.isoformat(),
            }

    except psycopg2.Error as e:
        logger.error(f"Failed to create events partition {part_name}: {e}")
        return {"partition": part_name, "action": "failed", "error": str(e)}


def drop_old_partitions(
    table_name: str,
    retention_months: int,
) -> List[Dict[str, Any]]:
    """
    Find and drop partitions older than retention_months.
    Returns list of actions taken.
    """
    conn    = get_db_connection()
    cutoff  = date.today() - relativedelta(months=retention_months)
    results = []

    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        # Get all child partitions
        cur.execute(
            """
            SELECT c.relname AS partition_name
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            JOIN pg_inherits i ON i.inhrelid = c.oid
            JOIN pg_class parent ON parent.oid = i.inhparent
            WHERE parent.relname = %s
              AND n.nspname = 'public'
              AND c.relname != %s || '_default'
            ORDER BY c.relname
            """,
            (table_name, table_name)
        )
        partitions = [row["partition_name"] for row in cur.fetchall()]

    for part_name in partitions:
        try:
            # Parse date from partition name
            suffix = part_name[len(table_name) + 1:]  # Strip "orders_" or "events_"

            if table_name == "orders":
                # Format: YYYY_MM
                part_date = datetime.strptime(suffix, "%Y_%m").date()
            elif table_name == "events":
                # Format: YYYY_MM_DD
                part_date = datetime.strptime(suffix, "%Y_%m_%d").date()
            else:
                continue

            if part_date < cutoff:
                logger.info(f"Dropping old partition: {part_name} (date: {part_date})")

                with get_db_connection().cursor() as cur:
                    # Log before dropping
                    cur.execute(
                        f"SELECT COUNT(*) FROM {part_name}"
                    )
                    row_count = cur.fetchone()[0]

                    # Detach first (allows VACUUM to continue on parent)
                    cur.execute(
                        f"ALTER TABLE {table_name} DETACH PARTITION {part_name}"
                    )
                    cur.execute(f"DROP TABLE IF EXISTS {part_name}")

                    # Log to partition maintenance table
                    cur.execute(
                        """
                        INSERT INTO partition_maintenance_log
                            (table_name, partition_name, action, executed_at)
                        VALUES (%s, %s, 'DROPPED', NOW())
                        """,
                        (table_name, part_name)
                    )

                results.append({
                    "partition": part_name,
                    "action":    "dropped",
                    "date":      part_date.isoformat(),
                    "rows":      row_count,
                })

        except (ValueError, psycopg2.Error) as e:
            logger.warning(f"Could not process partition {part_name}: {e}")
            results.append({
                "partition": part_name,
                "action":    "failed",
                "error":     str(e),
            })

    return results


def update_glue_catalog_partitions(
    table_name: str,
    new_partitions: List[Dict],
    s3_bucket: str,
) -> None:
    """
    Register new Glue catalog partitions so Redshift Spectrum can query them.
    Only called for the data lake S3 partitions (not RDS partitions).
    """
    if not GLUE_DATABASE or not new_partitions:
        return

    batch_partitions = []
    for part_info in new_partitions:
        if part_info.get("action") != "created":
            continue

        if table_name == "events" and "date" in part_info:
            dt   = datetime.strptime(part_info["date"], "%Y-%m-%d")
            vals = [str(dt.year), f"{dt.month:02d}", f"{dt.day:02d}"]
            s3_loc = (
                f"s3://{s3_bucket}/{table_name}/"
                f"year={dt.year}/month={dt.month:02d}/day={dt.day:02d}/"
            )
        else:
            continue

        batch_partitions.append({
            "Values": vals,
            "StorageDescriptor": {
                "Location": s3_loc,
                "InputFormat":  "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
                "SerdeInfo": {
                    "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
                },
            }
        })

    if not batch_partitions:
        return

    # Batch create in chunks of 100 (Glue API limit)
    for i in range(0, len(batch_partitions), 100):
        batch = batch_partitions[i:i + 100]
        try:
            _glue_client.batch_create_partition(
                DatabaseName    = GLUE_DATABASE,
                TableName       = table_name,
                PartitionInputList = batch,
            )
            logger.info(
                f"Registered {len(batch)} Glue partitions for {table_name}"
            )
        except ClientError as e:
            if "AlreadyExistsException" in str(e):
                logger.debug(f"Some Glue partitions already exist — skipping")
            else:
                logger.warning(f"Failed to register Glue partitions: {e}")


# ─────────────────────────────────────────────────────────────
# Lambda Handler
# ─────────────────────────────────────────────────────────────

def lambda_handler(event: Dict, context) -> Dict:
    """
    Main handler.
    Creates future partitions and drops old ones based on retention policy.
    """
    logger.info(f"Partition manager triggered. Event: {json.dumps(event)}")

    # Parse parameters from EventBridge payload
    months_ahead       = event.get("months_ahead",       DEFAULT_MONTHS_AHEAD)
    retention_months   = event.get("retention_months",   DEFAULT_RETENTION_MONTHS)
    events_days_ahead  = event.get("events_days_ahead",  DEFAULT_EVENTS_DAYS_AHEAD)
    events_retention_days = event.get("events_retention_days", DEFAULT_EVENTS_RETENTION_DAYS)
    dry_run            = event.get("dry_run", False)

    if dry_run:
        logger.info("DRY RUN mode — no partitions will be created or dropped")

    results = {
        "orders_created": [],
        "orders_dropped": [],
        "events_created": [],
        "events_dropped": [],
        "errors": [],
        "executed_at": datetime.now(timezone.utc).isoformat(),
    }

    # ── Create future orders partitions ──────────────────────
    today = date.today()
    for i in range(months_ahead + 1):
        future = today + relativedelta(months=i)
        if not dry_run:
            result = create_orders_partition(future.year, future.month)
        else:
            result = {"partition": f"orders_{future.year}_{future.month:02d}", "action": "dry_run"}
        results["orders_created"].append(result)
        if result.get("action") == "failed":
            results["errors"].append(result)

    # ── Create future events partitions ──────────────────────
    for i in range(events_days_ahead):
        future_day = today + timedelta(days=i)
        if not dry_run:
            result = create_events_partition(future_day)
        else:
            result = {"partition": f"events_{future_day.strftime('%Y_%m_%d')}", "action": "dry_run"}
        results["events_created"].append(result)
        if result.get("action") == "failed":
            results["errors"].append(result)

    # ── Drop old partitions ───────────────────────────────────
    if not dry_run:
        orders_dropped = drop_old_partitions("orders", retention_months)
        results["orders_dropped"] = orders_dropped

        events_retention_months = max(1, events_retention_days // 30)
        events_dropped = drop_old_partitions("events", events_retention_months)
        results["events_dropped"] = events_dropped

    # ── Summary ───────────────────────────────────────────────
    created_count = (
        sum(1 for r in results["orders_created"] if r.get("action") == "created") +
        sum(1 for r in results["events_created"] if r.get("action") == "created")
    )
    dropped_count = (
        sum(1 for r in results["orders_dropped"] if r.get("action") == "dropped") +
        sum(1 for r in results["events_dropped"] if r.get("action") == "dropped")
    )
    error_count = len(results["errors"])

    summary = (
        f"Partitions created: {created_count}, dropped: {dropped_count}, errors: {error_count}. "
        f"Retention: orders={retention_months}mo, events={events_retention_days}d. "
        f"dry_run={dry_run}"
    )
    logger.info(summary)

    if error_count > 0:
        send_alert(
            "Partition creation failures",
            f"{error_count} partition operations failed:\n" +
            json.dumps(results["errors"], indent=2)
        )

    return {
        "statusCode": 200 if error_count == 0 else 207,
        "body": summary,
        "details": results,
    }

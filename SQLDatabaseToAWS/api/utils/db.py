"""
Database connection utility.

Uses a module-level psycopg2 connection that is reused across Lambda
invocations within the same execution environment (warm starts).
Credentials are fetched once from AWS Secrets Manager and cached.
"""

import json
import logging
import os
from contextlib import contextmanager
from typing import Optional

import boto3
import psycopg2
import psycopg2.extras
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)

# Module-level cached objects (survive warm Lambda re-use)
_connection: Optional[psycopg2.extensions.connection] = None
_db_credentials: Optional[dict] = None


def _get_credentials() -> dict:
    """Fetch DB credentials from Secrets Manager (cached after first call)."""
    global _db_credentials
    if _db_credentials:
        return _db_credentials

    secret_name = os.environ["RDS_SECRET_NAME"]
    region = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")

    client = boto3.client("secretsmanager", region_name=region)
    try:
        response = client.get_secret_value(SecretId=secret_name)
        _db_credentials = json.loads(response["SecretString"])
        logger.info("DB credentials loaded from Secrets Manager.")
        return _db_credentials
    except ClientError as exc:
        logger.error("Failed to fetch secret %s: %s", secret_name, exc)
        raise


def get_connection() -> psycopg2.extensions.connection:
    """
    Return a live psycopg2 connection, creating or reconnecting as needed.
    Connection is kept alive across warm Lambda invocations.
    """
    global _connection

    if _connection and not _connection.closed:
        try:
            # Cheap liveness check
            _connection.cursor().execute("SELECT 1")
            return _connection
        except psycopg2.OperationalError:
            logger.warning("Stale connection detected — reconnecting.")
            _connection = None

    creds = _get_credentials()
    _connection = psycopg2.connect(
        host=os.environ["RDS_HOST"],
        port=int(os.environ.get("RDS_PORT", "5432")),
        dbname=os.environ.get("DB_NAME", "ecommerce"),
        user=creds["username"],
        password=creds["password"],
        connect_timeout=5,
        options="-c search_path=public",
    )
    _connection.autocommit = False
    logger.info("New DB connection established.")
    return _connection


@contextmanager
def get_cursor(commit: bool = False):
    """
    Context manager yielding a RealDictCursor.

    Usage:
        with get_cursor(commit=True) as cur:
            cur.execute("INSERT INTO ...")

    Rolls back automatically on exception.
    """
    conn = get_connection()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        yield cur
        if commit:
            conn.commit()
        else:
            conn.rollback()   # read-only: release any implicit txn
    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()

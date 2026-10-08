"""Database state tracking module supporting SQLite and AWS DynamoDB."""

from __future__ import annotations

import logging
import sqlite3
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from ctis_etl import config

logger = logging.getLogger(__name__)

# Lazy DynamoDB table
_dynamodb_table = None


def get_dynamodb_table():
    """Initializes and returns boto3 DynamoDB Table resource if available."""
    global _dynamodb_table
    if _dynamodb_table is not None:
        return _dynamodb_table

    try:
        import boto3
        from botocore.config import Config
        session = boto3.Session(
            aws_access_key_id=config.AWS_ACCESS_KEY_ID or None,
            aws_secret_access_key=config.AWS_SECRET_ACCESS_KEY or None,
            region_name=config.AWS_REGION or None,
        )
        dynamodb = session.resource(
            "dynamodb",
            config=Config(retries={"max_attempts": 4, "mode": "adaptive"})
        )
        _dynamodb_table = dynamodb.Table(config.DYNAMODB_TABLE_NAME)
        return _dynamodb_table
    except ImportError:
        logger.warning("boto3 is not installed. DynamoDB state tracking disabled.")
        return None
    except Exception as e:
        logger.error(f"Failed connecting to DynamoDB table '{config.DYNAMODB_TABLE_NAME}': {e}")
        return None


def init_sqlite_db() -> None:
    """Creates SQLite tracking tables and indexes if they do not exist."""
    with sqlite3.connect(config.SQLITE_DB_PATH) as conn:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA busy_timeout=30000;")
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS trials (
                ct_number TEXT PRIMARY KEY,
                status TEXT NOT NULL,
                retry_count INTEGER DEFAULT 0,
                last_publish_date TEXT,
                last_fetched_at TEXT,
                error_message TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
        """)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_trials_status ON trials(status);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_trials_updated ON trials(updated_at);")


def mark_processing(ct_number: str) -> None:
    """Marks trial as currently in-flight to prevent duplicate worker pickup."""
    now_iso = datetime.now(timezone.utc).isoformat()
    init_sqlite_db()
    with sqlite3.connect(config.SQLITE_DB_PATH) as conn:
        conn.execute(
            "UPDATE trials SET status = 'PROCESSING', updated_at = ? WHERE ct_number = ?",
            (now_iso, ct_number)
        )
        conn.commit()


def reset_stale_processing(timeout_minutes: int = 15) -> int:
    """Recovers trials left in PROCESSING state if a container or worker crashed."""
    init_sqlite_db()
    from datetime import timedelta
    cutoff = (datetime.now(timezone.utc) - timedelta(minutes=timeout_minutes)).isoformat()
    now_iso = datetime.now(timezone.utc).isoformat()
    with sqlite3.connect(config.SQLITE_DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute(
            "UPDATE trials SET status = 'PENDING', updated_at = ? WHERE status = 'PROCESSING' AND updated_at < ?",
            (now_iso, cutoff)
        )
        conn.commit()
        return cursor.rowcount
        conn.execute("PRAGMA busy_timeout=15000;")
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS trials (
                ct_number TEXT PRIMARY KEY,
                status TEXT NOT NULL,
                retry_count INTEGER DEFAULT 0,
                last_publish_date TEXT,
                last_fetched_at TEXT,
                error_message TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
        """)
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_trials_status ON trials(status);")

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS pipeline_runs (
                run_id TEXT PRIMARY KEY,
                run_type TEXT NOT NULL,
                started_at TEXT NOT NULL,
                completed_at TEXT,
                status TEXT NOT NULL,
                trials_discovered INTEGER DEFAULT 0,
                trials_processed INTEGER DEFAULT 0,
                trials_succeeded INTEGER DEFAULT 0,
                trials_failed INTEGER DEFAULT 0
            );
        """)
        conn.commit()


def get_trial_status(ct_number: str) -> Optional[Dict[str, Any]]:
    """Fetches the state record for a single trial."""
    init_sqlite_db()
    with sqlite3.connect(config.SQLITE_DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM trials WHERE ct_number = ?", (ct_number,))
        row = cursor.fetchone()
        if row:
            return dict(row)
    return None


def stage_trial(ct_number: str, api_publish_date: Optional[str]) -> str:
    """Determines whether a trial is new, updated, or up to date.

    Stages the trial in DB as PENDING or UPDATE_PENDING if processing is required.
    Returns: 'PENDING', 'UPDATE_PENDING', or 'UNCHANGED'
    """
    init_sqlite_db()
    now_iso = datetime.now(timezone.utc).isoformat()
    existing = get_trial_status(ct_number)

    if existing is None:
        # Brand new trial
        with sqlite3.connect(config.SQLITE_DB_PATH) as conn:
            conn.execute(
                """
                INSERT INTO trials (ct_number, status, retry_count, last_publish_date, created_at, updated_at)
                VALUES (?, 'PENDING', 0, ?, ?, ?)
                """,
                (ct_number, api_publish_date, now_iso, now_iso),
            )
            conn.commit()
        return "PENDING"

    current_status = existing.get("status")
    db_date = existing.get("last_publish_date")

    # If already successfully fetched and publication date hasn't changed, skip
    if current_status == "SUCCESS" and db_date == api_publish_date and api_publish_date is not None:
        return "UNCHANGED"

    # Needs update or retry
    new_status = "UPDATE_PENDING" if current_status == "SUCCESS" else "PENDING"
    with sqlite3.connect(config.SQLITE_DB_PATH) as conn:
        conn.execute(
            """
            UPDATE trials
            SET status = ?, last_publish_date = ?, updated_at = ?
            WHERE ct_number = ?
            """,
            (new_status, api_publish_date, now_iso, ct_number),
        )
        conn.commit()

    return new_status


def mark_success(ct_number: str, last_publish_date: Optional[str]) -> None:
    """Updates trial state to SUCCESS in SQLite and DynamoDB."""
    now_iso = datetime.now(timezone.utc).isoformat()
    init_sqlite_db()

    # Update SQLite
    with sqlite3.connect(config.SQLITE_DB_PATH) as conn:
        conn.execute(
            """
            INSERT INTO trials (ct_number, status, retry_count, last_publish_date, last_fetched_at, error_message, created_at, updated_at)
            VALUES (?, 'SUCCESS', 0, ?, ?, NULL, ?, ?)
            ON CONFLICT(ct_number) DO UPDATE SET
                status = 'SUCCESS',
                retry_count = 0,
                last_publish_date = COALESCE(excluded.last_publish_date, trials.last_publish_date),
                last_fetched_at = excluded.last_fetched_at,
                error_message = NULL,
                updated_at = excluded.updated_at
            """,
            (ct_number, last_publish_date, now_iso, now_iso, now_iso),
        )
        conn.commit()

    # Update DynamoDB if enabled
    if config.STATE_BACKEND in ("dynamodb", "both"):
        table = get_dynamodb_table()
        if table:
            try:
                table.put_item(
                    Item={
                        config.DYNAMODB_PARTITION_KEY: ct_number,
                        "ct_number": ct_number,
                        "status": "SUCCESS",
                        "retry_count": 0,
                        "last_publish_date": last_publish_date or "",
                        "last_fetched_at": now_iso,
                        "updated_at": now_iso,
                    }
                )
            except Exception as e:
                logger.error(f"DynamoDB put_item failed for {ct_number}: {e}")


def mark_failure(ct_number: str, error_message: str) -> None:
    """Increments retry count and updates trial state to FAILED if threshold exceeded."""
    now_iso = datetime.now(timezone.utc).isoformat()
    init_sqlite_db()

    with sqlite3.connect(config.SQLITE_DB_PATH) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT retry_count FROM trials WHERE ct_number = ?", (ct_number,))
        row = cursor.fetchone()
        retry_count = (row[0] + 1) if row else 1
        new_status = "FAILED" if retry_count >= 3 else "PENDING"

        cursor.execute(
            """
            INSERT INTO trials (ct_number, status, retry_count, error_message, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(ct_number) DO UPDATE SET
                status = excluded.status,
                retry_count = excluded.retry_count,
                error_message = excluded.error_message,
                updated_at = excluded.updated_at
            """,
            (ct_number, new_status, retry_count, error_message, now_iso, now_iso),
        )
        conn.commit()

    if config.STATE_BACKEND in ("dynamodb", "both"):
        table = get_dynamodb_table()
        if table:
            try:
                table.put_item(
                    Item={
                        config.DYNAMODB_PARTITION_KEY: ct_number,
                        "ct_number": ct_number,
                        "status": new_status,
                        "retry_count": retry_count,
                        "error_message": error_message,
                        "updated_at": now_iso,
                    }
                )
            except Exception as e:
                logger.error(f"DynamoDB failure update failed for {ct_number}: {e}")


def get_pending_trials(filter_type: Optional[str] = None) -> List[str]:
    """Retrieves list of ct_numbers ready for processing.

    Args:
        filter_type: 'new' (only PENDING), 'updates' (only UPDATE_PENDING), or None (both)
    """
    init_sqlite_db()
    with sqlite3.connect(config.SQLITE_DB_PATH) as conn:
        cursor = conn.cursor()
        if filter_type == "new":
            cursor.execute(
                """
                SELECT ct_number FROM trials
                WHERE status = 'PENDING'
                ORDER BY retry_count ASC, updated_at ASC
                """
            )
        elif filter_type in ("update", "updates"):
            cursor.execute(
                """
                SELECT ct_number FROM trials
                WHERE status = 'UPDATE_PENDING'
                ORDER BY retry_count ASC, updated_at ASC
                """
            )
        else:
            cursor.execute(
                """
                SELECT ct_number FROM trials
                WHERE status IN ('PENDING', 'UPDATE_PENDING')
                ORDER BY retry_count ASC, updated_at ASC
                """
            )
        return [row[0] for row in cursor.fetchall()]


def record_pipeline_run(
    run_id: str,
    run_type: str,
    started_at: str,
    completed_at: Optional[str] = None,
    status: str = "RUNNING",
    discovered: int = 0,
    processed: int = 0,
    succeeded: int = 0,
    failed: int = 0,
) -> None:
    """Logs pipeline run metrics for monitoring and auditing."""
    init_sqlite_db()
    with sqlite3.connect(config.SQLITE_DB_PATH) as conn:
        conn.execute(
            """
            INSERT INTO pipeline_runs (
                run_id, run_type, started_at, completed_at, status,
                trials_discovered, trials_processed, trials_succeeded, trials_failed
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(run_id) DO UPDATE SET
                completed_at = excluded.completed_at,
                status = excluded.status,
                trials_discovered = excluded.trials_discovered,
                trials_processed = excluded.trials_processed,
                trials_succeeded = excluded.trials_succeeded,
                trials_failed = excluded.trials_failed
            """,
            (run_id, run_type, started_at, completed_at, status, discovered, processed, succeeded, failed),
        )
        conn.commit()

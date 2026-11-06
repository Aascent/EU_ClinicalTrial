"""Database state tracking module supporting SQLite and AWS DynamoDB."""

from __future__ import annotations

import logging
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from anz_etl import config

logger = logging.getLogger(__name__)

_dynamodb_table = None


def get_dynamodb_table():
    """Initializes and returns boto3 DynamoDB Table resource if available."""
    global _dynamodb_table
    if _dynamodb_table is not None:
        return _dynamodb_table

    if not config.AWS_ACCESS_KEY_ID or not config.DYNAMODB_TABLE_NAME:
        return None

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
    except Exception as e:
        logger.error(f"Failed connecting to DynamoDB table '{config.DYNAMODB_TABLE_NAME}': {e}")
        return None


def get_sqlite_path() -> Path:
    """Returns the resolved SQLite database path."""
    path = config.SQLITE_DB_PATH
    if path.is_dir():
        path = path / "tracker.db"
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def init_sqlite_db() -> None:
    """Creates SQLite tracking tables and indexes if they do not exist."""
    db_path = get_sqlite_path()
    with sqlite3.connect(db_path) as conn:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA busy_timeout=15000;")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS anz_trials (
                actrn TEXT PRIMARY KEY,
                title TEXT,
                registration_date TEXT,
                last_updated_date TEXT,
                sync_status TEXT NOT NULL DEFAULT 'PENDING',
                attempts INTEGER DEFAULT 0,
                last_checked TEXT,
                error_message TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_anz_sync_status ON anz_trials(sync_status);
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_anz_updated_date ON anz_trials(last_updated_date);
        """)
        conn.commit()


def sync_state_from_dynamodb() -> int:
    """Restores tracked trials from DynamoDB into local SQLite state."""
    if config.STATE_BACKEND not in ("dynamodb", "both"):
        return 0
    table = get_dynamodb_table()
    if not table:
        return 0
    try:
        scanned_items = []
        resp = table.scan()
        scanned_items.extend(resp.get("Items", []))
        while "LastEvaluatedKey" in resp:
            resp = table.scan(ExclusiveStartKey=resp["LastEvaluatedKey"])
            scanned_items.extend(resp.get("Items", []))

        if not scanned_items:
            return 0

        init_sqlite_db()
        with sqlite3.connect(get_sqlite_path()) as conn:
            for item in scanned_items:
                actrn = item.get(config.DYNAMODB_PARTITION_KEY) or item.get("actrn")
                if not actrn:
                    continue
                conn.execute("""
                    INSERT OR IGNORE INTO anz_trials (
                        actrn, title, registration_date, last_updated_date,
                        sync_status, attempts, last_checked, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, 0, ?, ?, ?)
                """, (
                    actrn,
                    item.get("title", f"Trial {actrn}"),
                    item.get("registration_date", ""),
                    item.get("last_updated_date", ""),
                    item.get("sync_status", "SUCCESS"),
                    item.get("last_checked", ""),
                    item.get("created_at", ""),
                    item.get("updated_at", ""),
                ))
            conn.commit()
        logger.info(f"Synchronized {len(scanned_items)} trials from DynamoDB into local SQLite state.")
        return len(scanned_items)
    except Exception as e:
        logger.warning(f"Failed syncing from DynamoDB: {e}")
        return 0


def init_db() -> None:
    """Initializes state tracking databases."""
    if config.STATE_BACKEND in ("sqlite", "both"):
        init_sqlite_db()
    if config.STATE_BACKEND in ("dynamodb", "both"):
        sync_state_from_dynamodb()


def record_trial(
    actrn: str,
    title: Optional[str] = None,
    registration_date: Optional[str] = None,
    last_updated_date: Optional[str] = None,
    sync_status: str = "PENDING",
) -> None:
    """Inserts or updates initial trial discovery record."""
    now_iso = datetime.now(timezone.utc).isoformat()
    if config.STATE_BACKEND in ("sqlite", "both"):
        db_path = get_sqlite_path()
        with sqlite3.connect(db_path) as conn:
            conn.execute("""
                INSERT INTO anz_trials (
                    actrn, title, registration_date, last_updated_date,
                    sync_status, attempts, last_checked, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, 0, ?, ?, ?)
                ON CONFLICT(actrn) DO UPDATE SET
                    title=COALESCE(excluded.title, anz_trials.title),
                    sync_status=excluded.sync_status,
                    last_updated_date=COALESCE(excluded.last_updated_date, anz_trials.last_updated_date),
                    updated_at=excluded.updated_at
            """, (actrn, title, registration_date, last_updated_date, sync_status, now_iso, now_iso, now_iso))
            conn.commit()


    if config.STATE_BACKEND in ("dynamodb", "both"):
        table = get_dynamodb_table()
        if table:
            try:
                table.put_item(
                    Item={
                        config.DYNAMODB_PARTITION_KEY: actrn,
                        "title": title or f"Trial {actrn}",
                        "sync_status": sync_status,
                        "registration_date": registration_date or "",
                        "last_updated_date": last_updated_date or "",
                        "last_checked": now_iso,
                        "created_at": now_iso,
                        "updated_at": now_iso,
                    }
                )
            except Exception as e:
                logger.error(f"Failed recording DynamoDB item {actrn}: {e}")


def update_trial_status(
    actrn: str,
    status: str,
    error_message: Optional[str] = None,
    last_updated_date: Optional[str] = None,
) -> None:
    """Updates sync status for a given trial."""
    now_iso = datetime.now(timezone.utc).isoformat()
    if config.STATE_BACKEND in ("sqlite", "both"):
        db_path = get_sqlite_path()
        with sqlite3.connect(db_path) as conn:
            conn.execute("""
                UPDATE anz_trials
                SET sync_status = ?,
                    attempts = attempts + 1,
                    last_checked = ?,
                    error_message = ?,
                    last_updated_date = COALESCE(?, last_updated_date),
                    updated_at = ?
                WHERE actrn = ?
            """, (status, now_iso, error_message, last_updated_date, now_iso, actrn))
            conn.commit()

    if config.STATE_BACKEND in ("dynamodb", "both"):
        table = get_dynamodb_table()
        if table:
            try:
                table.update_item(
                    Key={config.DYNAMODB_PARTITION_KEY: actrn},
                    UpdateExpression="SET sync_status = :s, last_checked = :c, error_message = :e, updated_at = :u",
                    ExpressionAttributeValues={
                        ":s": status,
                        ":c": now_iso,
                        ":e": error_message or "",
                        ":u": now_iso,
                    }
                )
            except Exception as e:
                logger.error(f"Failed updating DynamoDB item {actrn}: {e}")


def get_stats() -> Dict[str, Any]:
    """Retrieves sync count summary."""
    stats = {"total": 0, "success": 0, "failed": 0, "pending": 0}
    if config.STATE_BACKEND in ("sqlite", "both"):
        db_path = get_sqlite_path()
        if db_path.exists():
            with sqlite3.connect(db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT sync_status, count(*) FROM anz_trials GROUP BY sync_status")
                for row in cursor.fetchall():
                    s = (row[0] or "").upper()
                    count = row[1]
                    stats["total"] += count
                    if s == "SUCCESS":
                        stats["success"] += count
                    elif s == "FAILED":
                        stats["failed"] += count
                    else:
                        stats["pending"] += count
    return stats


def mark_processing(actrn: str) -> None:
    """Marks a trial status as PROCESSING."""
    now_iso = datetime.now(timezone.utc).isoformat()
    if config.STATE_BACKEND in ("sqlite", "both"):
        db_path = get_sqlite_path()
        with sqlite3.connect(db_path) as conn:
            conn.execute("""
                UPDATE anz_trials
                SET sync_status = 'PROCESSING',
                    last_checked = ?,
                    updated_at = ?
                WHERE actrn = ?
            """, (now_iso, now_iso, actrn))
            conn.commit()


def get_pending_trials(limit: int = 100) -> List[str]:
    """Fetches list of pending trial ACTRNs."""
    pending = []
    if config.STATE_BACKEND in ("sqlite", "both"):
        db_path = get_sqlite_path()
        if db_path.exists():
            with sqlite3.connect(db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT actrn FROM anz_trials WHERE sync_status IN ('PENDING', 'PROCESSING') ORDER BY created_at ASC LIMIT ?",
                    (limit,)
                )
                for row in cursor.fetchall():
                    pending.append(row[0])
    return pending


def get_failed_trials(limit: int = 50) -> List[Dict[str, Any]]:
    """Fetches list of failed trials for retry."""
    failed = []
    if config.STATE_BACKEND in ("sqlite", "both"):
        db_path = get_sqlite_path()
        if db_path.exists():
            with sqlite3.connect(db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT actrn, attempts, error_message FROM anz_trials WHERE sync_status = 'FAILED' LIMIT ?",
                    (limit,)
                )
                for row in cursor.fetchall():
                    failed.append(dict(row))
    return failed


def get_recent_trials(limit: int = 50) -> List[Dict[str, Any]]:
    """Fetches list of recently tracked trials for the dashboard."""
    trials = []
    if config.STATE_BACKEND in ("sqlite", "both"):
        db_path = get_sqlite_path()
        if db_path.exists():
            with sqlite3.connect(db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT actrn, title, registration_date, last_updated_date, sync_status, attempts, last_checked, error_message, updated_at "
                    "FROM anz_trials ORDER BY updated_at DESC LIMIT ?",
                    (limit,)
                )
                for row in cursor.fetchall():
                    trials.append(dict(row))
    return trials


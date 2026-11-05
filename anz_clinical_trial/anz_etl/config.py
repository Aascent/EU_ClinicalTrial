"""Configuration module for ANZCTR ETL Pipeline.

Loads settings from environment variables and .env file.
"""

from __future__ import annotations

import os
from pathlib import Path

# Base Paths
BASE_DIR = Path(__file__).resolve().parent.parent


def _load_env_file() -> None:
    env_file = BASE_DIR / ".env"
    if not env_file.exists():
        return
    try:
        from dotenv import load_dotenv
        load_dotenv(env_file)
    except ImportError:
        with open(env_file, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if "=" in line:
                    key, val = line.split("=", 1)
                    key = key.strip()
                    val = val.strip().strip("'\"")
                    if key and key not in os.environ:
                        os.environ[key] = val


_load_env_file()

# Storage Directories
_data_dir_env = os.getenv("LOCAL_DATA_DIR")
if _data_dir_env:
    DATA_DIR = Path(_data_dir_env) if Path(_data_dir_env).is_absolute() else (BASE_DIR / _data_dir_env).resolve()
else:
    DATA_DIR = BASE_DIR / "data"

RAW_DIR = DATA_DIR / "raw"
BRONZE_DIR = DATA_DIR / "bronze"
SILVER_DIR = DATA_DIR / "silver"
QUARANTINE_DIR = BASE_DIR / "quarantine"
LOGS_DIR = BASE_DIR / "logs"

# SQLite DB Path (auto-resolves if volume-mounted as a directory by Docker)
_env_sqlite = os.getenv("SQLITE_DB_PATH")
if _env_sqlite:
    SQLITE_DB_PATH = Path(_env_sqlite) if Path(_env_sqlite).is_absolute() else (BASE_DIR / _env_sqlite).resolve()
else:
    SQLITE_DB_PATH = BASE_DIR / "tracker.db"

if SQLITE_DB_PATH.is_dir():
    SQLITE_DB_PATH = SQLITE_DB_PATH / "tracker.db"

# Ensure runtime directories exist
DATA_DIR.mkdir(parents=True, exist_ok=True)
RAW_DIR.mkdir(parents=True, exist_ok=True)
BRONZE_DIR.mkdir(parents=True, exist_ok=True)
SILVER_DIR.mkdir(parents=True, exist_ok=True)
QUARANTINE_DIR.mkdir(parents=True, exist_ok=True)
LOGS_DIR.mkdir(parents=True, exist_ok=True)
if SQLITE_DB_PATH.parent.exists() is False:
    SQLITE_DB_PATH.parent.mkdir(parents=True, exist_ok=True)

# ANZCTR API Endpoints
ANZCTR_WS_URL = os.getenv(
    "ANZCTR_WS_URL",
    "https://api.anzctr.org.au/WebServices/AnzctrWebservices.asmx",
).rstrip("/")
METHODOLOGY_URL = os.getenv(
    "METHODOLOGY_URL",
    "https://clinicaltrialsaustralia.io/methodology",
)

# AWS Configuration
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "")
AWS_REGION = os.getenv("AWS_REGION", "ap-southeast-2")

# Amazon S3 Configuration
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "")
S3_PREFIX = os.getenv("S3_PREFIX", "anzctr/").strip("/")
if S3_PREFIX:
    S3_PREFIX += "/"

S3_BRONZE_PREFIX = f"{S3_PREFIX}bronze/"
S3_SILVER_PREFIX = f"{S3_PREFIX}silver/"

# Amazon DynamoDB Configuration
DYNAMODB_TABLE_NAME = os.getenv("DYNAMODB_TABLE_NAME", "ANZ_Clinical")
DYNAMODB_PARTITION_KEY = os.getenv("DYNAMODB_PARTITION_KEY", "anzctr")

# Runtime & Pipeline Settings
STORAGE_BACKEND = os.getenv("STORAGE_BACKEND", "local").lower()  # 'local', 's3', 'both'
STATE_BACKEND = os.getenv("STATE_BACKEND", "sqlite").lower()     # 'sqlite', 'dynamodb', 'both'
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "5"))
LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "7"))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
LOG_FILE_MAX_BYTES = int(os.getenv("LOG_FILE_MAX_BYTES", str(10 * 1024 * 1024)))
LOG_BACKUP_COUNT = int(os.getenv("LOG_BACKUP_COUNT", "5"))
STATUS_JSON_PATH = DATA_DIR / "pipeline_status.json"
PORT = int(os.getenv("PORT", "8081"))
CRON_SCHEDULE = os.getenv("CRON_SCHEDULE", "0 3 * * *")
RUN_ON_STARTUP = os.getenv("RUN_ON_STARTUP", "true").lower() in ("true", "1", "yes")
STARTUP_MODE = os.getenv("STARTUP_MODE", "status")

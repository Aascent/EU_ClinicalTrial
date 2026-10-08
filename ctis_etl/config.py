"""Configuration module for CTIS ETL Pipeline.

Loads settings from environment variables and .env file.
"""

from __future__ import annotations

import os
from pathlib import Path

# Base Paths
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = BASE_DIR / "data"
QUARANTINE_DIR = BASE_DIR / "quarantine"
LOGS_DIR = BASE_DIR / "logs"
SQLITE_DB_PATH = BASE_DIR / "tracker.db"

# Ensure runtime directories exist
DATA_DIR.mkdir(parents=True, exist_ok=True)
QUARANTINE_DIR.mkdir(parents=True, exist_ok=True)
LOGS_DIR.mkdir(parents=True, exist_ok=True)

# Load .env file
def _load_env_file() -> None:
    env_file = BASE_DIR / ".env"
    if not env_file.exists():
        return
    try:
        from dotenv import load_dotenv
        load_dotenv(env_file)
    except ImportError:
        # Fallback manual parser if python-dotenv is not installed yet
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

# AWS Configuration
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID") or os.getenv("ACCESS_KEY", "")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY") or os.getenv("SECRET_ACCESS_KEY", "")
AWS_REGION = os.getenv("AWS_REGION") or os.getenv("AWS_DEFAULT_REGION", "us-east-1")

# Amazon S3 Configuration
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "aascent-mindgram")
S3_PREFIX = os.getenv("S3_PREFIX", "ctis/").strip("/")
if S3_PREFIX:
    S3_PREFIX += "/"

# Amazon DynamoDB Configuration
DYNAMODB_TABLE_NAME = os.getenv("DYNAMODB_TABLE_NAME") or os.getenv("TABLE_NAME", "EU_Clinical")
DYNAMODB_PARTITION_KEY = os.getenv("DYNAMODB_PARTITION_KEY") or os.getenv("PARTITION_KEYS", "euc")

# CTIS API Configuration
CTIS_API_BASE_URL = os.getenv("CTIS_API_BASE_URL", "https://euclinicaltrials.eu/ctis-public-api").rstrip("/")
SEARCH_URL = f"{CTIS_API_BASE_URL}/search"
RETRIEVE_URL = f"{CTIS_API_BASE_URL}/retrieve"

# Runtime Settings
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "5"))
LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "7"))
STORAGE_BACKEND = os.getenv("STORAGE_BACKEND", "s3").lower()  # 's3', 'local', or 'both'
STATE_BACKEND = os.getenv("STATE_BACKEND", "dynamodb").lower()  # 'dynamodb', 'sqlite', or 'both'
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

# Default HTTP Headers
DEFAULT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Content-Type": "application/json",
    "Accept": "application/json",
}

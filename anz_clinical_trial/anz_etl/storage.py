"""Storage module for ANZCTR ETL Pipeline (Bronze & Silver layers, S3 & Local)."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

from anz_etl import config

logger = logging.getLogger(__name__)

_s3_client = None


def get_s3_client():
    """Initializes and returns boto3 S3 client if available."""
    global _s3_client
    if _s3_client is not None:
        return _s3_client

    if not config.AWS_ACCESS_KEY_ID or not config.S3_BUCKET_NAME:
        return None

    try:
        import boto3
        from botocore.config import Config
        session = boto3.Session(
            aws_access_key_id=config.AWS_ACCESS_KEY_ID or None,
            aws_secret_access_key=config.AWS_SECRET_ACCESS_KEY or None,
            region_name=config.AWS_REGION or None,
        )
        _s3_client = session.client(
            "s3",
            config=Config(retries={"max_attempts": 4, "mode": "adaptive"})
        )
        return _s3_client
    except Exception as e:
        logger.error(f"Failed connecting to S3 bucket '{config.S3_BUCKET_NAME}': {e}")
        return None


def save_raw_endpoint_payload(api_name: str, identifier: str, raw_content: str) -> Path:
    """Saves raw untouched API XML response from any of the initial test endpoints."""
    # Clean filename
    safe_id = "".join(c if c.isalnum() or c in ("-", "_") else "_" for c in identifier) or "payload"
    filename = f"{safe_id}.xml"
    endpoint_dir = config.RAW_DIR / api_name.lower()
    endpoint_dir.mkdir(parents=True, exist_ok=True)
    local_path = endpoint_dir / filename

    try:
        with open(local_path, "w", encoding="utf-8") as f:
            f.write(raw_content)

        if config.STORAGE_BACKEND in ("s3", "both"):
            s3 = get_s3_client()
            if s3:
                s3_key = f"{config.S3_PREFIX}raw/{api_name.lower()}/{filename}"
                s3.put_object(
                    Bucket=config.S3_BUCKET_NAME,
                    Key=s3_key,
                    Body=raw_content.encode("utf-8"),
                    ContentType="application/xml",
                )
        return local_path
    except Exception as e:
        logger.error(f"Failed saving raw endpoint payload {api_name}/{filename}: {e}")
        raise


def save_bronze_record(actrn: str, raw_content: str, is_xml: bool = True) -> bool:
    """Saves raw API response in the Bronze layer."""
    ext = "xml" if is_xml else "json"
    filename = f"raw.{ext}"
    local_path = config.BRONZE_DIR / actrn / filename
    local_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        with open(local_path, "w", encoding="utf-8") as f:
            f.write(raw_content)

        if config.STORAGE_BACKEND in ("s3", "both"):
            s3 = get_s3_client()
            if s3:
                s3_key = f"{config.S3_BRONZE_PREFIX}{actrn}/{filename}"
                s3.put_object(
                    Bucket=config.S3_BUCKET_NAME,
                    Key=s3_key,
                    Body=raw_content.encode("utf-8"),
                    ContentType="application/xml" if is_xml else "application/json",
                )
        return True
    except Exception as e:
        logger.error(f"Failed saving Bronze record for {actrn}: {e}")
        return False


def save_silver_records(actrn: str, domain_payloads: Dict[str, Dict[str, Any]]) -> bool:
    """Saves parsed domain payloads in the Silver layer and data/{actrn} directory."""
    success = True
    for domain_name, data in domain_payloads.items():
        filename = f"{domain_name}.json"
        content_str = json.dumps(data, indent=2, ensure_ascii=False)

        # Save to silver/{actrn}
        silver_path = config.SILVER_DIR / actrn / filename
        silver_path.parent.mkdir(parents=True, exist_ok=True)
        # Also mirror to data/{actrn} for direct dashboard visibility
        root_data_path = config.DATA_DIR / actrn / filename
        root_data_path.parent.mkdir(parents=True, exist_ok=True)

        try:
            with open(silver_path, "w", encoding="utf-8") as f:
                f.write(content_str)
            with open(root_data_path, "w", encoding="utf-8") as f:
                f.write(content_str)

            if config.STORAGE_BACKEND in ("s3", "both"):
                s3 = get_s3_client()
                if s3:
                    s3_key = f"{config.S3_SILVER_PREFIX}{actrn}/{filename}"
                    s3.put_object(
                        Bucket=config.S3_BUCKET_NAME,
                        Key=s3_key,
                        Body=content_str.encode("utf-8"),
                        ContentType="application/json",
                    )
        except Exception as e:
            logger.error(f"Failed saving Silver record {domain_name} for {actrn}: {e}")
            success = False

    return success


def quarantine_record(actrn: str, raw_content: str, error_reason: str) -> Path:
    """Stores unparseable or error records in quarantine."""
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    path = config.QUARANTINE_DIR / f"{actrn}_{timestamp}.json"
    payload = {
        "actrn": actrn,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "error": error_reason,
        "content": raw_content,
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
    return path


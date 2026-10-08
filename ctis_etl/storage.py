"""Storage module handling writing trial JSON outputs to local disk and AWS S3."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

from ctis_etl import config

logger = logging.getLogger(__name__)

# Lazy S3 client
_s3_client = None


def get_s3_client():
    """Returns an authenticated boto3 S3 client if configured."""
    global _s3_client
    if _s3_client is not None:
        return _s3_client

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
            config=Config(retries={"max_attempts": 3, "mode": "standard"})
        )
        return _s3_client
    except ImportError:
        logger.warning("boto3 is not installed. S3 uploads are disabled until boto3 is installed.")
        return None
    except Exception as e:
        logger.error(f"Failed to initialize S3 client: {e}")
        return None


def _write_local_json(target_path: Path, payload: Any) -> bool:
    """Helper to atomically write a JSON file and verify non-empty size."""
    try:
        target_path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = target_path.with_suffix(".tmp")
        with open(temp_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, ensure_ascii=False)
        temp_path.replace(target_path)
        return target_path.stat().st_size > 0
    except Exception as e:
        logger.error(f"Failed writing local file {target_path}: {e}")
        return False


def _upload_s3_json(
    s3_client: Any,
    bucket: str,
    key: str,
    payload: Any,
    metadata: Dict[str, str],
) -> bool:
    """Helper to upload a JSON payload to S3 with encryption and metadata tags."""
    try:
        payload_bytes = json.dumps(payload, indent=2, ensure_ascii=False).encode("utf-8")
        put_kwargs = {
            "Bucket": bucket,
            "Key": key,
            "Body": payload_bytes,
            "ContentType": "application/json",
            "Metadata": metadata,
        }
        if config.S3_SERVER_SIDE_ENCRYPTION:
            put_kwargs["ServerSideEncryption"] = config.S3_SERVER_SIDE_ENCRYPTION
        s3_client.put_object(**put_kwargs)
        return True
    except Exception as e:
        logger.error(f"Failed uploading to s3://{bucket}/{key}: {e}")
        return False


def save_trial_files(
    ct_number: str,
    parsed_files: Dict[str, Any],
    raw_payload: Optional[Dict[str, Any]] = None,
    backend: Optional[str] = None,
) -> Dict[str, bool]:
    """Persists trial entities across Bronze (raw) and Silver (6 domain files) tiers.

    Args:
        ct_number: Trial identifier (e.g. '2026-527084-15-00')
        parsed_files: Silver tier dictionary of filename -> domain JSON payload (6 files)
        raw_payload: Bronze tier unmodified API response payload (raw.json)
        backend: Storage backend ('local', 's3', or 'both'). Defaults to config.STORAGE_BACKEND.

    Returns:
        Dict indicating success status for each persisted entity.
    """
    backend = (backend or config.STORAGE_BACKEND).lower()
    results: Dict[str, bool] = {}

    save_local = backend in ("local", "both")
    save_s3 = backend in ("s3", "both")

    # Local Directory Persistence
    if save_local:
        # 1. Bronze Tier (Raw untouched response)
        if raw_payload is not None:
            bronze_path = config.BRONZE_DIR / ct_number / "raw.json"
            results["local:bronze:raw.json"] = _write_local_json(bronze_path, raw_payload)

        # 2. Silver Tier (6 domain JSON files)
        for filename, payload in parsed_files.items():
            # Standard silver directory
            silver_path = config.SILVER_DIR / ct_number / filename
            results[f"local:silver:{filename}"] = _write_local_json(silver_path, payload)
            # Direct directory for convenience / backward compatibility
            direct_path = config.DATA_DIR / ct_number / filename
            _write_local_json(direct_path, payload)

    # AWS S3 Persistence
    if save_s3:
        s3 = get_s3_client()
        if s3 is None:
            logger.warning(f"S3 client unavailable. Falling back to local disk for {ct_number}.")
            if not save_local:
                return save_trial_files(
                    ct_number=ct_number,
                    parsed_files=parsed_files,
                    raw_payload=raw_payload,
                    backend="local",
                )
        else:
            now_iso = datetime.now(timezone.utc).isoformat()

            # 1. Bronze Tier Upload
            if raw_payload is not None:
                bronze_key = f"{config.S3_BRONZE_PREFIX}{ct_number}/raw.json"
                results["s3:bronze:raw.json"] = _upload_s3_json(
                    s3,
                    config.S3_BUCKET_NAME,
                    bronze_key,
                    raw_payload,
                    {"ct-number": ct_number, "filename": "raw.json", "ingested-at": now_iso},
                )

            # 2. Silver Tier Uploads
            for filename, payload in parsed_files.items():
                silver_key = f"{config.S3_SILVER_PREFIX}{ct_number}/{filename}"
                results[f"s3:silver:{filename}"] = _upload_s3_json(
                    s3,
                    config.S3_BUCKET_NAME,
                    silver_key,
                    payload,
                    {"ct-number": ct_number, "filename": filename, "ingested-at": now_iso},
                )
                # Direct root prefix upload
                legacy_key = f"{config.S3_PREFIX}{ct_number}/{filename}"
                _upload_s3_json(
                    s3,
                    config.S3_BUCKET_NAME,
                    legacy_key,
                    payload,
                    {"ct-number": ct_number, "filename": filename, "ingested-at": now_iso},
                )

    return results


def quarantine_payload(ct_number: str, raw_payload: Any, error_msg: str) -> Path:
    """Saves malformed or failed payload to quarantine directory for post-mortem analysis."""
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    safe_ct = ct_number.replace("/", "_")
    base_name = f"{safe_ct}_{timestamp}"

    json_path = config.QUARANTINE_DIR / f"{base_name}.json"
    err_path = config.QUARANTINE_DIR / f"{base_name}.error.log"

    try:
        with open(json_path, "w", encoding="utf-8") as f:
            if isinstance(raw_payload, (dict, list)):
                json.dump(raw_payload, f, indent=2, ensure_ascii=False)
            else:
                f.write(str(raw_payload))

        with open(err_path, "w", encoding="utf-8") as f:
            f.write(f"Timestamp: {timestamp}\n")
            f.write(f"Trial ID: {ct_number}\n")
            f.write(f"Error:\n{error_msg}\n")

        logger.warning(f"Quarantined trial payload for {ct_number} at {json_path}")
    except Exception as e:
        logger.error(f"Failed writing to quarantine for {ct_number}: {e}")

    return json_path

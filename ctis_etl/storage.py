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


def save_trial_files(
    ct_number: str,
    parsed_files: Dict[str, Any],
    backend: Optional[str] = None,
) -> Dict[str, bool]:
    """Saves the 6 JSON files according to the chosen storage backend.

    Args:
        ct_number: Trial identifier (e.g. '2026-527084-15-00')
        parsed_files: Dictionary of filename -> json-serializable payload
        backend: Storage backend ('local', 's3', or 'both'). Defaults to config.STORAGE_BACKEND.

    Returns:
        Dict indicating success status for each file.
    """
    backend = (backend or config.STORAGE_BACKEND).lower()
    results = {}

    save_local = backend in ("local", "both")
    save_s3 = backend in ("s3", "both")

    # Local Directory Save
    if save_local:
        trial_dir = config.DATA_DIR / ct_number
        trial_dir.mkdir(parents=True, exist_ok=True)
        for filename, payload in parsed_files.items():
            file_path = trial_dir / filename
            try:
                temp_path = file_path.with_suffix(".tmp")
                with open(temp_path, "w", encoding="utf-8") as f:
                    json.dump(payload, f, indent=2, ensure_ascii=False)
                temp_path.replace(file_path)
                results[f"local:{filename}"] = file_path.stat().st_size > 0
            except Exception as e:
                logger.error(f"Failed writing local file {file_path}: {e}")
                results[f"local:{filename}"] = False

    # AWS S3 Save
    if save_s3:
        s3 = get_s3_client()
        if s3 is None:
            logger.warning(f"S3 client unavailable. Falling back to local disk for {ct_number}.")
            if not save_local:
                return save_trial_files(ct_number, parsed_files, backend="local")
        else:
            now_iso = datetime.now(timezone.utc).isoformat()
            for filename, payload in parsed_files.items():
                s3_key = f"{config.S3_PREFIX}{ct_number}/{filename}"
                try:
                    payload_bytes = json.dumps(payload, indent=2, ensure_ascii=False).encode("utf-8")
                    put_kwargs = {
                        "Bucket": config.S3_BUCKET_NAME,
                        "Key": s3_key,
                        "Body": payload_bytes,
                        "ContentType": "application/json",
                        "Metadata": {
                            "ct-number": ct_number,
                            "ingested-at": now_iso,
                            "filename": filename,
                        },
                    }
                    if config.S3_SERVER_SIDE_ENCRYPTION:
                        put_kwargs["ServerSideEncryption"] = config.S3_SERVER_SIDE_ENCRYPTION

                    s3.put_object(**put_kwargs)
                    results[f"s3:{filename}"] = True
                except Exception as e:
                    logger.error(f"Failed uploading to s3://{config.S3_BUCKET_NAME}/{s3_key}: {e}")
                    results[f"s3:{filename}"] = False

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

"""Migration script to backfill and update all existing trial_documents.json records

(both locally and in AWS S3) with the permanent 'url' field and remove redundant 'downloadUrl'.
"""

import concurrent.futures
import json
import logging
import re
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from ctis_etl import config, storage

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger("url_migration")


def update_document_list(ct_number: str, docs: Any) -> Tuple[Any, bool]:
    """Updates document objects so that every entry with a uuid has a permanent 'url' and no 'downloadUrl'.

    Returns (updated_docs, was_modified).
    """
    if not isinstance(docs, list):
        return docs, False

    modified = False
    updated_docs = []

    for doc in docs:
        if not isinstance(doc, dict):
            updated_docs.append(doc)
            continue

        doc_copy = dict(doc)
        uuid = str(doc_copy.get("uuid") or "").strip()

        expected_url = f"https://euclinicaltrials.eu/ctis-public-api/documents/{ct_number}/{uuid}/download" if uuid else None

        current_url = doc_copy.get("url")
        # Check if url is missing, or is an old ephemeral S3 presigned URL
        if expected_url and (not current_url or "amazonaws.com" in str(current_url) or current_url != expected_url):
            doc_copy["url"] = expected_url
            modified = True

        # Ensure redundant downloadUrl is stripped
        if "downloadUrl" in doc_copy:
            doc_copy.pop("downloadUrl", None)
            modified = True

        updated_docs.append(doc_copy)

    return updated_docs, modified


def migrate_local_files() -> int:
    """Scans and updates local trial_documents.json files."""
    local_dir = config.DATA_DIR
    logger.info(f"Scanning local directory: {local_dir.resolve()}...")
    doc_paths = list(local_dir.glob("**/trial_documents.json"))
    logger.info(f"Found {len(doc_paths)} local trial_documents.json files.")

    updated_count = 0
    for path in doc_paths:
        ct_number = path.parent.name
        try:
            with open(path, "r", encoding="utf-8") as f:
                content = json.load(f)

            new_content, changed = update_document_list(ct_number, content)
            if changed:
                with open(path, "w", encoding="utf-8") as f:
                    json.dump(new_content, f, indent=2, ensure_ascii=False)
                updated_count += 1
        except Exception as e:
            logger.error(f"Failed updating local file {path}: {e}")

    logger.info(f"Finished local update: {updated_count}/{len(doc_paths)} files updated.")
    return updated_count


def process_single_s3_key(s3_client: Any, bucket: str, key: str) -> Tuple[str, bool, Optional[str]]:
    """Fetches a single trial_documents.json from S3, enriches url field, and uploads back if changed."""
    # S3 Key pattern: ctis/silver/{ct_number}/trial_documents.json
    parts = key.split("/")
    # Extract ct_number (e.g. 2nd to last part)
    ct_number = parts[-2] if len(parts) >= 2 else ""

    try:
        resp = s3_client.get_object(Bucket=bucket, Key=key)
        raw_body = resp["Body"].read().decode("utf-8")
        docs = json.loads(raw_body)

        updated_docs, changed = update_document_list(ct_number, docs)
        if changed:
            new_json_bytes = json.dumps(updated_docs, indent=2, ensure_ascii=False).encode("utf-8")
            s3_client.put_object(
                Bucket=bucket,
                Key=key,
                Body=new_json_bytes,
                ContentType="application/json",
            )
            return key, True, None
        return key, False, None
    except Exception as e:
        return key, False, str(e)


def migrate_s3_files(max_workers: int = 35) -> Tuple[int, int, int]:
    """Lists all trial_documents.json files in S3 and updates them concurrently with worker threads."""
    s3 = storage.get_s3_client()
    if s3 is None:
        logger.error("Could not obtain S3 client. Please verify AWS credentials.")
        return 0, 0, 0

    bucket = config.S3_BUCKET_NAME
    prefix = f"{config.S3_SILVER_PREFIX}"
    logger.info(f"Listing S3 keys from s3://{bucket}/{prefix}...")

    paginator = s3.get_paginator("list_objects_v2")
    s3_keys: List[str] = []

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix, PaginationConfig={"PageSize": 1000}):
        for obj in page.get("Contents", []):
            k = obj["Key"]
            if k.endswith("/trial_documents.json"):
                s3_keys.append(k)

    total_keys = len(s3_keys)
    logger.info(f"Found {total_keys} trial_documents.json files in S3. Starting concurrent update with {max_workers} workers...")

    if not s3_keys:
        return 0, 0, 0

    updated_count = 0
    skipped_count = 0
    error_count = 0
    processed_count = 0

    start_time = time.time()

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_key = {
            executor.submit(process_single_s3_key, s3, bucket, key): key
            for key in s3_keys
        }

        for future in concurrent.futures.as_completed(future_to_key):
            processed_count += 1
            key, changed, err = future.result()

            if err:
                error_count += 1
                logger.warning(f"Error on {key}: {err}")
            elif changed:
                updated_count += 1
            else:
                skipped_count += 1

            if processed_count % 500 == 0 or processed_count == total_keys:
                elapsed = time.time() - start_time
                rate = processed_count / elapsed if elapsed > 0 else 0
                logger.info(
                    f"Progress: {processed_count}/{total_keys} ({processed_count*100/total_keys:.1f}%) | "
                    f"Updated: {updated_count} | Skipped: {skipped_count} | Errors: {error_count} | "
                    f"Speed: {rate:.1f} keys/sec"
                )

    total_time = time.time() - start_time
    logger.info(
        f"Completed S3 migration in {total_time:.1f}s: "
        f"{updated_count} updated, {skipped_count} skipped, {error_count} errors out of {total_keys} total."
    )
    return updated_count, skipped_count, error_count


if __name__ == "__main__":
    logger.info("=== Starting Trial Documents URL Migration ===")
    local_updated = migrate_local_files()
    s3_updated, s3_skipped, s3_errors = migrate_s3_files(max_workers=35)
    logger.info("=== Migration Complete ===")
    logger.info(f"Summary: Local updated: {local_updated} | S3 updated: {s3_updated} | S3 already valid: {s3_skipped} | Errors: {s3_errors}")

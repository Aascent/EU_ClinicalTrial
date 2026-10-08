"""Main entry point and orchestrator for EU CTIS Data Pipeline."""

from __future__ import annotations

import argparse
import concurrent.futures
from datetime import datetime, timedelta, timezone
import logging
import sys
import uuid
from typing import List, Optional

from ctis_etl import config, database, parser, storage
from ctis_etl.api_client import CTISClient, CTISNotFoundError

from logging.handlers import RotatingFileHandler
import re
import signal
import threading

# Global shutdown event for graceful container termination
_shutdown_event = threading.Event()


def _handle_shutdown_signal(signum: int, frame: Any) -> None:
    sig_name = "SIGINT" if signum == signal.SIGINT else "SIGTERM"
    logger.warning(f"Shutdown signal {sig_name} received. Finishing active tasks and stopping gracefully...")
    _shutdown_event.set()


try:
    signal.signal(signal.SIGINT, _handle_shutdown_signal)
    signal.signal(signal.SIGTERM, _handle_shutdown_signal)
except (ValueError, AttributeError):
    pass

# Configure rotating file logging for production disk safety
rotating_handler = RotatingFileHandler(
    config.LOGS_DIR / "ctis_etl.log",
    maxBytes=config.LOG_FILE_MAX_BYTES,
    backupCount=config.LOG_BACKUP_COUNT,
    encoding="utf-8",
)

logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        rotating_handler,
    ],
)
logger = logging.getLogger("ctis_etl")


def parse_ctis_date(date_str: Optional[str]) -> Optional[datetime]:
    """Parses date string from Search API (e.g. '07/10/2026', 'HU: 07/10/2026', or ISO format)."""
    if not date_str or not isinstance(date_str, str):
        return None
    date_str = date_str.strip()
    # Strip country code prefixes like 'HU: ', 'FR: ', 'DE: '
    date_str = re.sub(r"^[A-Za-z]{2,3}:\s*", "", date_str)

    # Try DD/MM/YYYY and ISO formats
    for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%S.%f"):
        try:
            return datetime.strptime(date_str.split(".")[0] if "." in date_str and fmt == "%Y-%m-%dT%H:%M:%S" else date_str, fmt)
        except ValueError:
            continue
    return None


def process_single_trial(
    client: CTISClient,
    ct_number: str,
    storage_backend: Optional[str] = None,
) -> bool:
    """Retrieves, validates, parses, stores a single trial and updates state DB."""
    if _shutdown_event.is_set():
        logger.info(f"Skipping {ct_number} due to active shutdown signal.")
        return False

    database.mark_processing(ct_number)
    logger.info(f"Processing trial: {ct_number}")
    raw_payload = None
    try:
        raw_payload = client.retrieve_trial(ct_number)

        # Validate core invariants
        if not raw_payload.get("ctNumber"):
            raise ValueError(f"Payload missing valid ctNumber: {ct_number}")

        # Parse into 6 discrete files (Silver Layer)
        silver_files = parser.parse_trial_dossier(raw_payload)

        # Generate business-ready analytical summary (Gold Layer)
        gold_record = parser.generate_gold_analytics(raw_payload)

        # Persist across Medallion storage tiers (Bronze, Silver, Gold)
        save_results = storage.save_trial_files(
            ct_number=ct_number,
            parsed_files=silver_files,
            raw_payload=raw_payload,
            gold_record=gold_record,
            backend=storage_backend,
        )

        failed_files = [f for f, ok in save_results.items() if not ok]
        if failed_files or not save_results:
            raise IOError(f"Failed to persist files for trial {ct_number}: {failed_files}")

        # Mark success in database
        publish_date = raw_payload.get("publishDate") or raw_payload.get("decisionDate")
        database.mark_success(ct_number, publish_date)
        logger.info(f"Successfully processed trial: {ct_number}")
        return True

    except Exception as e:
        error_msg = str(e)
        logger.error(f"Error processing trial {ct_number}: {error_msg}")
        storage.quarantine_payload(ct_number, raw_payload or {"error": error_msg}, error_msg)
        database.mark_failure(ct_number, error_msg)
        return False


def run_incremental(
    client: CTISClient,
    lookback_days: int = 7,
    max_workers: int = 5,
    storage_backend: Optional[str] = None,
) -> None:
    """Discovers recently updated trials within lookback window and synchronizes them."""
    run_id = str(uuid.uuid4())
    started_at = datetime.now(timezone.utc).isoformat()
    database.record_pipeline_run(run_id, "INCREMENTAL", started_at, status="RUNNING")

def discover_and_stage_recent(
    client: CTISClient,
    lookback_days: int = 7,
) -> int:
    """Discovers recent trials within lookback window and stages them in database.

    Returns total count of discovered trials.
    """
    cutoff_date = datetime.now() - timedelta(days=lookback_days)
    logger.info(f"Scanning Search API for trials within last {lookback_days} days (cutoff: {cutoff_date.strftime('%Y-%m-%d')})...")

    discovered = 0
    current_page = 1
    hit_cutoff = False

    while not hit_cutoff:
        search_resp = client.search_page(page=current_page, size=100, sort_property="decisionDate", sort_direction="DESC")
        items = search_resp.get("data", [])
        if not items:
            break

        for item in items:
            discovered += 1
            ct_number = item.get("ctNumber")
            if not ct_number:
                continue

            raw_date = item.get("lastPublicationUpdate") or item.get("decisionDateOverall") or item.get("lastUpdated")
            dt = parse_ctis_date(raw_date)

            if dt and dt < cutoff_date:
                hit_cutoff = True
                break

            database.stage_trial(ct_number, raw_date)

        pagination = search_resp.get("pagination", {})
        if current_page >= pagination.get("totalPages", current_page):
            break
        current_page += 1

    return discovered


def _process_trial_queue(
    client: CTISClient,
    queue: List[str],
    max_workers: int = 5,
    storage_backend: Optional[str] = None,
) -> tuple[int, int]:
    """Helper to process a list of trials with ThreadPoolExecutor. Returns (succeeded, failed)."""
    succeeded = 0
    failed = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_map = {
            executor.submit(process_single_trial, client, ct_num, storage_backend): ct_num
            for ct_num in queue
        }
        for future in concurrent.futures.as_completed(future_map):
            ct_num = future_map[future]
            try:
                if future.result():
                    succeeded += 1
                else:
                    failed += 1
            except Exception as e:
                failed += 1
                logger.error(f"Task failure for {ct_num}: {e}")
    return succeeded, failed


def run_new_trials(
    client: CTISClient,
    lookback_days: int = 7,
    max_workers: int = 5,
    storage_backend: Optional[str] = None,
) -> None:
    """Process 2: Checks CTIS for brand new trials published recently and ingests them."""
    run_id = str(uuid.uuid4())
    started_at = datetime.now(timezone.utc).isoformat()
    database.record_pipeline_run(run_id, "NEW_TRIALS", started_at, status="RUNNING")

    logger.info(">>> [PROCESS 2] Checking for brand NEW clinical trials...")
    discovered = discover_and_stage_recent(client, lookback_days)
    new_queue = database.get_pending_trials(filter_type="new")
    logger.info(f"Discovered {len(new_queue)} brand NEW trials ready for ingestion.")

    succeeded, failed = _process_trial_queue(client, new_queue, max_workers, storage_backend)
    completed_at = datetime.now(timezone.utc).isoformat()
    database.record_pipeline_run(
        run_id=run_id,
        run_type="NEW_TRIALS",
        started_at=started_at,
        completed_at=completed_at,
        status="COMPLETED",
        discovered=discovered,
        processed=len(new_queue),
        succeeded=succeeded,
        failed=failed,
    )
    logger.info(f"[PROCESS 2 COMPLETED] New trials processed: {len(new_queue)}, Succeeded: {succeeded}, Failed: {failed}")


def run_updated_trials(
    client: CTISClient,
    lookback_days: int = 7,
    max_workers: int = 5,
    storage_backend: Optional[str] = None,
) -> None:
    """Process 3: Checks CTIS for updates or amendments to existing trials and updates them."""
    run_id = str(uuid.uuid4())
    started_at = datetime.now(timezone.utc).isoformat()
    database.record_pipeline_run(run_id, "UPDATED_TRIALS", started_at, status="RUNNING")

    logger.info(">>> [PROCESS 3] Checking for UPDATES / AMENDMENTS to existing trials...")
    discovered = discover_and_stage_recent(client, lookback_days)
    updates_queue = database.get_pending_trials(filter_type="updates")
    logger.info(f"Discovered {len(updates_queue)} UPDATED trials ready for re-ingestion.")

    succeeded, failed = _process_trial_queue(client, updates_queue, max_workers, storage_backend)
    completed_at = datetime.now(timezone.utc).isoformat()
    database.record_pipeline_run(
        run_id=run_id,
        run_type="UPDATED_TRIALS",
        started_at=started_at,
        completed_at=completed_at,
        status="COMPLETED",
        discovered=discovered,
        processed=len(updates_queue),
        succeeded=succeeded,
        failed=failed,
    )
    logger.info(f"[PROCESS 3 COMPLETED] Updated trials processed: {len(updates_queue)}, Succeeded: {succeeded}, Failed: {failed}")


def run_incremental(
    client: CTISClient,
    lookback_days: int = 7,
    max_workers: int = 5,
    storage_backend: Optional[str] = None,
) -> None:
    """Discovers both new and updated trials within lookback window and synchronizes them."""
    run_id = str(uuid.uuid4())
    started_at = datetime.now(timezone.utc).isoformat()
    database.record_pipeline_run(run_id, "INCREMENTAL", started_at, status="RUNNING")

    logger.info(f"Starting combined incremental sync (new + updates) with {lookback_days}-day lookback...")
    discovered = discover_and_stage_recent(client, lookback_days)
    pending_queue = database.get_pending_trials()
    logger.info(f"Queue size for ingestion (new + updates): {len(pending_queue)} trials")

    succeeded, failed = _process_trial_queue(client, pending_queue, max_workers, storage_backend)
    completed_at = datetime.now(timezone.utc).isoformat()
    database.record_pipeline_run(
        run_id=run_id,
        run_type="INCREMENTAL",
        started_at=started_at,
        completed_at=completed_at,
        status="COMPLETED",
        discovered=discovered,
        processed=len(pending_queue),
        succeeded=succeeded,
        failed=failed,
    )
    logger.info(f"Incremental sync finished. Processed: {len(pending_queue)}, Succeeded: {succeeded}, Failed: {failed}")


def run_full(
    client: CTISClient,
    max_workers: int = 5,
    storage_backend: Optional[str] = None,
) -> None:
    """Discovers all trials and processes the full catalog."""
    run_id = str(uuid.uuid4())
    started_at = datetime.now(timezone.utc).isoformat()
    database.record_pipeline_run(run_id, "FULL", started_at, status="RUNNING")

    logger.info("Starting FULL historical catalog ingestion (processing page-by-page)...")
    total_discovered = 0
    total_processed = 0
    total_succeeded = 0
    total_failed = 0
    total_skipped = 0

    current_page = 1
    page_size = 100

    while True:
        logger.info(f"[Catalog Discovery] Fetching Search API page {current_page} (size={page_size})...")
        try:
            resp = client.search_page(page=current_page, size=page_size, sort_property="decisionDate", sort_direction="DESC")
        except Exception as e:
            logger.error(f"Error fetching search page {current_page}: {e}. Retrying page after 5s...")
            import time
            time.sleep(5)
            continue

        items = resp.get("data", [])
        pagination = resp.get("pagination", {})
        total_pages = pagination.get("totalPages", current_page)
        total_records = pagination.get("totalRecords", total_discovered)

        if not items:
            break

        # Stage items for this page
        page_pending = []
        for item in items:
            total_discovered += 1
            ct_number = item.get("ctNumber")
            if not ct_number:
                continue
            raw_date = item.get("lastPublicationUpdate") or item.get("decisionDateOverall") or item.get("lastUpdated")
            state = database.stage_trial(ct_number, raw_date)
            if state in ("PENDING", "UPDATE_PENDING"):
                page_pending.append(ct_number)
            else:
                total_skipped += 1

        # Process any pending trials for this batch
        if page_pending:
            logger.info(
                f"[Page {current_page}/{total_pages}] Ingesting {len(page_pending)} trials "
                f"({len(items) - len(page_pending)} already up to date)..."
            )
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_map = {
                    executor.submit(process_single_trial, client, ct_num, storage_backend): ct_num
                    for ct_num in page_pending
                }
                for future in concurrent.futures.as_completed(future_map):
                    ct_num = future_map[future]
                    total_processed += 1
                    try:
                        if future.result():
                            total_succeeded += 1
                        else:
                            total_failed += 1
                    except Exception as e:
                        total_failed += 1
                        logger.error(f"Failure processing {ct_num}: {e}")
        else:
            logger.info(f"[Page {current_page}/{total_pages}] All {len(items)} trials already up to date. Skipping batch.")

        logger.info(
            f"Progress: Page {current_page}/{total_pages} | "
            f"Discovered: {total_discovered}/{total_records} | Succeeded: {total_succeeded} | "
            f"Skipped: {total_skipped} | Failed: {total_failed}"
        )

        if current_page >= total_pages:
            break
        current_page += 1

    completed_at = datetime.now(timezone.utc).isoformat()
    database.record_pipeline_run(
        run_id=run_id,
        run_type="FULL",
        started_at=started_at,
        completed_at=completed_at,
        status="COMPLETED",
        discovered=total_discovered,
        processed=total_processed,
        succeeded=total_succeeded,
        failed=total_failed,
    )
    logger.info(
        f"FULL catalog ingestion finished! Total Discovered: {total_discovered}, "
        f"Processed: {total_processed}, Succeeded: {total_succeeded}, "
        f"Skipped: {total_skipped}, Failed: {total_failed}"
    )


def run_check(client: CTISClient) -> None:
    """Pre-flight diagnostic check verifying CTIS API, local disk, SQLite, S3, and DynamoDB."""
    print("=================================================================")
    print("       EU CTIS Data Pipeline - System Diagnostic Check          ")
    print("=================================================================")
    all_ok = True

    # 1. CTIS API Connectivity
    print("\n[1/5] Checking CTIS Public API connectivity...")
    try:
        resp = client.search_page(page=1, size=2)
        total = resp.get("pagination", {}).get("totalRecords", 0)
        items = resp.get("data", [])
        if total > 0 and len(items) > 0:
            print(f"  [PASS] CTIS Search API reachable. Total records in database: {total}")
            sample_ct = items[0].get("ctNumber")
            print(f"  [PASS] Sample trial retrieve testing on '{sample_ct}'...")
            trial_data = client.retrieve_trial(sample_ct)
            if trial_data.get("ctNumber") == sample_ct:
                print(f"  [PASS] CTIS Retrieve API verified successfully.")
        else:
            print(f"  [FAIL] CTIS Search API returned 0 records.")
            all_ok = False
    except Exception as e:
        print(f"  [FAIL] CTIS API communication error: {e}")
        all_ok = False

    # 2. Local Filesystem Write Access
    print("\n[2/5] Checking Local Storage & Directory Permissions...")
    try:
        test_file = config.DATA_DIR / ".write_test"
        test_file.write_text("ok", encoding="utf-8")
        test_file.unlink()
        print(f"  [PASS] Local data directory ({config.DATA_DIR}) writable.")
        print(f"  [PASS] Quarantine directory ({config.QUARANTINE_DIR}) writable.")
        print(f"  [PASS] Logs directory ({config.LOGS_DIR}) writable.")
    except Exception as e:
        print(f"  [FAIL] Local filesystem write error: {e}")
        all_ok = False

    # 3. SQLite Database State Check
    print("\n[3/5] Checking SQLite Database ('tracker.db')...")
    try:
        database.init_sqlite_db()
        with database.sqlite3.connect(config.SQLITE_DB_PATH) as conn:
            cnt = conn.execute("SELECT count(*) FROM trials").fetchone()[0]
            print(f"  [PASS] SQLite database accessible. Current tracked trials: {cnt}")
    except Exception as e:
        print(f"  [FAIL] SQLite database error: {e}")
        all_ok = False

    # 4. Amazon S3 Check
    print(f"\n[4/5] Checking Amazon S3 Bucket ('{config.S3_BUCKET_NAME}')...")
    s3 = storage.get_s3_client()
    if s3 is None:
        print("  [WARN] boto3 is not installed or S3 client could not initialize. S3 uploads disabled.")
    else:
        try:
            s3.head_bucket(Bucket=config.S3_BUCKET_NAME)
            print(f"  [PASS] Successfully connected to S3 bucket '{config.S3_BUCKET_NAME}' in region '{config.AWS_REGION}'.")
        except Exception as e:
            print(f"  [FAIL] S3 Bucket access error: {e}")
            all_ok = False

    # 5. Amazon DynamoDB Check
    print(f"\n[5/5] Checking Amazon DynamoDB Table ('{config.DYNAMODB_TABLE_NAME}')...")
    table = database.get_dynamodb_table()
    if table is None:
        print("  [WARN] boto3 is not installed or DynamoDB client could not initialize. DynamoDB disabled.")
    else:
        try:
            table.load()
            print(f"  [PASS] Successfully connected to DynamoDB table '{config.DYNAMODB_TABLE_NAME}' (status: {table.table_status}).")
        except Exception as e:
            print(f"  [FAIL] DynamoDB Table access error: {e}")
            all_ok = False

    print("\n=================================================================")
    if all_ok:
        print("  DIAGNOSTIC STATUS: ALL CRITICAL SYSTEMS OPERATIONAL [OK]")
    else:
        print("  DIAGNOSTIC STATUS: ISSUES DETECTED - PLEASE REVIEW LOGS ABOVE")
    print("=================================================================\n")


def main() -> None:
    """Command-line argument parser and dispatcher."""
    parser_cli = argparse.ArgumentParser(description="EU CTIS Data Pipeline ETL CLI")
    parser_cli.add_argument(
        "--mode",
        choices=["incremental", "historical", "full", "new", "updates", "single", "retry-failed", "check"],
        default="incremental",
        help="Pipeline execution mode: 'historical' (all data), 'new' (only new trials), 'updates' (only updated trials), 'incremental' (new + updates), 'single' (one trial), 'retry-failed', 'check' (pre-flight diagnostics)",
    )
    parser_cli.add_argument(
        "--lookback-days",
        type=int,
        default=config.LOOKBACK_DAYS,
        help="Lookback window in days for new/updates/incremental sync (default: 7)",
    )
    parser_cli.add_argument(
        "--ct-number",
        type=str,
        help="Trial identifier required for '--mode single' (e.g. 2026-527084-15-00)",
    )
    parser_cli.add_argument(
        "--workers",
        type=int,
        default=config.MAX_WORKERS,
        help="Concurrent worker threads (default: 5)",
    )
    parser_cli.add_argument(
        "--storage",
        choices=["local", "s3", "both"],
        default=config.STORAGE_BACKEND,
        help="Storage target backend (default: from .env)",
    )

    args = parser_cli.parse_args()
    client = CTISClient(base_url=config.CTIS_API_BASE_URL)
    database.init_sqlite_db()
    stale_count = database.reset_stale_processing(timeout_minutes=15)
    if stale_count > 0:
        logger.info(f"Automatically recovered {stale_count} stale trials left from interrupted runs.")

    if args.mode == "check":
        run_check(client)
        sys.exit(0)

    if args.mode == "single":
        if not args.ct_number:
            logger.error("--ct-number is required when using --mode single")
            sys.exit(1)
        ok = process_single_trial(client, args.ct_number, storage_backend=args.storage)
        sys.exit(0 if ok else 1)

    elif args.mode in ("historical", "full"):
        run_full(
            client=client,
            max_workers=args.workers,
            storage_backend=args.storage,
        )

    elif args.mode == "new":
        run_new_trials(
            client=client,
            lookback_days=args.lookback_days,
            max_workers=args.workers,
            storage_backend=args.storage,
        )

    elif args.mode == "updates":
        run_updated_trials(
            client=client,
            lookback_days=args.lookback_days,
            max_workers=args.workers,
            storage_backend=args.storage,
        )

    elif args.mode == "incremental":
        run_incremental(
            client=client,
            lookback_days=args.lookback_days,
            max_workers=args.workers,
            storage_backend=args.storage,
        )

    elif args.mode == "retry-failed":
        # Pull trials currently marked FAILED and re-queue as PENDING
        with database.sqlite3.connect(config.SQLITE_DB_PATH) as conn:
            conn.execute("UPDATE trials SET status = 'PENDING', retry_count = 0 WHERE status = 'FAILED'")
            conn.commit()
        pending = database.get_pending_trials()
        logger.info(f"Re-queued {len(pending)} previously failed trials. Processing...")
        with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as executor:
            futures = [executor.submit(process_single_trial, client, ct, args.storage) for ct in pending]
            concurrent.futures.wait(futures)


if __name__ == "__main__":
    main()

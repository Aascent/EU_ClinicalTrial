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

# Configure logging
logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(config.LOGS_DIR / "ctis_etl.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger("ctis_etl")


def parse_ctis_date(date_str: Optional[str]) -> Optional[datetime]:
    """Parses date string from Search API (typically DD/MM/YYYY) or ISO string."""
    if not date_str or not isinstance(date_str, str):
        return None
    date_str = date_str.strip()

    # Try DD/MM/YYYY format
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
    logger.info(f"Processing trial: {ct_number}")
    raw_payload = None
    try:
        raw_payload = client.retrieve_trial(ct_number)

        # Validate core invariants
        if not raw_payload.get("ctNumber"):
            raise ValueError(f"Payload missing valid ctNumber: {ct_number}")

        # Parse into 6 discrete files
        parsed_files = parser.parse_trial_dossier(raw_payload)

        # Save to S3 and/or local storage
        save_results = storage.save_trial_files(
            ct_number=ct_number,
            parsed_files=parsed_files,
            backend=storage_backend,
        )

        if not any(save_results.values()):
            raise IOError(f"Failed to persist files for trial {ct_number} (results: {save_results})")

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

    cutoff_date = datetime.now() - timedelta(days=lookback_days)
    logger.info(f"Starting incremental sync with {lookback_days}-day lookback (cutoff: {cutoff_date.strftime('%Y-%m-%d')})")

    discovered = 0
    current_page = 1
    hit_cutoff = False

    while not hit_cutoff:
        logger.info(f"Querying Search API page {current_page}...")
        search_resp = client.search_page(page=current_page, size=100, sort_property="decisionDate", sort_direction="DESC")
        items = search_resp.get("data", [])
        if not items:
            break

        for item in items:
            discovered += 1
            ct_number = item.get("ctNumber")
            if not ct_number:
                continue

            # Check dates against lookback cutoff
            raw_date = item.get("lastPublicationUpdate") or item.get("decisionDateOverall") or item.get("lastUpdated")
            dt = parse_ctis_date(raw_date)

            if dt and dt < cutoff_date:
                logger.info(f"Reached record {ct_number} with date {raw_date} prior to cutoff {cutoff_date}. Halting pagination.")
                hit_cutoff = True
                break

            database.stage_trial(ct_number, raw_date)

        pagination = search_resp.get("pagination", {})
        if current_page >= pagination.get("totalPages", current_page):
            break
        current_page += 1

    # Process all staged trials
    pending_queue = database.get_pending_trials()
    logger.info(f"Queue size for ingestion: {len(pending_queue)} trials")

    succeeded = 0
    failed = 0

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_map = {
            executor.submit(process_single_trial, client, ct_num, storage_backend): ct_num
            for ct_num in pending_queue
        }
        for future in concurrent.futures.as_completed(future_map):
            ct_num = future_map[future]
            try:
                ok = future.result()
                if ok:
                    succeeded += 1
                else:
                    failed += 1
            except Exception as e:
                failed += 1
                logger.error(f"Task failure for {ct_num}: {e}")

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

    logger.info("Starting FULL historical catalog discovery...")
    discovered = 0

    for item in client.iterate_search_trials(page_size=100, sort_property="decisionDate"):
        discovered += 1
        ct_number = item.get("ctNumber")
        if ct_number:
            raw_date = item.get("lastPublicationUpdate") or item.get("decisionDateOverall")
            database.stage_trial(ct_number, raw_date)

    pending_queue = database.get_pending_trials()
    logger.info(f"Full discovery found {discovered} total records. Ingestion queue: {len(pending_queue)} trials")

    succeeded = 0
    failed = 0

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_map = {
            executor.submit(process_single_trial, client, ct_num, storage_backend): ct_num
            for ct_num in pending_queue
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

    completed_at = datetime.now(timezone.utc).isoformat()
    database.record_pipeline_run(
        run_id=run_id,
        run_type="FULL",
        started_at=started_at,
        completed_at=completed_at,
        status="COMPLETED",
        discovered=discovered,
        processed=len(pending_queue),
        succeeded=succeeded,
        failed=failed,
    )
    logger.info(f"Full catalog sync finished. Discovered: {discovered}, Processed: {len(pending_queue)}, Succeeded: {succeeded}, Failed: {failed}")


def main() -> None:
    """Command-line argument parser and dispatcher."""
    parser_cli = argparse.ArgumentParser(description="EU CTIS Data Pipeline ETL CLI")
    parser_cli.add_argument(
        "--mode",
        choices=["incremental", "full", "single", "retry-failed"],
        default="incremental",
        help="Pipeline execution mode (default: incremental)",
    )
    parser_cli.add_argument(
        "--lookback-days",
        type=int,
        default=config.LOOKBACK_DAYS,
        help="Lookback window in days for incremental sync (default: 7)",
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

    if args.mode == "single":
        if not args.ct_number:
            logger.error("--ct-number is required when using --mode single")
            sys.exit(1)
        ok = process_single_trial(client, args.ct_number, storage_backend=args.storage)
        sys.exit(0 if ok else 1)

    elif args.mode == "incremental":
        run_incremental(
            client=client,
            lookback_days=args.lookback_days,
            max_workers=args.workers,
            storage_backend=args.storage,
        )

    elif args.mode == "full":
        run_full(
            client=client,
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

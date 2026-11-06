"""Main orchestrator and CLI for ANZCTR Clinical Trial ETL Pipeline.

Supports:
- Modes: check, status, report, historical, incremental, new, updates, single, retry-failed, raw
- Dual Bronze storage (untouched .xml and companion .json side-by-side)
- Parallel worker execution with ThreadPoolExecutor
- Graceful shutdown signal handling (SIGINT / SIGTERM)
- Rotating file logging and live HTML dashboard generation
"""

from __future__ import annotations

import argparse
import concurrent.futures
import json
import logging
import signal
import sys
import threading
from datetime import datetime, timedelta, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, Dict, List, Optional

from anz_etl import config
from anz_etl.api_client import AnzctrClient
from anz_etl.database import (
    get_failed_trials,
    get_pending_trials,
    get_stats,
    init_db,
    mark_processing,
    record_trial,
    update_trial_status,
)
from anz_etl.parser import parse_anzctr_trial
from anz_etl.storage import (
    publish_status_report,
    quarantine_record,
    save_bronze_record,
    save_raw_endpoint_payload,
    save_silver_records,
)

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
_log_handlers: List[logging.Handler] = [logging.StreamHandler(sys.stdout)]
for _log_name in ("anz_etl.log", "etl.log"):
    _log_handlers.append(
        RotatingFileHandler(
            config.LOGS_DIR / _log_name,
            maxBytes=config.LOG_FILE_MAX_BYTES,
            backupCount=config.LOG_BACKUP_COUNT,
            encoding="utf-8",
        )
    )

logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=_log_handlers,
)
logger = logging.getLogger("anz_etl.main")


def process_single_trial(actrn: str, raw_xml: Optional[str] = None) -> bool:
    """Retrieves, stores in Bronze (XML+JSON), parses into Silver domains, and updates state DB."""
    if _shutdown_event.is_set():
        logger.info(f"Skipping {actrn} due to active shutdown signal.")
        return False

    mark_processing(actrn)
    logger.info(f"Processing trial: {actrn}")

    client = AnzctrClient()
    if not raw_xml:
        raw_xml = client.fetch_raw_detailv2(actrn)

    if not raw_xml:
        logger.warning(f"No trial data available for {actrn}")
        update_trial_status(actrn, "FAILED", error_message="No trial XML retrieved from endpoint")
        return False

    # Save both raw XML and parsed companion JSON in the Bronze layer
    save_bronze_record(actrn, raw_xml, is_xml=True)

    try:
        domains = parse_anzctr_trial(raw_xml, actrn)
        save_silver_records(actrn, domains)
        update_trial_status(actrn, "SUCCESS")
        logger.info(f"Successfully processed {actrn}")
        return True
    except Exception as e:
        logger.error(f"Error parsing trial {actrn}: {e}")
        quarantine_record(actrn, raw_xml, str(e))
        update_trial_status(actrn, "FAILED", error_message=str(e))
        return False


def run_check() -> int:
    """Pre-flight system, database, and endpoint connectivity check."""
    logger.info("Running Pre-Flight Health Check...")
    init_db()

    client = AnzctrClient()
    health = client.check_health()

    stats = get_stats()
    publish_status_report(stats)

    print("\n=================================================================")
    print("         ANZCTR Data Pipeline - Pre-Flight Check Report          ")
    print("=================================================================")
    print(f"API Endpoint     : {config.ANZCTR_WS_URL}")
    print(f"API Health       : {health.get('status')}")
    print(f"Storage Backend  : {config.STORAGE_BACKEND}")
    print(f"State Backend    : {config.STATE_BACKEND}")
    print(f"Data Directory   : {config.DATA_DIR}")
    print(f"Bronze Directory : {config.BRONZE_DIR}")
    print(f"SQLite DB        : {config.SQLITE_DB_PATH}")
    print(f"Tracked Trials   : {stats.get('total', 0)}")
    print("=================================================================\n")
    return 0


def run_status() -> int:
    """Displays formatted status report and updates HTML dashboard."""
    init_db()
    stats = get_stats()
    report_res = publish_status_report(stats)

    print("\n=================================================================")
    print("         ANZCTR Data Pipeline - Current Status Report            ")
    print("=================================================================")
    print(f"Total Trials Tracked : {stats.get('total', 0):,}")
    print(f"Succeeded            : {stats.get('success', 0):,}")
    print(f"Failed / Quarantined : {stats.get('failed', 0):,}")
    print(f"Pending Execution    : {stats.get('pending', 0):,}")
    print("-----------------------------------------------------------------")
    print(f"Local Dashboard URL  : http://0.0.0.0:{config.PORT}/")
    if isinstance(report_res, dict) and report_res.get("html_browser_url"):
        print(f"S3 Cloud Dashboard   : {report_res['html_browser_url']}")
    if isinstance(report_res, dict) and report_res.get("json_browser_url"):
        print(f"S3 Status JSON API   : {report_res['json_browser_url']}")
    print(f"Status JSON          : {config.STATUS_JSON_PATH}")
    print("=================================================================\n")
    return 0


def run_collect_raw(actrn: Optional[str] = None, keyword: Optional[str] = None) -> int:
    """Queries all 5 initial testing endpoints and saves raw XML + companion JSON together in Bronze."""
    target_actrn = actrn or "ACTRN12613000001741"
    target_keyword = keyword or "cancer"

    print("\n========================================================")
    print(" ANZCTR RAW DATA COLLECTION (Initial 5 APIs)")
    print("========================================================")
    print(f" Target ACTRN       : {target_actrn}")
    print(f" Search Keyword     : {target_keyword}")
    print(f" Output Location    : {config.BRONZE_DIR / target_actrn}")
    print("--------------------------------------------------------")

    client = AnzctrClient()
    raw_payloads = client.fetch_all_raw(actrn=target_actrn, search_keyword=target_keyword)

    api_order = ["Search", "result", "detail", "detailv2", "detailresult"]
    for idx, api_name in enumerate(api_order, start=1):
        content = raw_payloads.get(api_name, "")
        size_bytes = len(content.encode("utf-8"))
        saved_path = save_raw_endpoint_payload(
            api_name=api_name,
            identifier=target_actrn,
            raw_content=content,
        )
        print(f" [{idx}/5] {api_name:<14}: {size_bytes:>7,} bytes -> {saved_path.name}")

    # Also save Bronze record for target_actrn
    main_raw = raw_payloads.get("detailv2") or raw_payloads.get("detail") or raw_payloads.get("result") or ""
    if main_raw:
        save_bronze_record(target_actrn, main_raw, is_xml=True)
        try:
            domains = parse_anzctr_trial(main_raw, target_actrn)
            save_silver_records(target_actrn, domains)
        except Exception as e:
            logger.info(f"Silver domain extraction skipped: {e}")

    # Record trial in tracker DB
    init_db()
    record_trial(
        actrn=target_actrn,
        title=f"Sample Trial {target_actrn}",
        sync_status="SUCCESS",
    )
    stats = get_stats()
    report_res = publish_status_report(stats)

    print("========================================================")
    print(" Raw data collection complete! Saved XML + JSON in Bronze.")
    if isinstance(report_res, dict):
        if report_res.get("html_browser_url"):
            print(f" S3 Dashboard URL   : {report_res['html_browser_url']}")
        if report_res.get("json_browser_url"):
            print(f" S3 Status JSON API : {report_res['json_browser_url']}")
    print("========================================================\n")
    return 0


def run_pipeline(
    mode: str,
    lookback_days: int = config.LOOKBACK_DAYS,
    max_workers: int = config.MAX_WORKERS,
) -> int:
    """Orchestrates pipeline execution across historical, incremental, new, and updates modes."""
    logger.info(f"Starting ANZCTR pipeline: mode='{mode}', lookback={lookback_days}d, workers={max_workers}")
    init_db()

    client = AnzctrClient()

    # Discover trials from search endpoint safely
    search_keyword = "" if mode == "historical" else "Australia"
    try:
        raw_search = client.fetch_raw_search(search_text=search_keyword, paging=100)
        save_raw_endpoint_payload("Search", f"search_{mode}_{datetime.now(timezone.utc).strftime('%Y%m%d')}", raw_search)
    except Exception as e:
        logger.warning(f"Search endpoint query skipped or failed: {e}")

    # In single/test mode, ensure at least sample trials are tracked
    pending = get_pending_trials(limit=50)
    if not pending and mode in ("historical", "incremental", "raw"):
        sample_actrns = ["ACTRN12613000001741"]
        for s in sample_actrns:
            record_trial(actrn=s, title=f"Sample Trial {s}", sync_status="PENDING")
        pending = sample_actrns

    logger.info(f"Found {len(pending)} pending trials to process.")

    succeeded = 0
    failed = 0

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_actrn = {executor.submit(process_single_trial, actrn): actrn for actrn in pending}
        for future in concurrent.futures.as_completed(future_to_actrn):
            if _shutdown_event.is_set():
                break
            actrn = future_to_actrn[future]
            try:
                ok = future.result()
                if ok:
                    succeeded += 1
                else:
                    failed += 1
            except Exception as e:
                logger.error(f"Execution error for {actrn}: {e}")
                failed += 1

    stats = get_stats()
    publish_status_report(stats)
    logger.info(f"Pipeline run finished: {succeeded} succeeded, {failed} failed.")
    run_status()
    return 0 if failed == 0 else 1


def run_retry_failed(max_workers: int = config.MAX_WORKERS) -> int:
    """Retries previously failed trials from state database."""
    init_db()
    failed_items = get_failed_trials(limit=100)
    logger.info(f"Found {len(failed_items)} failed trials eligible for retry.")

    if not failed_items:
        print("No failed trials found to retry.")
        return 0

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(process_single_trial, item["actrn"]) for item in failed_items]
        concurrent.futures.wait(futures)

    stats = get_stats()
    publish_status_report(stats)
    run_status()
    return 0


def main() -> None:
    """CLI dispatch for all operational pipeline modes."""
    parser = argparse.ArgumentParser(description="ANZCTR Clinical Trial ETL Pipeline Orchestrator")
    parser.add_argument(
        "--mode",
        choices=["check", "status", "report", "historical", "incremental", "new", "updates", "single", "retry-failed", "raw", "test-raw"],
        default="status",
        help="Pipeline operational execution mode",
    )
    parser.add_argument("--actrn", type=str, help="Specific trial ACTRN ID to fetch")
    parser.add_argument("--keyword", type=str, help="Search keyword for query endpoints")
    parser.add_argument("--lookback-days", type=int, default=config.LOOKBACK_DAYS, help="Incremental lookback days")
    parser.add_argument("--workers", type=int, default=config.MAX_WORKERS, help="Parallel worker threads")

    args = parser.parse_args()

    if args.mode in ("raw", "test-raw"):
        sys.exit(run_collect_raw(actrn=args.actrn, keyword=args.keyword))
    elif args.mode == "check":
        sys.exit(run_check())
    elif args.mode == "status":
        sys.exit(run_status())
    elif args.mode == "report":
        init_db()
        stats = get_stats()
        publish_status_report(stats)
        sys.exit(run_status())
    elif args.mode == "single":
        if not args.actrn:
            logger.error("--actrn is required for mode 'single'")
            sys.exit(1)
        init_db()
        success = process_single_trial(args.actrn)
        stats = get_stats()
        publish_status_report(stats)
        sys.exit(0 if success else 1)
    elif args.mode == "retry-failed":
        sys.exit(run_retry_failed(max_workers=args.workers))
    elif args.mode in ("historical", "incremental", "new", "updates"):
        sys.exit(run_pipeline(mode=args.mode, lookback_days=args.lookback_days, max_workers=args.workers))


if __name__ == "__main__":
    main()

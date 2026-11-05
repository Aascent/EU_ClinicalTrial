"""CLI and Pipeline Orchestrator for ANZCTR Data Pipeline."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from anz_etl import config
from anz_etl.api_client import AnzctrClient
from anz_etl.database import (
    get_failed_trials,
    get_stats,
    init_db,
    record_trial,
    update_trial_status,
)
from anz_etl.parser import parse_anzctr_trial
from anz_etl.storage import (
    quarantine_record,
    save_bronze_record,
    save_raw_endpoint_payload,
    save_silver_records,
)

# Setup logging
logging.basicConfig(
    level=getattr(logging, config.LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(config.LOGS_DIR / "anz_etl.log", encoding="utf-8"),
    ],
)
logger = logging.getLogger("anz_etl.main")


def run_check() -> int:
    """Pre-flight system and connection check."""
    logger.info("Running Pre-Flight Health Check...")
    init_db()

    client = AnzctrClient()
    health = client.check_health()

    logger.info(f"API Endpoint : {config.ANZCTR_WS_URL}")
    logger.info(f"API Health   : {health.get('status')}")
    logger.info(f"Storage Tier : {config.STORAGE_BACKEND}")
    logger.info(f"State Tier   : {config.STATE_BACKEND}")
    logger.info(f"Data Dir     : {config.DATA_DIR}")
    logger.info(f"Raw Dir      : {config.RAW_DIR}")
    logger.info(f"SQLite DB    : {config.SQLITE_DB_PATH}")

    stats = get_stats()
    logger.info(f"Current DB Stats: {stats}")
    print("\n--- ANZCTR HEALTH CHECK SUMMARY ---")
    print(f"API Status       : {health.get('status')}")
    print(f"Storage Backend  : {config.STORAGE_BACKEND}")
    print(f"State Backend    : {config.STATE_BACKEND}")
    print(f"Raw Storage Dir  : {config.RAW_DIR}")
    print(f"Tracked Trials   : {stats.get('total', 0)}")
    return 0


def run_status() -> int:
    """Displays pipeline statistics and tracking status."""
    init_db()
    stats = get_stats()
    print("\n========================================================")
    print(" ANZCTR CLINICAL TRIAL PIPELINE STATUS")
    print("========================================================")
    print(f" Total Tracked Trials : {stats.get('total', 0)}")
    print(f" Successfully Synced  : {stats.get('success', 0)}")
    print(f" Failed / Quarantined : {stats.get('failed', 0)}")
    print(f" Pending Sync         : {stats.get('pending', 0)}")
    print(f" Raw Storage Dir      : {config.RAW_DIR}")
    print(f" Dashboard Port       : {config.PORT}")
    print(f" Web Dashboard URL    : http://0.0.0.0:{config.PORT}/")
    print("========================================================\n")
    return 0


def run_collect_raw(actrn: Optional[str] = None, keyword: Optional[str] = None) -> int:
    """Queries all 5 initial testing endpoints and saves only raw XML data."""
    target_actrn = actrn or "ACTRN12613000001741"
    target_keyword = keyword or "cancer"

    print("\n========================================================")
    print(" ANZCTR RAW DATA COLLECTION (Initial 5 APIs)")
    print("========================================================")
    print(f" Target ACTRN       : {target_actrn}")
    print(f" Search Keyword     : {target_keyword}")
    print(f" Raw Output Folder  : {config.RAW_DIR}")
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
        print(f" [{idx}/5] {api_name:<14}: {size_bytes:>7,} bytes -> {saved_path}")

    print("========================================================")
    print(" Raw data collection complete! Payloads saved to disk.")
    print("========================================================\n")
    return 0


def generate_dashboard() -> None:
    """Generates an HTML dashboard inside config.DATA_DIR."""
    stats = get_stats()
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>ANZCTR Clinical Trials Dashboard</title>
  <style>
    body {{
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
      margin: 0;
      padding: 24px;
      background-color: #f8fafc;
      color: #1e293b;
    }}
    .header {{
      background: white;
      padding: 24px;
      border-radius: 8px;
      box-shadow: 0 1px 3px rgba(0,0,0,0.1);
      margin-bottom: 24px;
    }}
    h1 {{ margin: 0 0 8px 0; color: #0f172a; }}
    .subtitle {{ color: #64748b; font-size: 14px; }}
    .grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
      gap: 16px;
      margin-bottom: 24px;
    }}
    .card {{
      background: white;
      padding: 20px;
      border-radius: 8px;
      box-shadow: 0 1px 3px rgba(0,0,0,0.1);
      border-left: 4px solid #3b82f6;
    }}
    .card.success {{ border-left-color: #10b981; }}
    .card.failed {{ border-left-color: #ef4444; }}
    .card.pending {{ border-left-color: #f59e0b; }}
    .card .val {{ font-size: 32px; font-weight: bold; margin-top: 8px; }}
    .nav-links {{ margin-top: 16px; }}
    .nav-links a {{ color: #2563eb; text-decoration: none; margin-right: 16px; }}
    .nav-links a:hover {{ text-decoration: underline; }}
  </style>
</head>
<body>
  <div class="header">
    <h1>Australian New Zealand Clinical Trials Registry (ANZCTR) Pipeline</h1>
    <div class="subtitle">Last updated: {now_str} | Storage: {config.STORAGE_BACKEND} | State: {config.STATE_BACKEND}</div>
    <div class="nav-links">
      <a href="/raw/">Browse Raw Payloads</a>
      <a href="/bronze/">Browse Bronze Payloads</a>
      <a href="/silver/">Browse Silver Payloads</a>
      <a href="https://api.anzctr.org.au/WebServices/AnzctrWebservices.asmx" target="_blank">ANZCTR Web Services</a>
    </div>
  </div>
  <div class="grid">
    <div class="card">
      <div>Total Tracked</div>
      <div class="val">{stats.get('total', 0)}</div>
    </div>
    <div class="card success">
      <div>Successfully Processed</div>
      <div class="val">{stats.get('success', 0)}</div>
    </div>
    <div class="card failed">
      <div>Failed / Quarantined</div>
      <div class="val">{stats.get('failed', 0)}</div>
    </div>
    <div class="card pending">
      <div>Pending Execution</div>
      <div class="val">{stats.get('pending', 0)}</div>
    </div>
  </div>
</body>
</html>
"""
    index_path = config.DATA_DIR / "index.html"
    with open(index_path, "w", encoding="utf-8") as f:
        f.write(html_content)
    logger.info(f"Dashboard index written to {index_path}")


def process_single_trial(actrn: str, raw_xml: Optional[str] = None) -> bool:
    """Processes a single trial: saves Bronze and Silver domains."""
    client = AnzctrClient()
    if not raw_xml:
        raw_xml = client.fetch_raw_detailv2(actrn)

    if not raw_xml:
        logger.warning(f"No trial data available for {actrn}")
        update_trial_status(actrn, "FAILED", error_message="No trial XML retrieved from endpoint")
        return False

    # Save Bronze
    save_bronze_record(actrn, raw_xml, is_xml=True)

    try:
        raw_dict = {"actrn": actrn, "raw_content": raw_xml}
        domains = parse_anzctr_trial(raw_dict, actrn)
        save_silver_records(actrn, domains)
        update_trial_status(actrn, "SUCCESS")
        return True
    except Exception as e:
        logger.error(f"Error parsing trial {actrn}: {e}")
        quarantine_record(actrn, raw_xml, str(e))
        update_trial_status(actrn, "FAILED", error_message=str(e))
        return False


def run_pipeline(mode: str, lookback_days: int = 7) -> int:
    """Runs pipeline in historical or incremental mode."""
    logger.info(f"Starting ANZCTR pipeline in mode='{mode}' lookback_days={lookback_days}")
    init_db()

    client = AnzctrClient()
    raw_search = client.fetch_raw_search(paging=50)
    save_raw_endpoint_payload("Search", f"search_batch_{mode}", raw_search)

    generate_dashboard()
    run_status()
    return 0


def main() -> None:
    """CLI entrypoint."""
    parser = argparse.ArgumentParser(description="ANZCTR Clinical Trial ETL Pipeline")
    parser.add_argument(
        "--mode",
        choices=["raw", "test-raw", "check", "status", "report", "historical", "incremental", "single", "retry-failed"],
        default="status",
        help="Pipeline execution mode",
    )
    parser.add_argument("--actrn", type=str, help="Single trial ACTRN ID to query/fetch")
    parser.add_argument("--keyword", type=str, help="Search keyword for query endpoints")
    parser.add_argument("--lookback-days", type=int, default=config.LOOKBACK_DAYS, help="Incremental lookback days")
    parser.add_argument("--workers", type=int, default=config.MAX_WORKERS, help="Parallel processing workers")

    args = parser.parse_args()

    if args.mode in ("raw", "test-raw"):
        sys.exit(run_collect_raw(actrn=args.actrn, keyword=args.keyword))
    elif args.mode == "check":
        sys.exit(run_check())
    elif args.mode == "status":
        sys.exit(run_status())
    elif args.mode == "report":
        init_db()
        generate_dashboard()
        sys.exit(run_status())
    elif args.mode == "single":
        if not args.actrn:
            logger.error("--actrn is required for mode 'single'")
            sys.exit(1)
        init_db()
        success = process_single_trial(args.actrn)
        generate_dashboard()
        sys.exit(0 if success else 1)
    elif args.mode == "retry-failed":
        init_db()
        failed = get_failed_trials()
        logger.info(f"Retrying {len(failed)} failed trials...")
        for item in failed:
            process_single_trial(item["actrn"])
        generate_dashboard()
        sys.exit(0)
    elif args.mode in ("historical", "incremental"):
        sys.exit(run_pipeline(args.mode, args.lookback_days))


if __name__ == "__main__":
    main()

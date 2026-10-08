"""CLI viewer to inspect the tracker.db SQLite database in a clean tabular view."""

import sqlite3
from pathlib import Path

DB_PATH = Path("tracker.db")

def main():
    if not DB_PATH.exists():
        print(f"Database file '{DB_PATH}' does not exist yet.")
        return

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    print("=" * 110)
    print("                      EU CTIS PIPELINE STATE (tracker.db)")
    print("=" * 110)

    # 1. Trials Status
    cursor.execute("""
        SELECT ct_number, status, retry_count, last_publish_date, updated_at
        FROM trials
        ORDER BY updated_at DESC
    """)
    trials = cursor.fetchall()

    print(f"\n[TRIALS TABLE] Total records: {len(trials)}")
    header = f"{'#':<4} | {'Trial Number (ctNumber)':<24} | {'Status':<10} | {'Retries':<7} | {'Publish Date':<25} | {'Updated At'}"
    print(header)
    print("-" * len(header))

    for idx, row in enumerate(trials, 1):
        ct = row["ct_number"]
        st = row["status"]
        ret = row["retry_count"]
        pub = (row["last_publish_date"] or "")[:23]
        upd = (row["updated_at"] or "")[:23]
        print(f"{idx:<4} | {ct:<24} | {st:<10} | {ret:<7} | {pub:<25} | {upd}")

    # 2. Pipeline Runs
    cursor.execute("""
        SELECT run_id, run_type, started_at, completed_at, status, trials_discovered, trials_processed, trials_succeeded, trials_failed
        FROM pipeline_runs
        ORDER BY started_at DESC
        LIMIT 10
    """)
    runs = cursor.fetchall()

    if runs:
        print(f"\n[PIPELINE RUNS TABLE] Recent runs (last {len(runs)}):")
        r_header = f"{'Run Type':<14} | {'Status':<10} | {'Discovered':<10} | {'Processed':<10} | {'Succeeded':<10} | {'Failed':<8} | {'Started At'}"
        print(r_header)
        print("-" * len(r_header))
        for r in runs:
            rtype = r["run_type"]
            st = r["status"]
            disc = r["trials_discovered"]
            proc = r["trials_processed"]
            succ = r["trials_succeeded"]
            fail = r["trials_failed"]
            start = (r["started_at"] or "")[:19]
            print(f"{rtype:<14} | {st:<10} | {disc:<10} | {proc:<10} | {succ:<10} | {fail:<8} | {start}")

    conn.close()

    try:
        from ctis_etl import storage
        report_urls = storage.publish_pipeline_status_report()
        if "json_browser_url" in report_urls:
            print("\n[LIVE STATUS URLS - OPEN DIRECTLY IN BROWSER]")
            print(f"JSON Status URL (7-day link):  {report_urls['json_browser_url']}")
            if "html_browser_url" in report_urls:
                print(f"HTML Dashboard  (7-day link):  {report_urls['html_browser_url']}")
            print(f"Local JSON File:               {report_urls.get('local_json_path')}")
    except Exception as e:
        pass

    print("\n" + "=" * 110)

if __name__ == "__main__":
    main()


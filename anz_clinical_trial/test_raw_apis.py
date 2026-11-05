#!/usr/bin/env python3
"""Standalone script to test and collect raw data from the 5 ANZCTR API endpoints:

1. Search       -> AnzctrTrialSearch
2. result       -> ActSearchResults
3. detail       -> AnzctrTrialDetails
4. detailv2     -> AnzctrTrialDetailsV2
5. detailresult -> ActMoreInfo
"""

import sys
from pathlib import Path

# Add project root to sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent))

from anz_etl.api_client import AnzctrClient
from anz_etl.storage import save_raw_endpoint_payload
from anz_etl import config


def main():
    actrn = sys.argv[1] if len(sys.argv) > 1 else "ACTRN12613000001741"
    keyword = sys.argv[2] if len(sys.argv) > 2 else "cancer"

    print("=" * 60)
    print(" ANZCTR Initial API Testing & Raw Data Collection")
    print("=" * 60)
    print(f" Target ACTRN : {actrn}")
    print(f" Keyword      : {keyword}")
    print(f" Raw Output   : {config.RAW_DIR}")
    print("-" * 60)

    client = AnzctrClient()

    endpoints = [
        ("Search", lambda: client.fetch_raw_search(search_text=keyword)),
        ("result", lambda: client.fetch_raw_result(keyword=keyword)),
        ("detail", lambda: client.fetch_raw_detail(ids=actrn)),
        ("detailv2", lambda: client.fetch_raw_detailv2(ids=actrn, version=1)),
        ("detailresult", lambda: client.fetch_raw_detailresult(trial_id=actrn)),
    ]

    for idx, (name, caller) in enumerate(endpoints, start=1):
        try:
            print(f"[{idx}/5] Fetching raw XML from '{name}'...", end=" ", flush=True)
            raw_xml = caller()
            size = len(raw_xml.encode("utf-8"))
            out_file = save_raw_endpoint_payload(name, actrn, raw_xml)
            print(f"OK ({size:,} bytes) -> {out_file.name}")
        except Exception as e:
            print(f"FAILED ({e})")

    print("-" * 60)
    print(f"Completed! All raw XML files are stored in: {config.RAW_DIR}")
    print("=" * 60)


if __name__ == "__main__":
    main()


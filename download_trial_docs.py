"""CLI script to generate fresh download URLs and download CTIS trial documents as .pdf files."""

import argparse
import json
import logging
import re
from pathlib import Path
from typing import Optional
import requests

from ctis_etl.api_client import CTISClient
from ctis_etl import config

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("download_docs")


def sanitize_filename(name: str) -> str:
    """Removes or replaces characters not allowed in file names."""
    clean = re.sub(r'[<>:"/\\|?*]', "_", name).strip()
    return clean[:180]


def generate_fresh_url(url_or_ct: str, uuid: Optional[str] = None) -> str:
    """Generates a brand-new, active 5-minute pre-signed AWS S3 URL from a permanent endpoint or CT/UUID."""
    client = CTISClient()
    return client.resolve_document_s3_url(url_or_ct, uuid)


def download_single_document(url_or_ct: str, uuid: Optional[str] = None, output_path: Optional[Path] = None) -> Path:
    """Generates a fresh S3 download URL on the fly and saves the PDF to disk."""
    client = CTISClient()
    fresh_s3_url = client.resolve_document_s3_url(url_or_ct, uuid)

    if output_path is None:
        filename = f"{uuid or 'document'}.pdf"
        output_path = Path(filename)

    output_path.parent.mkdir(parents=True, exist_ok=True)

    resp = requests.get(fresh_s3_url, stream=True, timeout=60)
    resp.raise_for_status()

    with open(output_path, "wb") as f:
        for chunk in resp.iter_content(chunk_size=65536):
            f.write(chunk)

    size_kb = output_path.stat().st_size / 1024
    logger.info(f"[OK] Downloaded {output_path.name} ({size_kb:.1f} KB) -> {output_path.resolve()}")
    return output_path


def download_trial_documents(ct_number: str, output_dir: Optional[Path] = None) -> int:
    """Fetches and saves all PDF documents for a clinical trial using fresh URLs."""
    client = CTISClient()

    if output_dir is None:
        output_dir = config.DATA_DIR / "silver" / ct_number / "documents"
    output_dir.mkdir(parents=True, exist_ok=True)

    # 1. Try local silver trial_documents.json first to avoid unnecessary API calls
    silver_docs_file = config.DATA_DIR / "silver" / ct_number / "trial_documents.json"
    doc_entries = []

    if silver_docs_file.exists():
        try:
            with open(silver_docs_file, "r", encoding="utf-8") as f:
                doc_entries = json.load(f)
            logger.info(f"Loaded {len(doc_entries)} documents from local cache: {silver_docs_file}")
        except Exception as e:
            logger.warning(f"Could not read local {silver_docs_file}: {e}")

    # 2. If not found locally, fetch via CTIS retrieve API
    if not doc_entries:
        logger.info(f"Retrieving trial dossier from CTIS API for {ct_number}...")
        full_info = client.retrieve_trial(ct_number)

        def extract_docs(obj):
            if isinstance(obj, dict):
                if "uuid" in obj and ("title" in obj or "documentType" in obj or "fileName" in obj):
                    doc_entries.append(obj)
                for v in obj.values():
                    extract_docs(v)
            elif isinstance(obj, list):
                for item in obj:
                    extract_docs(item)

        extract_docs(full_info)

    # Deduplicate by UUID
    unique_docs = {}
    for d in doc_entries:
        u = d.get("uuid")
        if u and u not in unique_docs:
            unique_docs[u] = d

    if not unique_docs:
        logger.warning(f"No documents found for trial {ct_number}")
        return 0

    logger.info(f"Found {len(unique_docs)} documents for {ct_number}. Resolving fresh URLs & downloading PDFs...")

    downloaded = 0
    for uuid_val, doc_info in unique_docs.items():
        title = doc_info.get("title") or doc_info.get("fileName") or uuid_val
        clean_title = sanitize_filename(title)
        if not clean_title.lower().endswith(".pdf"):
            filename = f"{clean_title}.pdf"
        else:
            filename = clean_title

        target_path = output_dir / filename

        # Support stored permanent API URL if present, otherwise ct_number + uuid
        doc_url = doc_info.get("url") or doc_info.get("downloadUrl")

        try:
            logger.info(f"Resolving fresh download URL for '{filename}' ({uuid_val})...")
            if doc_url and doc_url.startswith("http"):
                s3_url = client.resolve_document_s3_url(doc_url)
            else:
                s3_url = client.resolve_document_s3_url(ct_number, uuid_val)

            if not s3_url:
                logger.warning(f"Could not resolve S3 URL for {uuid_val}")
                continue

            resp = requests.get(s3_url, stream=True, timeout=60)
            resp.raise_for_status()

            with open(target_path, "wb") as f:
                for chunk in resp.iter_content(chunk_size=65536):
                    f.write(chunk)

            size_kb = target_path.stat().st_size / 1024
            logger.info(f"  [OK] Saved {filename} ({size_kb:.1f} KB)")
            downloaded += 1
        except Exception as e:
            logger.error(f"  [FAIL] Failed downloading {uuid_val}: {e}")

    logger.info(f"Finished downloading {downloaded}/{len(unique_docs)} PDF documents to {output_dir.resolve()}")
    return downloaded


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate fresh CTIS download URLs or download documents as .pdf files")
    parser.add_argument("--ct-number", default=None, help="Clinical trial number (e.g. 2026-527084-15-00)")
    parser.add_argument("--uuid", default=None, help="Document UUID")
    parser.add_argument("--url", default=None, help="Permanent download endpoint URL")
    parser.add_argument("--out-dir", type=Path, default=None, help="Output directory for trial PDFs")
    parser.add_argument("--out", type=Path, default=None, help="Output file path for single document")
    parser.add_argument("--get-url", action="store_true", help="Only generate and print a fresh, valid S3 URL without downloading")

    args = parser.parse_args()

    # Mode 1: Generate fresh URL only
    if args.get_url:
        target = args.url or args.ct_number
        if not target:
            parser.error("Either --url or --ct-number with --uuid is required for --get-url.")
        fresh_url = generate_fresh_url(target, args.uuid)
        print("\n=== Fresh 5-Minute Download URL ===")
        print(fresh_url)
        print("===================================\n")

    # Mode 2: Download single document from permanent URL
    elif args.url:
        download_single_document(args.url, args.uuid, args.out)

    # Mode 3: Download all documents for trial
    elif args.ct_number:
        if args.uuid:
            download_single_document(args.ct_number, args.uuid, args.out)
        else:
            download_trial_documents(args.ct_number, args.out_dir)

    else:
        parser.print_help()

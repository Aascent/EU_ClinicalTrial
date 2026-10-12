"""CLI script to download all trial documents as proper .pdf files with their real titles."""

import argparse
import logging
import re
from pathlib import Path
import requests

from ctis_etl.api_client import CTISClient
from ctis_etl import config

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("download_docs")


def sanitize_filename(name: str) -> str:
    """Removes or replaces characters not allowed in file names."""
    clean = re.sub(r'[<>:"/\\|?*]', "_", name).strip()
    return clean[:180]


def download_trial_documents(ct_number: str, output_dir: Path = None) -> int:
    """Fetches and saves all PDF documents for a clinical trial with proper .pdf extensions."""
    client = CTISClient()
    
    if output_dir is None:
        output_dir = config.DATA_DIR / "silver" / ct_number / "documents"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    logger.info(f"Retrieving document metadata for trial {ct_number}...")
    full_info = client.retrieve_trial(ct_number)
    
    # Extract document entries from full trial information
    doc_entries = []
    
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
        
    logger.info(f"Found {len(unique_docs)} documents for {ct_number}. Downloading as .pdf...")
    
    downloaded = 0
    for uuid_val, doc_info in unique_docs.items():
        title = doc_info.get("title") or doc_info.get("fileName") or uuid_val
        clean_title = sanitize_filename(title)
        if not clean_title.lower().endswith(".pdf"):
            filename = f"{clean_title}.pdf"
        else:
            filename = clean_title
            
        target_path = output_dir / filename
        
        try:
            logger.info(f"Resolving download URL for '{filename}' ({uuid_val})...")
            s3_url = client.resolve_document_s3_url(ct_number, uuid_val)
            if not s3_url:
                logger.warning(f"Could not resolve S3 URL for {uuid_val}")
                continue
                
            resp = requests.get(s3_url, stream=True, timeout=30)
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
    parser = argparse.ArgumentParser(description="Download all CTIS documents as .pdf files")
    parser.add_argument("--ct-number", required=True, help="Clinical trial number (e.g. 2025-522058-38-00)")
    parser.add_argument("--out-dir", type=Path, default=None, help="Optional output directory")
    args = parser.parse_args()
    
    download_trial_documents(args.ct_number, args.out_dir)

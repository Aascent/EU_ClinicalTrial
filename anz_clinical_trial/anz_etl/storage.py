"""Storage module for ANZCTR ETL Pipeline (Bronze & Silver layers, S3 & Local)."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

from anz_etl import config

logger = logging.getLogger(__name__)

_s3_client = None


def get_s3_client():
    """Initializes and returns boto3 S3 client if available."""
    global _s3_client
    if _s3_client is not None:
        return _s3_client

    if not config.AWS_ACCESS_KEY_ID or not config.S3_BUCKET_NAME:
        return None

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
            config=Config(retries={"max_attempts": 4, "mode": "adaptive"})
        )
        return _s3_client
    except Exception as e:
        logger.error(f"Failed connecting to S3 bucket '{config.S3_BUCKET_NAME}': {e}")
        return None


def _xml_to_dict(xml_content: str) -> Dict[str, Any]:
    """Converts raw XML text into a nested JSON-compatible dictionary."""
    try:
        import xmltodict
        return xmltodict.parse(xml_content)
    except ImportError:
        pass

    try:
        import xml.etree.ElementTree as ET
        root = ET.fromstring(xml_content)

        def _elem_to_dict(elem):
            tag = elem.tag.split("}")[-1]
            children = list(elem)
            if children:
                d = {}
                for child in children:
                    c_tag = child.tag.split("}")[-1]
                    c_dict = _elem_to_dict(child)
                    if c_tag in d:
                        if not isinstance(d[c_tag], list):
                            d[c_tag] = [d[c_tag]]
                        d[c_tag].append(c_dict[c_tag])
                    else:
                        d[c_tag] = c_dict[c_tag]
                return {tag: d}
            return {tag: (elem.text or "").strip()}

        return _elem_to_dict(root)
    except Exception:
        return {"raw_text": xml_content}


def save_raw_endpoint_payload(api_name: str, identifier: str, raw_content: str) -> Path:
    """Saves raw untouched API XML response and a companion JSON file together in Bronze."""
    safe_id = "".join(c if c.isalnum() or c in ("-", "_") else "_" for c in identifier) or "payload"
    
    # Primary storage: Bronze layer grouped by trial identifier
    bronze_trial_dir = config.BRONZE_DIR / safe_id
    bronze_trial_dir.mkdir(parents=True, exist_ok=True)
    xml_path = bronze_trial_dir / f"{api_name}.xml"
    json_path = bronze_trial_dir / f"{api_name}.json"

    # Mirror copy in data/raw/
    endpoint_dir = config.RAW_DIR / api_name.lower()
    endpoint_dir.mkdir(parents=True, exist_ok=True)
    raw_xml_path = endpoint_dir / f"{safe_id}.xml"
    raw_json_path = endpoint_dir / f"{safe_id}.json"

    try:
        # Save raw XML
        with open(xml_path, "w", encoding="utf-8") as f:
            f.write(raw_content)
        with open(raw_xml_path, "w", encoding="utf-8") as f:
            f.write(raw_content)

        # Convert and save companion JSON
        parsed_dict = _xml_to_dict(raw_content)
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(parsed_dict, f, indent=2, ensure_ascii=False)
        with open(raw_json_path, "w", encoding="utf-8") as f:
            json.dump(parsed_dict, f, indent=2, ensure_ascii=False)

        if config.STORAGE_BACKEND in ("s3", "both"):
            s3 = get_s3_client()
            if s3:
                s3.put_object(
                    Bucket=config.S3_BUCKET_NAME,
                    Key=f"{config.S3_BRONZE_PREFIX}{safe_id}/{api_name}.xml",
                    Body=raw_content.encode("utf-8"),
                    ContentType="application/xml",
                )
                s3.put_object(
                    Bucket=config.S3_BUCKET_NAME,
                    Key=f"{config.S3_BRONZE_PREFIX}{safe_id}/{api_name}.json",
                    Body=json.dumps(parsed_dict, indent=2).encode("utf-8"),
                    ContentType="application/json",
                )
        return json_path
    except Exception as e:
        logger.error(f"Failed saving raw endpoint payload {api_name}/{safe_id}: {e}")
        raise


def save_bronze_record(actrn: str, raw_content: str, is_xml: bool = True) -> bool:
    """Saves raw XML and companion JSON together in the Bronze layer."""
    bronze_dir = config.BRONZE_DIR / actrn
    bronze_dir.mkdir(parents=True, exist_ok=True)

    xml_path = bronze_dir / "raw.xml"
    json_path = bronze_dir / "raw.json"

    try:
        if is_xml:
            with open(xml_path, "w", encoding="utf-8") as f:
                f.write(raw_content)
            parsed_dict = _xml_to_dict(raw_content)
            with open(json_path, "w", encoding="utf-8") as f:
                json.dump(parsed_dict, f, indent=2, ensure_ascii=False)
        else:
            with open(json_path, "w", encoding="utf-8") as f:
                f.write(raw_content)

        if config.STORAGE_BACKEND in ("s3", "both"):
            s3 = get_s3_client()
            if s3:
                if is_xml:
                    s3.put_object(
                        Bucket=config.S3_BUCKET_NAME,
                        Key=f"{config.S3_BRONZE_PREFIX}{actrn}/raw.xml",
                        Body=raw_content.encode("utf-8"),
                        ContentType="application/xml",
                    )
                    s3.put_object(
                        Bucket=config.S3_BUCKET_NAME,
                        Key=f"{config.S3_BRONZE_PREFIX}{actrn}/raw.json",
                        Body=json.dumps(parsed_dict, indent=2).encode("utf-8"),
                        ContentType="application/json",
                    )
                else:
                    s3.put_object(
                        Bucket=config.S3_BUCKET_NAME,
                        Key=f"{config.S3_BRONZE_PREFIX}{actrn}/raw.json",
                        Body=raw_content.encode("utf-8"),
                        ContentType="application/json",
                    )
        return True
    except Exception as e:
        logger.error(f"Failed saving Bronze record for {actrn}: {e}")
        return False


def save_silver_records(actrn: str, domain_payloads: Dict[str, Dict[str, Any]]) -> bool:
    """Saves parsed domain payloads in the Silver layer and data/{actrn} directory."""
    success = True
    for domain_name, data in domain_payloads.items():
        filename = f"{domain_name}.json"
        content_str = json.dumps(data, indent=2, ensure_ascii=False)

        # Save to silver/{actrn}
        silver_path = config.SILVER_DIR / actrn / filename
        silver_path.parent.mkdir(parents=True, exist_ok=True)
        # Also mirror to data/{actrn} for direct dashboard visibility
        root_data_path = config.DATA_DIR / actrn / filename
        root_data_path.parent.mkdir(parents=True, exist_ok=True)

        try:
            with open(silver_path, "w", encoding="utf-8") as f:
                f.write(content_str)
            with open(root_data_path, "w", encoding="utf-8") as f:
                f.write(content_str)

            if config.STORAGE_BACKEND in ("s3", "both"):
                s3 = get_s3_client()
                if s3:
                    s3_key = f"{config.S3_SILVER_PREFIX}{actrn}/{filename}"
                    s3.put_object(
                        Bucket=config.S3_BUCKET_NAME,
                        Key=s3_key,
                        Body=content_str.encode("utf-8"),
                        ContentType="application/json",
                    )
        except Exception as e:
            logger.error(f"Failed saving Silver record {domain_name} for {actrn}: {e}")
            success = False

    return success


def quarantine_record(actrn: str, raw_content: str, error_reason: str) -> Path:
    """Stores unparseable or error records in quarantine."""
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    path = config.QUARANTINE_DIR / f"{actrn}_{timestamp}.json"
    payload = {
        "actrn": actrn,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "error": error_reason,
        "content": raw_content,
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
    return path


def publish_status_report(stats: Dict[str, Any]) -> Path:
    """Publishes current pipeline status metrics as JSON and builds the HTML dashboard."""
    now_utc = datetime.now(timezone.utc)
    report_data = {
        "registry": "ANZCTR",
        "last_updated": now_utc.isoformat(),
        "total_trials": stats.get("total", 0),
        "succeeded": stats.get("success", 0),
        "failed": stats.get("failed", 0),
        "pending": stats.get("pending", 0),
        "storage_backend": config.STORAGE_BACKEND,
        "state_backend": config.STATE_BACKEND,
        "status_code": "HEALTHY" if stats.get("failed", 0) == 0 else "DEGRADED",
    }

    # Write pipeline_status.json
    status_path = config.STATUS_JSON_PATH
    with open(status_path, "w", encoding="utf-8") as f:
        json.dump(report_data, f, indent=2)

    # Build interactive HTML dashboard
    now_str = now_utc.strftime("%Y-%m-%d %H:%M:%S UTC")
    html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <meta http-equiv="refresh" content="60">
  <title>ANZCTR Clinical Trials Dashboard</title>
  <style>
    body {{
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
      margin: 0;
      padding: 32px;
      background-color: #f1f5f9;
      color: #0f172a;
    }}
    .header {{
      background: white;
      padding: 28px 32px;
      border-radius: 12px;
      box-shadow: 0 4px 6px -1px rgba(0,0,0,0.05);
      margin-bottom: 24px;
      border-left: 6px solid #0284c7;
    }}
    h1 {{ margin: 0 0 8px 0; font-size: 26px; }}
    .subtitle {{ color: #64748b; font-size: 14px; }}
    .grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
      gap: 16px;
      margin-bottom: 24px;
    }}
    .card {{
      background: white;
      padding: 22px;
      border-radius: 12px;
      box-shadow: 0 2px 4px rgba(0,0,0,0.04);
      border-left: 5px solid #0284c7;
    }}
    .card.success {{ border-left-color: #10b981; }}
    .card.failed {{ border-left-color: #ef4444; }}
    .card.pending {{ border-left-color: #f59e0b; }}
    .card .label {{ color: #64748b; font-size: 13px; text-transform: uppercase; letter-spacing: 0.05em; font-weight: 600; }}
    .card .val {{ font-size: 36px; font-weight: 700; margin-top: 8px; color: #0f172a; }}
    .nav-box {{
      background: white;
      padding: 20px 24px;
      border-radius: 12px;
      box-shadow: 0 2px 4px rgba(0,0,0,0.04);
    }}
    .nav-box h3 {{ margin: 0 0 12px 0; font-size: 16px; }}
    .nav-links a {{
      display: inline-block;
      color: #0284c7;
      text-decoration: none;
      font-weight: 500;
      margin-right: 20px;
      padding: 6px 12px;
      background: #f0f9ff;
      border-radius: 6px;
      border: 1px solid #bae6fd;
    }}
    .nav-links a:hover {{ background: #e0f2fe; text-decoration: underline; }}
  </style>
</head>
<body>
  <div class="header">
    <h1>Australian New Zealand Clinical Trials Registry (ANZCTR) Pipeline</h1>
    <div class="subtitle">Live status updated: {now_str} | Storage: {config.STORAGE_BACKEND} | State: {config.STATE_BACKEND}</div>
  </div>
  <div class="grid">
    <div class="card">
      <div class="label">Total Tracked</div>
      <div class="val">{stats.get('total', 0):,}</div>
    </div>
    <div class="card success">
      <div class="label">Succeeded</div>
      <div class="val">{stats.get('success', 0):,}</div>
    </div>
    <div class="card failed">
      <div class="label">Failed</div>
      <div class="val">{stats.get('failed', 0):,}</div>
    </div>
    <div class="card pending">
      <div class="label">Pending</div>
      <div class="val">{stats.get('pending', 0):,}</div>
    </div>
  </div>
  <div class="nav-box">
    <h3>Storage & Endpoints</h3>
    <div class="nav-links">
      <a href="/bronze/">Browse Bronze Payloads</a>
      <a href="/silver/">Browse Silver Payloads</a>
      <a href="/raw/">Browse Raw API Payloads</a>
      <a href="/pipeline_status.json" target="_blank">Status JSON API</a>
      <a href="https://api.anzctr.org.au/WebServices/AnzctrWebservices.asmx" target="_blank">ANZCTR Web Services</a>
    </div>
  </div>
</body>
</html>
"""
    dashboard_path = config.DATA_DIR / "index.html"
    status_html_path = config.DATA_DIR / "pipeline_status.html"
    with open(dashboard_path, "w", encoding="utf-8") as f:
        f.write(html_content)
    with open(status_html_path, "w", encoding="utf-8") as f:
        f.write(html_content)

    results = {
        "status_json_path": str(status_path),
        "local_html_path": str(dashboard_path),
        "html_browser_url": None,
        "json_browser_url": None,
    }

    if config.STORAGE_BACKEND in ("s3", "both"):
        s3 = get_s3_client()
        if s3:
            try:
                # 365 days expiration
                expires_in = 365 * 86400
                s3_json_key = f"{config.S3_PREFIX}pipeline_status.json"
                s3_html_key = f"{config.S3_PREFIX}pipeline_status.html"

                s3.put_object(
                    Bucket=config.S3_BUCKET_NAME,
                    Key=s3_json_key,
                    Body=json.dumps(report_data, indent=2).encode("utf-8"),
                    ContentType="application/json",
                    CacheControl="no-cache, no-store, must-revalidate",
                )
                s3.put_object(
                    Bucket=config.S3_BUCKET_NAME,
                    Key=s3_html_key,
                    Body=html_content.encode("utf-8"),
                    ContentType="text/html",
                    CacheControl="no-cache, no-store, must-revalidate",
                )
                s3.put_object(
                    Bucket=config.S3_BUCKET_NAME,
                    Key=f"{config.S3_PREFIX}index.html",
                    Body=html_content.encode("utf-8"),
                    ContentType="text/html",
                    CacheControl="no-cache, no-store, must-revalidate",
                )

                json_url = s3.generate_presigned_url(
                    "get_object",
                    Params={"Bucket": config.S3_BUCKET_NAME, "Key": s3_json_key},
                    ExpiresIn=expires_in,
                )
                html_url = s3.generate_presigned_url(
                    "get_object",
                    Params={"Bucket": config.S3_BUCKET_NAME, "Key": s3_html_key},
                    ExpiresIn=expires_in,
                )
                results["html_browser_url"] = html_url
                results["json_browser_url"] = json_url
            except Exception as e:
                logger.error(f"Failed uploading status report to S3: {e}")

    return results


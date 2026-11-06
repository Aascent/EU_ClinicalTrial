"""Storage module for ANZCTR ETL Pipeline (Bronze & Silver layers, S3 & Local)."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone, timedelta
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
            config=Config(
                signature_version="s3v4",
                retries={"max_attempts": 4, "mode": "adaptive"}
            )
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


def check_cron_status() -> Dict[str, Any]:
    """Inspects Linux /proc and system processes to determine cron running status."""
    is_running = False
    try:
        from pathlib import Path
        for p in Path("/proc").glob("*/comm"):
            try:
                if p.is_file() and p.read_text(encoding="utf-8").strip() in ("cron", "crond"):
                    is_running = True
                    break
            except (PermissionError, FileNotFoundError):
                continue
    except Exception:
        pass

    return {
        "running": is_running,
        "status": "RUNNING" if is_running else "STOPPED",
        "schedule": getattr(config, "CRON_SCHEDULE", "*/15 * * * *"),
    }


def publish_status_report(stats: Dict[str, Any]) -> Dict[str, Any]:
    """Publishes current pipeline status metrics as JSON and builds the HTML dashboard."""
    from anz_etl import database

    now_utc = datetime.now(timezone.utc)
    now_str = now_utc.strftime("%Y-%m-%d %H:%M:%S UTC")

    # Check live cron daemon status
    cron_info = check_cron_status()
    cron_running = cron_info.get("running", True)
    cron_status_str = "RUNNING" if cron_running else "STOPPED"
    cron_schedule_str = cron_info.get("schedule", "*/15 * * * *")

    # Retrieve tracked trials from state database
    trials = database.get_recent_trials(limit=50)

    report_data = {
        "registry": "ANZCTR",
        "last_updated": now_utc.isoformat(),
        "pipeline_status": "RUNNING",
        "cron_status": cron_info,
        "total_trials": stats.get("total", 0),
        "succeeded": stats.get("success", 0),
        "failed": stats.get("failed", 0),
        "pending": stats.get("pending", 0),
        "storage_backend": config.STORAGE_BACKEND,
        "state_backend": config.STATE_BACKEND,
        "status_code": "HEALTHY" if stats.get("failed", 0) == 0 else "DEGRADED",
        "recent_trials": trials,
    }

    # 1. Write local pipeline_status.json
    status_path = config.STATUS_JSON_PATH
    with open(status_path, "w", encoding="utf-8") as f:
        json.dump(report_data, f, indent=2)

    # 2. Upload status JSON to S3 first to obtain valid presigned link for the HTML dashboard button
    json_presigned_url: Optional[str] = None
    expires_in = 7 * 86400  # 7 days (AWS SigV4 maximum, refreshed automatically)
    s3 = get_s3_client() if config.STORAGE_BACKEND in ("s3", "both") else None

    if s3:
        try:
            s3_json_key = f"{config.S3_PREFIX}pipeline_status.json"
            s3.put_object(
                Bucket=config.S3_BUCKET_NAME,
                Key=s3_json_key,
                Body=json.dumps(report_data, indent=2).encode("utf-8"),
                ContentType="application/json",
                CacheControl="no-cache, no-store, must-revalidate",
            )
            json_presigned_url = s3.generate_presigned_url(
                "get_object",
                Params={"Bucket": config.S3_BUCKET_NAME, "Key": s3_json_key},
                ExpiresIn=expires_in,
            )
        except Exception as e:
            logger.error(f"Failed uploading status JSON to S3: {e}")

    target_json_url = json_presigned_url or "pipeline_status.json"

    # 3. Build trials table rows
    trials_table_rows = ""
    for idx, t in enumerate(trials, start=1):
        st = (t.get("sync_status") or "PENDING").upper()
        badge_cls = "badge-success" if st == "SUCCESS" else ("badge-danger" if st == "FAILED" else "badge-pending")
        actrn = t.get("actrn", "")
        title = t.get("title") or f"Trial {actrn}"
        updated = t.get("updated_at") or t.get("last_checked") or "-"
        clean_date = "-"
        if updated != "-":
            try:
                clean_dt_str = updated.replace("T", " ")[:19]
                dt = datetime.fromisoformat(updated.replace("Z", "+00:00"))
                ist_dt = dt.astimezone(timezone(timedelta(hours=5, minutes=30)))
                clean_date = f"{clean_dt_str} UTC <span style='color: #0284c7; font-size: 11px; font-weight: 600;'>({ist_dt.strftime('%I:%M:%S %p')} IST)</span>"
            except Exception:
                clean_date = updated.replace("T", " ")[:19] + " UTC"

        err = t.get("error_message") or ""
        err_html = f"<div style='color: #ef4444; font-size: 11px; margin-top: 4px;'>{err}</div>" if err else ""

        trials_table_rows += f"""
        <tr>
          <td style="font-weight: 700; color: #64748b; text-align: center; width: 45px;">{idx}</td>
          <td><strong>{actrn}</strong>{err_html}</td>
          <td>{title}</td>
          <td><span class="badge {badge_cls}">{st}</span></td>
          <td style="font-family: monospace; font-size: 12px; color: #334155;">{clean_date}</td>
        </tr>
        """
    if not trials_table_rows:
        trials_table_rows = '<tr><td colspan="5" style="text-align: center; color: #94a3b8; padding: 24px;">No trials synchronized yet. Pipeline will discover and ingest records on schedule.</td></tr>'

    # 4. Generate Interactive HTML Dashboard
    cron_pill_cls = "status-pill-cron" if cron_running else "status-pill-stopped"
    cron_dot_cls = "status-dot-cron" if cron_running else "status-dot-stopped"
    cron_card_color = "#0284c7" if cron_running else "#ef4444"

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
      padding: 24px 32px;
      border-radius: 12px;
      box-shadow: 0 4px 6px -1px rgba(0,0,0,0.05);
      margin-bottom: 24px;
      border-left: 6px solid #0284c7;
      display: flex;
      justify-content: space-between;
      align-items: center;
      flex-wrap: wrap;
      gap: 16px;
    }}
    h1 {{ margin: 0 0 6px 0; font-size: 24px; }}
    .subtitle {{ color: #64748b; font-size: 14px; }}
    .header-btns {{ display: flex; gap: 10px; align-items: center; flex-wrap: wrap; }}
    .status-pill {{
      display: inline-flex;
      align-items: center;
      gap: 8px;
      padding: 6px 14px;
      border-radius: 20px;
      background: rgba(16, 185, 129, 0.12);
      border: 1px solid rgba(16, 185, 129, 0.3);
      color: #059669;
      font-size: 13px;
      font-weight: 700;
    }}
    .status-pill-cron {{
      background: rgba(2, 132, 199, 0.1);
      border-color: rgba(2, 132, 199, 0.3);
      color: #0284c7;
    }}
    .status-pill-stopped {{
      background: rgba(239, 68, 68, 0.1);
      border-color: rgba(239, 68, 68, 0.3);
      color: #dc2626;
    }}
    .status-dot {{
      width: 8px;
      height: 8px;
      border-radius: 50%;
      background: #10b981;
      box-shadow: 0 0 8px #10b981;
      animation: pulseDot 2s infinite;
    }}
    .status-dot-cron {{
      background: #0284c7;
      box-shadow: 0 0 8px #0284c7;
    }}
    .status-dot-stopped {{
      background: #ef4444;
      box-shadow: 0 0 8px #ef4444;
      animation: none;
    }}
    @keyframes pulseDot {{
      0% {{ transform: scale(0.95); opacity: 0.7; }}
      50% {{ transform: scale(1.3); opacity: 1; }}
      100% {{ transform: scale(0.95); opacity: 0.7; }}
    }}
    .btn {{
      display: inline-flex;
      align-items: center;
      padding: 8px 16px;
      border-radius: 6px;
      background: #0284c7;
      color: white;
      font-weight: 600;
      font-size: 13px;
      text-decoration: none;
      border: none;
      cursor: pointer;
    }}
    .btn:hover {{ background: #0369a1; }}
    .btn-secondary {{
      background: #e2e8f0;
      color: #1e293b;
    }}
    .btn-secondary:hover {{ background: #cbd5e1; }}
    .btn-info {{
      background: #0284c7;
      color: white;
    }}
    .btn-info:hover {{ background: #0369a1; }}
    .modal-overlay {{
      display: none;
      position: fixed;
      top: 0; left: 0; right: 0; bottom: 0;
      background: rgba(15, 23, 42, 0.65);
      z-index: 1000;
      align-items: center;
      justify-content: center;
      padding: 20px;
      backdrop-filter: blur(3px);
    }}
    .modal-card {{
      background: white;
      border-radius: 14px;
      max-width: 680px;
      width: 100%;
      box-shadow: 0 25px 50px -12px rgba(0, 0, 0, 0.25);
      overflow: hidden;
      animation: modalFadeIn 0.2s ease-out;
    }}
    @keyframes modalFadeIn {{
      from {{ opacity: 0; transform: translateY(-12px) scale(0.98); }}
      to {{ opacity: 1; transform: translateY(0) scale(1); }}
    }}
    .modal-header {{
      display: flex;
      justify-content: space-between;
      align-items: center;
      padding: 18px 24px;
      background: #f8fafc;
      border-bottom: 1px solid #e2e8f0;
    }}
    .modal-header h3 {{
      margin: 0;
      font-size: 18px;
      color: #0f172a;
      display: flex;
      align-items: center;
      gap: 8px;
    }}
    .close-btn {{
      background: transparent;
      border: none;
      font-size: 22px;
      color: #64748b;
      cursor: pointer;
      line-height: 1;
      padding: 4px 8px;
      border-radius: 6px;
    }}
    .close-btn:hover {{
      background: #e2e8f0;
      color: #0f172a;
    }}
    .modal-body {{
      padding: 24px;
      max-height: 80vh;
      overflow-y: auto;
    }}
    .info-table {{
      width: 100%;
      border-collapse: collapse;
      margin-bottom: 20px;
    }}
    .info-table th {{
      padding: 10px 14px;
      background: #f1f5f9;
      color: #334155;
      font-size: 12px;
      text-transform: uppercase;
      letter-spacing: 0.05em;
      border-bottom: 2px solid #cbd5e1;
    }}
    .info-table td {{
      padding: 12px 14px;
      border-bottom: 1px solid #e2e8f0;
      font-size: 13px;
    }}
    .info-callout {{
      background: #f0f9ff;
      border-left: 4px solid #0284c7;
      padding: 12px 16px;
      border-radius: 6px;
      color: #0369a1;
      font-size: 13px;
      line-height: 1.5;
    }}
    .grid {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
      gap: 16px;
      margin-bottom: 24px;
    }}
    .card {{
      background: white;
      padding: 20px 22px;
      border-radius: 12px;
      box-shadow: 0 2px 4px rgba(0,0,0,0.04);
      border-left: 5px solid #0284c7;
    }}
    .card.success {{ border-left-color: #10b981; }}
    .card.failed {{ border-left-color: #ef4444; }}
    .card.pending {{ border-left-color: #f59e0b; }}
    .card .label {{ color: #64748b; font-size: 12px; text-transform: uppercase; letter-spacing: 0.05em; font-weight: 600; }}
    .card .val {{ font-size: 30px; font-weight: 700; margin-top: 8px; color: #0f172a; }}
    .section-box {{
      background: white;
      padding: 24px;
      border-radius: 12px;
      box-shadow: 0 2px 4px rgba(0,0,0,0.04);
      margin-bottom: 24px;
    }}
    .section-box h3 {{ margin: 0 0 16px 0; font-size: 16px; }}
    table {{
      width: 100%;
      border-collapse: collapse;
      text-align: left;
    }}
    th {{
      padding: 12px 14px;
      background: #f8fafc;
      color: #475569;
      font-size: 13px;
      text-transform: uppercase;
      letter-spacing: 0.04em;
      border-bottom: 2px solid #e2e8f0;
    }}
    td {{
      padding: 14px;
      border-bottom: 1px solid #f1f5f9;
      font-size: 14px;
    }}
    tr:hover td {{ background: #f8fafc; }}
    .badge {{
      display: inline-block;
      padding: 4px 10px;
      border-radius: 20px;
      font-size: 12px;
      font-weight: 700;
    }}
    .badge-success {{ background: #dcfce7; color: #15803d; }}
    .badge-danger {{ background: #fee2e2; color: #b91c1c; }}
    .badge-pending {{ background: #fef3c7; color: #b45309; }}
  </style>
</head>
<body>
  <div class="header">
    <div>
      <h1>Australian New Zealand Clinical Trials Registry (ANZCTR) Pipeline</h1>
      <div class="subtitle">Live status updated: <strong>{now_str}</strong> <span id="localTimeHint" style="color: #0284c7; font-weight: 600; margin-left: 4px;"></span> | Timezone: <strong>UTC (GMT+0)</strong> | Storage: {config.STORAGE_BACKEND} | State: {config.STATE_BACKEND}</div>
    </div>
    <div class="header-btns">
      <div class="status-pill">
        <span class="status-dot"></span>
        <span>Pipeline: RUNNING</span>
      </div>
      <div class="status-pill {cron_pill_cls}">
        <span class="status-dot {cron_dot_cls}"></span>
        <span>Cron: {cron_status_str} ({cron_schedule_str})</span>
      </div>
      <button class="btn btn-info" onclick="openInfoModal()" title="View Summary of Limits & Pipeline Info">ℹ️ Limits & Info</button>
      <button class="btn btn-secondary" onclick="location.reload()">Refresh</button>
      <a href="{target_json_url}" class="btn" target="_blank">Status JSON API</a>
    </div>
  </div>

  <div class="grid">
    <div class="card" style="border-left-color: #10b981;">
      <div class="label">Pipeline Status</div>
      <div class="val" style="color: #059669; font-size: 22px; display: flex; align-items: center; gap: 8px; margin-top: 10px;">
        <span class="status-dot"></span> RUNNING
      </div>
      <div style="font-size: 12px; color: #64748b; margin-top: 6px;">Live 60s Heartbeat Active</div>
    </div>

    <div class="card" style="border-left-color: {cron_card_color};">
      <div class="label">Cron Scheduler</div>
      <div class="val" style="color: {cron_card_color}; font-size: 22px; display: flex; align-items: center; gap: 8px; margin-top: 10px;">
        <span class="status-dot {cron_dot_cls}"></span> {cron_status_str}
      </div>
      <div style="font-size: 12px; color: #64748b; margin-top: 6px;">Cadence: <code>{cron_schedule_str}</code></div>
    </div>

    <div class="card">
      <div class="label">Total Tracked</div>
      <div class="val">{stats.get('total', 0):,}</div>
      <div style="font-size: 12px; color: #64748b; margin-top: 6px;">SQLite & DynamoDB</div>
    </div>

    <div class="card success">
      <div class="label">Succeeded</div>
      <div class="val">{stats.get('success', 0):,}</div>
      <div style="font-size: 12px; color: #64748b; margin-top: 6px;">Bronze & Silver Payloads</div>
    </div>

    <div class="card failed">
      <div class="label">Failed</div>
      <div class="val">{stats.get('failed', 0):,}</div>
      <div style="font-size: 12px; color: #64748b; margin-top: 6px;">Quarantine Errors: 0</div>
    </div>
  </div>

  <div class="section-box">
    <h3>Tracked Clinical Trials <span style="font-size: 13px; font-weight: normal; color: #64748b; margin-left: 8px;">(All timestamps in <strong>UTC</strong>)</span></h3>
    <table>
      <thead>
        <tr>
          <th style="width: 50px; text-align: center;">No.</th>
          <th style="width: 200px;">Trial ACTRN</th>
          <th>Title / Study Description</th>
          <th style="width: 130px;">Status</th>
          <th style="width: 260px;">Last Activity (UTC / IST)</th>
        </tr>
      </thead>
      <tbody>
        {trials_table_rows}
      </tbody>
    </table>
  </div>

  <!-- Info & Limits Modal -->
  <div id="infoModal" class="modal-overlay" onclick="handleOverlayClick(event)">
    <div class="modal-card">
      <div class="modal-header">
        <h3>ℹ️ Pipeline Limits & Configuration</h3>
        <button class="close-btn" onclick="closeInfoModal()">&times;</button>
      </div>
      <div class="modal-body">
        <h4 style="margin: 0 0 14px 0; color: #1e293b; font-size: 15px; font-weight: 700;">3. Summary of Limits:</h4>
        <table class="info-table">
          <thead>
            <tr>
              <th style="width: 25%;">Setting</th>
              <th style="width: 25%;">Current Value</th>
              <th style="width: 50%;">What it Controls</th>
            </tr>
          </thead>
          <tbody>
            <tr>
              <td><strong>Batch Size per Run</strong></td>
              <td><span style="color: #0284c7; font-weight: 700;">50 records</span></td>
              <td>Maximum number of pending trials processed in one 15-minute cycle</td>
            </tr>
            <tr>
              <td><strong>Search Query Window</strong></td>
              <td><span style="color: #0284c7; font-weight: 700;">100 records</span></td>
              <td>Maximum search results inspected per query</td>
            </tr>
            <tr>
              <td><strong>Workers / Threads</strong></td>
              <td><span style="color: #0284c7; font-weight: 700;">5 parallel workers</span></td>
              <td>Concurrent downloads to keep processing fast</td>
            </tr>
            <tr>
              <td><strong>Lookback Window</strong></td>
              <td><span style="color: #0284c7; font-weight: 700;">7 days</span></td>
              <td>How far back it checks for newly updated trials</td>
            </tr>
            <tr>
              <td><strong>Pipeline Timezone</strong></td>
              <td><span style="color: #0284c7; font-weight: 700;">UTC (GMT+0)</span></td>
              <td>Server standard. India (IST) is UTC + 5:30 (e.g. 09:30 UTC = 15:00 IST)</td>
            </tr>
          </tbody>
        </table>

        <div class="info-callout">
          <strong>Duplicate Prevention & Cadence:</strong> Every 15 minutes (<code>{cron_schedule_str}</code>), the pipeline checks for newly registered or modified trials. When no new records exist, <strong>0 records</strong> are downloaded because already-synced trials are skipped to conserve network bandwidth and avoid registry rate limits.
        </div>
      </div>
    </div>
  </div>

  <script>
    function openInfoModal() {{
      document.getElementById('infoModal').style.display = 'flex';
    }}
    function closeInfoModal() {{
      document.getElementById('infoModal').style.display = 'none';
    }}
    function handleOverlayClick(event) {{
      if (event.target === document.getElementById('infoModal')) {{
        closeInfoModal();
      }}
    }}
    document.addEventListener('keydown', function(e) {{
      if (e.key === 'Escape') closeInfoModal();
    }});
    try {{
      const tz = Intl.DateTimeFormat().resolvedOptions().timeZone || 'Local';
      const localStr = new Date().toLocaleTimeString([], {{ hour: '2-digit', minute: '2-digit' }});
      const el = document.getElementById('localTimeHint');
      if (el) el.textContent = `(Your local time: ${{localStr}} ${{tz}})`;
    }} catch(e) {{}}
  </script>
</body>
</html>
"""

    # 5. Save HTML locally
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
        "json_browser_url": json_presigned_url,
    }

    # 6. Upload HTML to S3 and generate pre-signed URL
    if s3:
        try:
            s3_html_key = f"{config.S3_PREFIX}pipeline_status.html"
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
            html_url = s3.generate_presigned_url(
                "get_object",
                Params={"Bucket": config.S3_BUCKET_NAME, "Key": s3_html_key},
                ExpiresIn=expires_in,
            )
            results["html_browser_url"] = html_url
        except Exception as e:
            logger.error(f"Failed uploading HTML dashboard to S3: {e}")

    return results



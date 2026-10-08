"""Storage module handling writing trial JSON outputs to local disk and AWS S3."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

from ctis_etl import config

logger = logging.getLogger(__name__)

# Lazy S3 client
_s3_client = None


def get_s3_client():
    """Returns an authenticated boto3 S3 client if configured."""
    global _s3_client
    if _s3_client is not None:
        return _s3_client

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
            config=Config(retries={"max_attempts": 3, "mode": "standard"})
        )
        return _s3_client
    except ImportError:
        logger.warning("boto3 is not installed. S3 uploads are disabled until boto3 is installed.")
        return None
    except Exception as e:
        logger.error(f"Failed to initialize S3 client: {e}")
        return None


def _write_local_json(target_path: Path, payload: Any) -> bool:
    """Helper to atomically write a JSON file and verify non-empty size."""
    try:
        target_path.parent.mkdir(parents=True, exist_ok=True)
        temp_path = target_path.with_suffix(".tmp")
        with open(temp_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, ensure_ascii=False)
        temp_path.replace(target_path)
        return target_path.stat().st_size > 0
    except Exception as e:
        logger.error(f"Failed writing local file {target_path}: {e}")
        return False


def _upload_s3_json(
    s3_client: Any,
    bucket: str,
    key: str,
    payload: Any,
    metadata: Dict[str, str],
) -> bool:
    """Helper to upload a JSON payload to S3 with encryption and metadata tags."""
    try:
        payload_bytes = json.dumps(payload, indent=2, ensure_ascii=False).encode("utf-8")
        put_kwargs = {
            "Bucket": bucket,
            "Key": key,
            "Body": payload_bytes,
            "ContentType": "application/json",
            "Metadata": metadata,
        }
        if config.S3_SERVER_SIDE_ENCRYPTION:
            put_kwargs["ServerSideEncryption"] = config.S3_SERVER_SIDE_ENCRYPTION
        s3_client.put_object(**put_kwargs)
        return True
    except Exception as e:
        logger.error(f"Failed uploading to s3://{bucket}/{key}: {e}")
        return False


def save_trial_files(
    ct_number: str,
    parsed_files: Dict[str, Any],
    raw_payload: Optional[Dict[str, Any]] = None,
    backend: Optional[str] = None,
) -> Dict[str, bool]:
    """Persists trial entities across Bronze (raw) and Silver (6 domain files) tiers.

    Args:
        ct_number: Trial identifier (e.g. '2026-527084-15-00')
        parsed_files: Silver tier dictionary of filename -> domain JSON payload (6 files)
        raw_payload: Bronze tier unmodified API response payload (raw.json)
        backend: Storage backend ('local', 's3', or 'both'). Defaults to config.STORAGE_BACKEND.

    Returns:
        Dict indicating success status for each persisted entity.
    """
    backend = (backend or config.STORAGE_BACKEND).lower()
    results: Dict[str, bool] = {}

    save_local = backend in ("local", "both")
    save_s3 = backend in ("s3", "both")

    # Local Directory Persistence
    if save_local:
        # 1. Bronze Tier (Raw untouched response)
        if raw_payload is not None:
            bronze_path = config.BRONZE_DIR / ct_number / "raw.json"
            results["local:bronze:raw.json"] = _write_local_json(bronze_path, raw_payload)

        # 2. Silver Tier (6 domain JSON files)
        for filename, payload in parsed_files.items():
            silver_path = config.SILVER_DIR / ct_number / filename
            results[f"local:silver:{filename}"] = _write_local_json(silver_path, payload)

    # AWS S3 Persistence
    if save_s3:
        s3 = get_s3_client()
        if s3 is None:
            logger.warning(f"S3 client unavailable. Falling back to local disk for {ct_number}.")
            if not save_local:
                return save_trial_files(
                    ct_number=ct_number,
                    parsed_files=parsed_files,
                    raw_payload=raw_payload,
                    backend="local",
                )
        else:
            now_iso = datetime.now(timezone.utc).isoformat()

            # 1. Bronze Tier Upload
            if raw_payload is not None:
                bronze_key = f"{config.S3_BRONZE_PREFIX}{ct_number}/raw.json"
                results["s3:bronze:raw.json"] = _upload_s3_json(
                    s3,
                    config.S3_BUCKET_NAME,
                    bronze_key,
                    raw_payload,
                    {"ct-number": ct_number, "filename": "raw.json", "ingested-at": now_iso},
                )

            # 2. Silver Tier Uploads
            for filename, payload in parsed_files.items():
                silver_key = f"{config.S3_SILVER_PREFIX}{ct_number}/{filename}"
                results[f"s3:silver:{filename}"] = _upload_s3_json(
                    s3,
                    config.S3_BUCKET_NAME,
                    silver_key,
                    payload,
                    {"ct-number": ct_number, "filename": filename, "ingested-at": now_iso},
                )

    return results


def quarantine_payload(ct_number: str, raw_payload: Any, error_msg: str) -> Path:
    """Saves malformed or failed payload to quarantine directory for post-mortem analysis."""
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    safe_ct = ct_number.replace("/", "_")
    base_name = f"{safe_ct}_{timestamp}"

    json_path = config.QUARANTINE_DIR / f"{base_name}.json"
    err_path = config.QUARANTINE_DIR / f"{base_name}.error.log"

    try:
        with open(json_path, "w", encoding="utf-8") as f:
            if isinstance(raw_payload, (dict, list)):
                json.dump(raw_payload, f, indent=2, ensure_ascii=False)
            else:
                f.write(str(raw_payload))

        with open(err_path, "w", encoding="utf-8") as f:
            f.write(f"Timestamp: {timestamp}\n")
            f.write(f"Trial ID: {ct_number}\n")
            f.write(f"Error:\n{error_msg}\n")

        logger.warning(f"Quarantined trial payload for {ct_number} at {json_path}")
    except Exception as e:
        logger.error(f"Failed writing to quarantine for {ct_number}: {e}")

    return json_path


def generate_dashboard_html(report: Dict[str, Any], json_url: Optional[str] = None) -> str:
    """Renders a self-contained, responsive HTML dashboard for the pipeline status report."""
    metrics = report.get("overall_metrics", {})
    runs_by_type = report.get("runs_by_type", {})
    daily_activity = report.get("daily_activity", [])
    recent_runs = report.get("recent_pipeline_runs", [])
    recent_trials = report.get("recently_updated_trials", [])
    env = report.get("environment", {})

    target_json_url = json_url or "pipeline_status.json"
    json_pretty_str = json.dumps(report, indent=2, ensure_ascii=False)

    def _fmt_ts(val: Optional[str]) -> str:
        if not val:
            return "-"
        clean = val.replace("T", " ")
        if "." in clean:
            clean = clean.split(".")[0]
        if "+00:00" in clean:
            clean = clean.replace("+00:00", " UTC")
        return clean

    runs_by_type_rows = ""
    for rtype, rdata in runs_by_type.items():
        runs_by_type_rows += f"""
        <tr>
            <td><strong style="color: #ffffff; font-size: 0.95rem;">{rtype}</strong></td>
            <td style="color: #f8fafc; font-weight: 600;">{rdata.get('total_runs', 0)}</td>
            <td style="color: #f8fafc; font-weight: 600;">{rdata.get('trials_discovered', 0)}</td>
            <td style="color: #f8fafc; font-weight: 600;">{rdata.get('trials_processed', 0)}</td>
            <td style="color: #34d399; font-weight: 700; font-size: 1rem;">{rdata.get('trials_succeeded', 0)}</td>
            <td style="color: {'#f87171' if rdata.get('trials_failed', 0) > 0 else '#94a3b8'}; font-weight: 700;">{rdata.get('trials_failed', 0)}</td>
            <td style="font-size: 0.88rem; color: #cbd5e1; font-family: Consolas, monospace;">{_fmt_ts(rdata.get('last_run_at'))}</td>
        </tr>
        """

    daily_rows = ""
    for row in daily_activity:
        daily_rows += f"""
        <tr>
            <td style="color: #ffffff; font-weight: 600; font-size: 0.9rem;">{row.get('date')}</td>
            <td><span class="badge badge-info">{row.get('run_type')}</span></td>
            <td style="color: #f8fafc; font-weight: 600;">{row.get('runs_count')}</td>
            <td style="color: #f8fafc; font-weight: 600;">{row.get('trials_discovered')}</td>
            <td style="color: #f8fafc; font-weight: 600;">{row.get('trials_processed')}</td>
            <td style="color: #34d399; font-weight: 700;">{row.get('trials_succeeded')}</td>
            <td style="color: {'#f87171' if row.get('trials_failed', 0) > 0 else '#94a3b8'};">{row.get('trials_failed')}</td>
        </tr>
        """

    recent_runs_rows = ""
    for run in recent_runs:
        st = run.get("status", "UNKNOWN")
        st_badge = "badge-success" if st == "COMPLETED" else ("badge-warning" if st == "RUNNING" else "badge-danger")
        recent_runs_rows += f"""
        <tr>
            <td style="font-family: Consolas, monospace; font-size: 0.88rem; color: #38bdf8; font-weight: 600;">{run.get('run_id', '')[:8]}...</td>
            <td><strong style="color: #ffffff;">{run.get('run_type')}</strong></td>
            <td><span class="badge {st_badge}">{st}</span></td>
            <td style="color: #f8fafc; font-weight: 600;">{run.get('trials_discovered', 0)}</td>
            <td style="color: #f8fafc; font-weight: 600;">{run.get('trials_processed', 0)}</td>
            <td style="color: #34d399; font-weight: 700;">{run.get('trials_succeeded', 0)}</td>
            <td style="color: {'#f87171' if run.get('trials_failed', 0) > 0 else '#94a3b8'};">{run.get('trials_failed', 0)}</td>
            <td style="font-size: 0.85rem; color: #cbd5e1; font-family: Consolas, monospace;">{_fmt_ts(run.get('started_at'))}</td>
            <td style="font-size: 0.85rem; color: #cbd5e1; font-family: Consolas, monospace;">{_fmt_ts(run.get('completed_at'))}</td>
        </tr>
        """

    recent_trials_rows = ""
    for tr in recent_trials:
        tst = tr.get("status", "UNKNOWN")
        badge = "badge-success" if tst == "SUCCESS" else ("badge-warning" if "PENDING" in tst else "badge-danger")
        recent_trials_rows += f"""
        <tr>
            <td style="font-family: Consolas, monospace; font-weight: 700; color: #ffffff; font-size: 0.95rem; letter-spacing: 0.02em;">{tr.get('ct_number')}</td>
            <td><span class="badge {badge}">{tst}</span></td>
            <td style="font-size: 0.9rem; color: #e2e8f0; font-weight: 500; font-family: Consolas, monospace;">{_fmt_ts(tr.get('last_publish_date'))}</td>
            <td style="font-size: 0.9rem; color: #e2e8f0; font-weight: 500; font-family: Consolas, monospace;">{_fmt_ts(tr.get('last_fetched_at'))}</td>
            <td style="font-family: Consolas, monospace; font-size: 0.88rem; color: #38bdf8; font-weight: 600;">{tr.get('s3_silver_prefix', '-')}</td>
        </tr>
        """

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>EU CTIS Pipeline - Live Activity & Audit Dashboard</title>
    <style>
        :root {{
            --bg: #0b1120;
            --surface: #1e293b;
            --surface-hover: #334155;
            --border: #334155;
            --text-primary: #f8fafc;
            --text-secondary: #e2e8f0;
            --text-muted: #cbd5e1;
            --accent: #38bdf8;
            --success: #34d399;
            --warning: #fbbf24;
            --danger: #f87171;
        }}
        * {{ box-sizing: border-box; margin: 0; padding: 0; }}
        body {{
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
            background: var(--bg);
            color: var(--text-primary);
            padding: 24px;
            line-height: 1.5;
        }}
        .container {{ max-width: 1240px; margin: 0 auto; }}
        header {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            flex-wrap: wrap;
            gap: 16px;
            padding-bottom: 24px;
            border-bottom: 1px solid var(--border);
            margin-bottom: 24px;
        }}
        h1 {{ font-size: 1.6rem; font-weight: 700; color: #ffffff; letter-spacing: -0.01em; }}
        .subtitle {{ font-size: 0.9rem; color: var(--text-muted); margin-top: 4px; }}
        .meta-actions {{ display: flex; gap: 12px; align-items: center; flex-wrap: wrap; }}
        .btn {{
            display: inline-flex;
            align-items: center;
            padding: 9px 18px;
            border-radius: 6px;
            background: var(--accent);
            color: #0b1120;
            font-weight: 700;
            font-size: 0.88rem;
            text-decoration: none;
            transition: transform 0.15s, opacity 0.2s;
            cursor: pointer;
            border: none;
        }}
        .btn:hover {{ opacity: 0.92; transform: translateY(-1px); }}
        .grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
            gap: 16px;
            margin-bottom: 32px;
        }}
        .card {{
            background: var(--surface);
            border: 1px solid var(--border);
            border-radius: 10px;
            padding: 20px;
            box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.2);
        }}
        .card-label {{ font-size: 0.82rem; text-transform: uppercase; letter-spacing: 0.06em; color: #94a3b8; font-weight: 600; }}
        .card-value {{ font-size: 2.2rem; font-weight: 800; margin-top: 8px; color: #ffffff; }}
        .card-desc {{ font-size: 0.8rem; color: #94a3b8; margin-top: 4px; }}
        .section-header {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin: 36px 0 16px 0;
            flex-wrap: wrap;
            gap: 12px;
        }}
        .section-title {{ font-size: 1.2rem; font-weight: 700; color: #38bdf8; letter-spacing: -0.01em; margin: 0; }}
        .search-input {{
            background: #0f172a;
            border: 1px solid #475569;
            color: #ffffff;
            padding: 9px 16px;
            border-radius: 6px;
            font-size: 0.88rem;
            width: 320px;
            outline: none;
            transition: border-color 0.2s;
        }}
        .search-input:focus {{ border-color: var(--accent); }}
        .search-input::placeholder {{ color: #64748b; }}
        .table-container {{
            background: var(--surface);
            border: 1px solid var(--border);
            border-radius: 10px;
            overflow-x: auto;
            margin-bottom: 24px;
            box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.2);
        }}
        table {{
            width: 100%;
            border-collapse: collapse;
            text-align: left;
            font-size: 0.92rem;
        }}
        th, td {{
            padding: 14px 18px;
            border-bottom: 1px solid #334155;
        }}
        th {{
            background: #1e293b;
            color: #93c5fd;
            font-weight: 700;
            text-transform: uppercase;
            font-size: 0.78rem;
            letter-spacing: 0.06em;
            border-bottom: 2px solid #475569;
        }}
        tr:nth-child(even) td {{
            background: rgba(255, 255, 255, 0.02);
        }}
        tr:hover td {{
            background: rgba(56, 189, 248, 0.08);
        }}
        tr:last-child td {{ border-bottom: none; }}
        .badge {{
            display: inline-block;
            padding: 4px 10px;
            border-radius: 4px;
            font-size: 0.78rem;
            font-weight: 700;
            letter-spacing: 0.03em;
        }}
        .badge-success {{ background: rgba(16, 185, 129, 0.2); color: #34d399; border: 1px solid rgba(16, 185, 129, 0.4); }}
        .badge-warning {{ background: rgba(245, 158, 11, 0.2); color: #fbbf24; border: 1px solid rgba(245, 158, 11, 0.4); }}
        .badge-danger {{ background: rgba(239, 68, 68, 0.2); color: #f87171; border: 1px solid rgba(239, 68, 68, 0.4); }}
        .badge-info {{ background: rgba(56, 189, 248, 0.2); color: #38bdf8; border: 1px solid rgba(56, 189, 248, 0.4); }}
        footer {{
            text-align: center;
            font-size: 0.85rem;
            color: #94a3b8;
            margin-top: 48px;
            padding-top: 24px;
            border-top: 1px solid var(--border);
        }}
    </style>
</head>
<body>
    <div class="container">
        <header>
            <div>
                <h1>EU CTIS Clinical Trial Pipeline Dashboard</h1>
                <div class="subtitle">Live status, execution history, and trial synchronization audit report</div>
            </div>
            <div class="meta-actions">
                <span class="badge badge-success">ONLINE</span>
                <a href="{target_json_url}" class="btn" target="_blank">View Raw JSON API</a>
            </div>
        </header>

        <div class="grid">
            <div class="card">
                <div class="card-label">Total Trials Tracked</div>
                <div class="card-value">{metrics.get('total_trials_tracked', 0)}</div>
                <div class="card-desc">Saved across Bronze & Silver</div>
            </div>
            <div class="card">
                <div class="card-label">Successfully Ingested</div>
                <div class="card-value" style="color: #34d399;">{metrics.get('succeeded', 0)}</div>
                <div class="card-desc">S3 + DynamoDB verified</div>
            </div>
            <div class="card">
                <div class="card-label">Pending Ingestion</div>
                <div class="card-value" style="color: #fbbf24;">{metrics.get('pending', 0) + metrics.get('update_pending', 0)}</div>
                <div class="card-desc">Awaiting background sync</div>
            </div>
            <div class="card">
                <div class="card-label">Failed Ingestion</div>
                <div class="card-value" style="color: {'#f87171' if metrics.get('failed', 0) > 0 else '#94a3b8'};">{metrics.get('failed', 0)}</div>
                <div class="card-desc">Quarantined errors</div>
            </div>
        </div>

        <div class="section-header">
            <h2 class="section-title">Execution Totals by Run Mode (Historical / New / Updates)</h2>
        </div>
        <div class="table-container">
            <table>
                <thead>
                    <tr>
                        <th>Run Mode</th>
                        <th>Runs Count</th>
                        <th>Discovered</th>
                        <th>Processed</th>
                        <th>Succeeded</th>
                        <th>Failed</th>
                        <th>Last Executed</th>
                    </tr>
                </thead>
                <tbody>
                    {runs_by_type_rows}
                </tbody>
            </table>
        </div>

        <div class="section-header">
            <h2 class="section-title">Daily Activity & Sync Breakdown</h2>
        </div>
        <div class="table-container">
            <table>
                <thead>
                    <tr>
                        <th>Date</th>
                        <th>Mode</th>
                        <th>Runs Count</th>
                        <th>Discovered</th>
                        <th>Processed</th>
                        <th>Succeeded</th>
                        <th>Failed</th>
                    </tr>
                </thead>
                <tbody>
                    {daily_rows if daily_rows else '<tr><td colspan="7" style="text-align: center; color: var(--text-muted);">No daily activity recorded yet.</td></tr>'}
                </tbody>
            </table>
        </div>

        <div class="section-header">
            <h2 class="section-title">Recent Pipeline Runs</h2>
        </div>
        <div class="table-container">
            <table>
                <thead>
                    <tr>
                        <th>Run ID</th>
                        <th>Type</th>
                        <th>Status</th>
                        <th>Discovered</th>
                        <th>Processed</th>
                        <th>Succeeded</th>
                        <th>Failed</th>
                        <th>Started At</th>
                        <th>Completed At</th>
                    </tr>
                </thead>
                <tbody>
                    {recent_runs_rows if recent_runs_rows else '<tr><td colspan="9" style="text-align: center; color: var(--text-muted);">No runs recorded yet.</td></tr>'}
                </tbody>
            </table>
        </div>

        <div class="section-header">
            <h2 class="section-title">Recently Synchronized Clinical Trials</h2>
            <input type="text" id="trialSearch" class="search-input" placeholder="🔍 Search by CT Number or Date..." onkeyup="filterTrials()">
        </div>
        <div class="table-container">
            <table id="trialsTable">
                <thead>
                    <tr>
                        <th>Trial CT Number</th>
                        <th>Status</th>
                        <th>EU Registry Publish Date</th>
                        <th>Last Fetched At</th>
                        <th>S3 Silver Storage Prefix</th>
                    </tr>
                </thead>
                <tbody>
                    {recent_trials_rows if recent_trials_rows else '<tr><td colspan="5" style="text-align: center; color: var(--text-muted);">No trials synchronized yet.</td></tr>'}
                </tbody>
            </table>
        </div>

        <div class="section-header">
            <h2 class="section-title">Live JSON Payload Preview (Expandable)</h2>
        </div>
        <details class="table-container" style="padding: 18px; background: #020617;">
            <summary style="cursor: pointer; font-weight: 700; color: var(--accent); margin-bottom: 12px; font-size: 0.95rem;">Click to view full JSON payload preview</summary>
            <pre style="background: transparent; padding: 12px; border-radius: 6px; overflow-x: auto; font-size: 0.82rem; color: #f1f5f9; font-family: Consolas, monospace; line-height: 1.45;"><code>{json_pretty_str}</code></pre>
        </details>

        <footer>
            Report generated at {report.get('generated_at', 'UTC')} | S3: {env.get('s3_bucket')} | DynamoDB: {env.get('dynamodb_table')}
        </footer>
    </div>

    <script>
        function filterTrials() {{
            var input = document.getElementById("trialSearch").value.toUpperCase();
            var rows = document.querySelectorAll("#trialsTable tbody tr");
            rows.forEach(function(r) {{
                r.style.display = r.innerText.toUpperCase().indexOf(input) > -1 ? "" : "none";
            }});
        }}
    </script>
</body>
</html>"""
    return html


def publish_pipeline_status_report(
    report_data: Optional[Dict[str, Any]] = None,
    generate_html: bool = True,
    expires_in_seconds: int = 604800,
) -> Dict[str, Any]:
    """Generates, saves locally, uploads to AWS S3, and returns browser pre-signed URLs for pipeline status report.

    Args:
        report_data: Optional pre-generated report dict. If None, pulled from database.
        generate_html: Whether to also generate and upload an interactive HTML dashboard.
        expires_in_seconds: Expiration for browser pre-signed URL (default 7 days).

    Returns:
        Dictionary containing local paths, S3 keys, and browser pre-signed URLs.
    """
    from ctis_etl import database

    if report_data is None:
        report_data = database.generate_pipeline_summary_report()

    results: Dict[str, Any] = {
        "report_generated_at": report_data.get("generated_at"),
        "overall_metrics": report_data.get("overall_metrics"),
    }

    # 1. Local JSON Persistence
    local_json_path = config.DATA_DIR / "pipeline_status.json"
    _write_local_json(local_json_path, report_data)
    results["local_json_path"] = str(local_json_path)

    # 2. S3 Persistence for JSON (Upload first to get presigned URL for HTML button)
    json_presigned_url: Optional[str] = None
    s3 = get_s3_client()
    if s3 is not None and config.STORAGE_BACKEND in ("s3", "both"):
        try:
            s3_json_key = f"{config.S3_PREFIX}pipeline_status.json"
            json_bytes = json.dumps(report_data, indent=2, ensure_ascii=False).encode("utf-8")
            s3.put_object(
                Bucket=config.S3_BUCKET_NAME,
                Key=s3_json_key,
                Body=json_bytes,
                ContentType="application/json",
                CacheControl="no-cache, no-store, must-revalidate",
            )
            json_presigned_url = s3.generate_presigned_url(
                "get_object",
                Params={"Bucket": config.S3_BUCKET_NAME, "Key": s3_json_key},
                ExpiresIn=expires_in_seconds,
            )
            results["s3_json_key"] = s3_json_key
            results["json_url"] = f"https://{config.S3_BUCKET_NAME}.s3.amazonaws.com/{s3_json_key}"
            results["json_browser_url"] = json_presigned_url
        except Exception as e:
            logger.error(f"Failed uploading status JSON to S3: {e}")

    # 3. HTML Dashboard Persistence (Injecting the valid presigned JSON URL into the button)
    if generate_html:
        target_json_link = json_presigned_url or "pipeline_status.json"
        html_content = generate_dashboard_html(report_data, json_url=target_json_link)

        local_html_path = config.DATA_DIR / "pipeline_status.html"
        try:
            with open(local_html_path, "w", encoding="utf-8") as f:
                f.write(html_content)
            results["local_html_path"] = str(local_html_path)
        except Exception as e:
            logger.error(f"Failed saving local HTML dashboard: {e}")

        if s3 is not None and config.STORAGE_BACKEND in ("s3", "both"):
            try:
                s3_html_key = f"{config.S3_PREFIX}pipeline_status.html"
                s3.put_object(
                    Bucket=config.S3_BUCKET_NAME,
                    Key=s3_html_key,
                    Body=html_content.encode("utf-8"),
                    ContentType="text/html",
                    CacheControl="no-cache, no-store, must-revalidate",
                )
                html_presigned_url = s3.generate_presigned_url(
                    "get_object",
                    Params={"Bucket": config.S3_BUCKET_NAME, "Key": s3_html_key},
                    ExpiresIn=expires_in_seconds,
                )
                results["s3_html_key"] = s3_html_key
                results["html_url"] = f"https://{config.S3_BUCKET_NAME}.s3.amazonaws.com/{s3_html_key}"
                results["html_browser_url"] = html_presigned_url
            except Exception as e:
                logger.error(f"Failed uploading status HTML to S3: {e}")

    logger.info("Published live pipeline status report and dashboard.")
    return results



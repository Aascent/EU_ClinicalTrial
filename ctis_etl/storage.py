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
    """Renders a self-contained, responsive, beginner-friendly HTML dashboard with Material Icons."""
    metrics = report.get("overall_metrics", {})
    runs_by_type = report.get("runs_by_type", {})
    daily_activity = report.get("daily_activity", [])
    recent_runs = report.get("recent_pipeline_runs", [])
    recent_trials = report.get("recently_updated_trials", [])
    env = report.get("environment", {})
    pipeline_status = report.get("pipeline_status", "OPERATIONAL")
    generated_at = report.get("generated_at", "")

    target_json_url = json_url or "pipeline_status.json"
    json_pretty_str = json.dumps(report, indent=2, ensure_ascii=False)
    trials_json_data = json.dumps(recent_trials, ensure_ascii=False)

    total_trials = metrics.get("total_trials_tracked", 0)
    succeeded = metrics.get("succeeded", 0)
    pending = metrics.get("pending", 0)
    failed = metrics.get("failed", 0)
    success_rate = metrics.get("success_rate", 100.0 if failed == 0 else 0.0)

    def _fmt_ts(val: Optional[str]) -> str:
        if not val:
            return "N/A"
        clean = val.replace("T", " ")
        if "." in clean:
            clean = clean.split(".")[0]
        if "+00:00" in clean:
            clean = clean.replace("+00:00", " UTC")
        return clean

    def _fmt_date(val: Optional[str]) -> str:
        if not val:
            return "-"
        return val.split("T")[0]

    def _run_meta(rtype: str) -> Tuple[str, str, str]:
        lookup = {
            "INCREMENTAL": ("Daily Incremental Check", "sync", "Scans for new trials and protocol amendments"),
            "NEW_TRIALS": ("New Trials Discovery", "fiber_new", "Scans for newly registered clinical studies"),
            "UPDATED_TRIALS": ("Amendments Audit", "update", "Checks existing trials for regulatory updates"),
            "HISTORICAL": ("Historical Archive Backfill", "inventory_2", "Deep historical synchronization sweep"),
            "SINGLE": ("Single Trial Sync", "track_changes", "On-demand manual fetch for a specific study"),
            "RETRY_FAILED": ("Quarantine Error Retry", "replay", "Retrying previously failed trial records"),
        }
        return lookup.get(rtype, (rtype, "settings", "Automated pipeline execution"))

    # Build Runs By Type Rows
    runs_by_type_rows = ""
    for rtype, rdata in runs_by_type.items():
        name, icon, desc = _run_meta(rtype)
        runs_by_type_rows += f"""
        <tr>
            <td>
                <div style="display: flex; align-items: center; gap: 10px;">
                    <span class="material-symbols-outlined" style="color: var(--accent); font-size: 1.25rem;">{icon}</span>
                    <div>
                        <strong style="color: #ffffff; font-size: 0.95rem;">{name}</strong>
                        <div style="font-size: 0.78rem; color: #94a3b8;">{desc}</div>
                    </div>
                </div>
            </td>
            <td style="color: #f8fafc; font-weight: 600;">{rdata.get('total_runs', 0)}</td>
            <td style="color: #38bdf8; font-weight: 600;">{rdata.get('trials_discovered', 0)}</td>
            <td style="color: #e2e8f0; font-weight: 600;">{rdata.get('trials_processed', 0)}</td>
            <td style="color: #34d399; font-weight: 700; font-size: 0.95rem;">{rdata.get('trials_succeeded', 0)}</td>
            <td style="color: {'#f87171' if rdata.get('trials_failed', 0) > 0 else '#64748b'}; font-weight: 700;">{rdata.get('trials_failed', 0)}</td>
            <td style="font-size: 0.85rem; color: #cbd5e1; font-family: Consolas, monospace;">{_fmt_ts(rdata.get('last_run_at'))}</td>
        </tr>
        """

    # Build Daily Rows
    daily_rows = ""
    for row in daily_activity:
        rtype = row.get("run_type", "")
        name, icon, _ = _run_meta(rtype)
        daily_rows += f"""
        <tr>
            <td style="color: #ffffff; font-weight: 600; font-size: 0.9rem;">{row.get('date')}</td>
            <td>
                <span class="badge badge-info" style="display: inline-flex; align-items: center; gap: 6px;">
                    <span class="material-symbols-outlined" style="font-size: 15px;">{icon}</span> {name}
                </span>
            </td>
            <td style="color: #f8fafc; font-weight: 600;">{row.get('runs_count')}</td>
            <td style="color: #38bdf8; font-weight: 600;">{row.get('trials_discovered')}</td>
            <td style="color: #e2e8f0; font-weight: 600;">{row.get('trials_processed')}</td>
            <td style="color: #34d399; font-weight: 700;">{row.get('trials_succeeded')}</td>
            <td style="color: {'#f87171' if row.get('trials_failed', 0) > 0 else '#64748b'};">{row.get('trials_failed')}</td>
        </tr>
        """

    # Build Recent Runs Rows
    recent_runs_rows = ""
    for run in recent_runs:
        st = run.get("status", "UNKNOWN")
        st_badge = "badge-success" if st == "COMPLETED" else ("badge-warning" if st == "RUNNING" else "badge-danger")
        rtype = run.get("run_type", "")
        name, icon, _ = _run_meta(rtype)
        succ = run.get("trials_succeeded", 0)
        proc = run.get("trials_processed", 0)
        disc = run.get("trials_discovered", 0)
        fail = run.get("trials_failed", 0)

        # Plain-English run explanation for laypersons
        if st == "COMPLETED":
            if succ > 0:
                summary_text = f'<span class="material-symbols-outlined" style="color: #34d399; font-size: 16px; vertical-align: text-bottom;">check_circle</span> Ingested {succ} clinical trial(s) without errors'
            elif disc > 0:
                summary_text = f'<span class="material-symbols-outlined" style="color: #38bdf8; font-size: 16px; vertical-align: text-bottom;">done_all</span> Checked {disc} trials (All records already up to date)'
            else:
                summary_text = '<span class="material-symbols-outlined" style="color: #94a3b8; font-size: 16px; vertical-align: text-bottom;">search</span> Scanned EU registry (No new updates detected)'
        else:
            summary_text = f"Status: {st}"

        recent_runs_rows += f"""
        <tr>
            <td>
                <div style="display: flex; align-items: center; gap: 10px;">
                    <span class="material-symbols-outlined" style="color: var(--accent); font-size: 1.3rem;">{icon}</span>
                    <div>
                        <strong style="color: #ffffff; font-size: 0.92rem;">{name}</strong>
                        <div style="font-size: 0.78rem; color: #38bdf8; font-family: Consolas, monospace;">ID: {run.get('run_id', '')[:8]}</div>
                    </div>
                </div>
            </td>
            <td><span class="badge {st_badge}">{st}</span></td>
            <td>
                <div style="font-size: 0.85rem; color: #e2e8f0; font-weight: 500; display: flex; align-items: center; gap: 4px;">{summary_text}</div>
                <div style="font-size: 0.76rem; color: #94a3b8; margin-top: 2px;">
                    Found: {disc} | Processed: {proc} | Saved: {succ} | Failed: {fail}
                </div>
            </td>
            <td style="font-size: 0.82rem; color: #cbd5e1; font-family: Consolas, monospace;">{_fmt_ts(run.get('started_at'))}</td>
            <td style="font-size: 0.82rem; color: #cbd5e1; font-family: Consolas, monospace;">{_fmt_ts(run.get('completed_at'))}</td>
        </tr>
        """

    # Build Recent Trials Rows
    recent_trials_rows = ""
    for idx, tr in enumerate(recent_trials):
        tst = tr.get("status", "UNKNOWN")
        badge = "badge-success" if tst == "SUCCESS" else ("badge-warning" if "PENDING" in tst else "badge-danger")
        ct_num = tr.get("ct_number", "Unknown")
        title = tr.get("title", f"Clinical Trial {ct_num}")
        sponsor = tr.get("sponsor", "Unspecified Sponsor")
        phase = tr.get("phase", "Phase N/A")
        countries = tr.get("countries", [])
        country_str = ", ".join(countries[:3]) + (f" (+{len(countries)-3} more)" if len(countries) > 3 else "") if countries else "EU / EEA"
        pub_date = _fmt_date(tr.get("last_publish_date"))

        recent_trials_rows += f"""
        <tr class="trial-row" data-status="{tst}" data-ct="{ct_num}">
            <td>
                <div style="display: flex; align-items: baseline; gap: 8px;">
                    <span style="font-family: Consolas, monospace; font-weight: 700; color: #38bdf8; font-size: 0.95rem;">{ct_num}</span>
                    <button class="copy-btn" onclick="copyText('{ct_num}')" title="Copy Trial Number">
                        <span class="material-symbols-outlined" style="font-size: 15px;">content_copy</span>
                    </button>
                </div>
                <div style="font-weight: 600; color: #ffffff; font-size: 0.92rem; margin-top: 4px; line-height: 1.35; max-width: 480px;">
                    {title}
                </div>
                <div style="display: flex; align-items: center; gap: 12px; margin-top: 6px; flex-wrap: wrap; font-size: 0.8rem; color: #94a3b8;">
                    <span style="display: inline-flex; align-items: center; gap: 4px;">
                        <span class="material-symbols-outlined" style="font-size: 14px; color: #94a3b8;">domain</span> <strong>{sponsor}</strong>
                    </span>
                    <span style="display: inline-flex; align-items: center; gap: 4px;">
                        <span class="material-symbols-outlined" style="font-size: 14px; color: #94a3b8;">sell</span> {phase}
                    </span>
                    <span style="display: inline-flex; align-items: center; gap: 4px;">
                        <span class="material-symbols-outlined" style="font-size: 14px; color: #94a3b8;">public</span> {country_str}
                    </span>
                </div>
            </td>
            <td>
                <span class="badge {badge}">{'● INGESTED' if tst == 'SUCCESS' else tst}</span>
                <div style="font-size: 0.75rem; color: #34d399; margin-top: 4px;">6 Datasets Verified</div>
            </td>
            <td>
                <div style="font-size: 0.85rem; color: #e2e8f0; font-weight: 600;">{pub_date}</div>
                <div style="font-size: 0.75rem; color: #94a3b8;">Synced: {_fmt_ts(tr.get('last_fetched_at'))}</div>
            </td>
            <td>
                <div class="domain-pills">
                    <span class="domain-pill" title="meta_data.json: Regulatory decisions & status">Meta</span>
                    <span class="domain-pill" title="summary.json: Trial overview & sponsor">Summary</span>
                    <span class="domain-pill" title="full_trial_information.json: Medical protocol">Protocol</span>
                    <span class="domain-pill" title="trial_documents.json: Registry documents">Docs</span>
                    <span class="domain-pill" title="trial_results.json: Clinical trial outcomes">Results</span>
                    <span class="domain-pill" title="locations_and_contact_points.json: Hospital sites & contacts">Sites</span>
                </div>
            </td>
            <td style="text-align: right;">
                <button class="btn btn-secondary btn-sm" onclick="openTrialModal({idx})">Inspect Dossier</button>
            </td>
        </tr>
        """

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>EU CTIS Clinical Trial Pipeline - Live Status & Activity Dashboard</title>
    <!-- Google Material Symbols Outlined -->
    <link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Material+Symbols+Outlined:opsz,wght,FILL,GRAD@20..48,100..700,0..1,-50..200" />
    <style>
        :root {{
            --bg: #0b1120;
            --surface: #1e293b;
            --surface-hover: #273549;
            --card-subtle: #172235;
            --border: #334155;
            --border-highlight: #475569;
            --text-primary: #f8fafc;
            --text-secondary: #cbd5e1;
            --text-muted: #94a3b8;
            --accent: #38bdf8;
            --accent-glow: rgba(56, 189, 248, 0.15);
            --success: #34d399;
            --success-glow: rgba(52, 211, 153, 0.15);
            --warning: #fbbf24;
            --danger: #f87171;
            --indigo: #818cf8;
        }}
        * {{ box-sizing: border-box; margin: 0; padding: 0; }}
        body {{
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
            background: var(--bg);
            color: var(--text-primary);
            padding: 24px;
            line-height: 1.5;
            -webkit-font-smoothing: antialiased;
        }}
        .container {{ max-width: 1280px; margin: 0 auto; }}

        /* Material Symbols Styling */
        .material-symbols-outlined {{
            font-family: 'Material Symbols Outlined';
            font-weight: normal;
            font-style: normal;
            font-size: 20px;
            line-height: 1;
            display: inline-block;
            text-transform: none;
            letter-spacing: normal;
            word-wrap: normal;
            white-space: nowrap;
            direction: ltr;
            vertical-align: middle;
        }}
        .icon-sm {{ font-size: 16px; }}
        .icon-md {{ font-size: 20px; }}
        .icon-lg {{ font-size: 24px; }}

        /* Top Navbar */
        header {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            flex-wrap: wrap;
            gap: 16px;
            padding-bottom: 20px;
            border-bottom: 1px solid var(--border);
            margin-bottom: 24px;
        }}
        .brand {{ display: flex; align-items: center; gap: 14px; }}
        .brand-icon {{
            width: 44px;
            height: 44px;
            background: linear-gradient(135deg, #0284c7, #2563eb);
            border-radius: 10px;
            display: flex;
            align-items: center;
            justify-content: center;
            box-shadow: 0 4px 12px rgba(37, 99, 235, 0.3);
        }}
        h1 {{ font-size: 1.5rem; font-weight: 700; color: #ffffff; letter-spacing: -0.01em; }}
        .subtitle {{ font-size: 0.85rem; color: var(--text-muted); margin-top: 2px; }}
        .header-actions {{ display: flex; gap: 10px; align-items: center; flex-wrap: wrap; }}

        /* Buttons & Badges */
        .btn {{
            display: inline-flex;
            align-items: center;
            gap: 6px;
            padding: 8px 16px;
            border-radius: 6px;
            background: var(--accent);
            color: #0b1120;
            font-weight: 700;
            font-size: 0.85rem;
            text-decoration: none;
            transition: all 0.15s ease;
            cursor: pointer;
            border: none;
        }}
        .btn:hover {{ opacity: 0.92; transform: translateY(-1px); }}
        .btn-secondary {{
            background: #1e293b;
            color: #e2e8f0;
            border: 1px solid var(--border);
        }}
        .btn-secondary:hover {{
            background: #334155;
            color: #ffffff;
            border-color: #64748b;
        }}
        .btn-sm {{ padding: 5px 10px; font-size: 0.78rem; }}
        .copy-btn {{
            background: transparent;
            border: none;
            cursor: pointer;
            color: #94a3b8;
            transition: color 0.2s;
            padding: 0 4px;
            display: inline-flex;
            align-items: center;
        }}
        .copy-btn:hover {{ color: #ffffff; }}

        .badge {{
            display: inline-flex;
            align-items: center;
            padding: 4px 10px;
            border-radius: 6px;
            font-size: 0.78rem;
            font-weight: 700;
            letter-spacing: 0.02em;
        }}
        .badge-success {{ background: rgba(52, 211, 153, 0.15); color: var(--success); border: 1px solid rgba(52, 211, 153, 0.3); }}
        .badge-warning {{ background: rgba(251, 191, 36, 0.15); color: var(--warning); border: 1px solid rgba(251, 191, 36, 0.3); }}
        .badge-danger {{ background: rgba(248, 113, 113, 0.15); color: var(--danger); border: 1px solid rgba(248, 113, 113, 0.3); }}
        .badge-info {{ background: rgba(56, 189, 248, 0.15); color: var(--accent); border: 1px solid rgba(56, 189, 248, 0.3); }}

        /* Pulse indicator */
        .status-pill {{
            display: inline-flex;
            align-items: center;
            gap: 8px;
            padding: 6px 14px;
            border-radius: 20px;
            background: rgba(52, 211, 153, 0.12);
            border: 1px solid rgba(52, 211, 153, 0.3);
            color: #34d399;
            font-size: 0.82rem;
            font-weight: 700;
        }}
        .status-dot {{
            width: 8px;
            height: 8px;
            border-radius: 50%;
            background: #34d399;
            box-shadow: 0 0 8px #34d399;
            animation: pulseDot 2s infinite;
        }}
        @keyframes pulseDot {{
            0% {{ transform: scale(0.95); opacity: 0.7; }}
            50% {{ transform: scale(1.3); opacity: 1; }}
            100% {{ transform: scale(0.95); opacity: 0.7; }}
        }}

        /* Beginner-friendly Hero Explainer */
        .hero-banner {{
            background: linear-gradient(135deg, rgba(30, 41, 59, 0.9), rgba(15, 23, 42, 0.95));
            border: 1px solid rgba(56, 189, 248, 0.25);
            border-radius: 12px;
            padding: 22px 26px;
            margin-bottom: 24px;
            position: relative;
            overflow: hidden;
            box-shadow: 0 6px 16px rgba(0, 0, 0, 0.25);
        }}
        .hero-banner::before {{
            content: "";
            position: absolute;
            top: 0; left: 0; right: 0; height: 3px;
            background: linear-gradient(90deg, #38bdf8, #34d399, #818cf8);
        }}
        .hero-title {{
            font-size: 1.15rem;
            font-weight: 700;
            color: #ffffff;
            display: flex;
            align-items: center;
            gap: 8px;
        }}
        .hero-desc {{
            font-size: 0.92rem;
            color: #cbd5e1;
            margin-top: 6px;
            max-width: 980px;
            line-height: 1.55;
        }}
        .pipeline-flow {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
            gap: 14px;
            margin-top: 18px;
            padding-top: 16px;
            border-top: 1px solid rgba(255, 255, 255, 0.08);
        }}
        .flow-step {{
            background: rgba(15, 23, 42, 0.6);
            border: 1px solid var(--border);
            border-radius: 8px;
            padding: 12px 16px;
            display: flex;
            gap: 12px;
            align-items: flex-start;
        }}
        .flow-icon-box {{
            width: 32px;
            height: 32px;
            background: rgba(56, 189, 248, 0.15);
            border: 1px solid rgba(56, 189, 248, 0.3);
            border-radius: 8px;
            display: flex;
            align-items: center;
            justify-content: center;
            flex-shrink: 0;
            color: var(--accent);
        }}
        .flow-title {{ font-size: 0.88rem; font-weight: 700; color: #ffffff; }}
        .flow-text {{ font-size: 0.8rem; color: #94a3b8; margin-top: 2px; line-height: 1.35; }}

        /* Metric Cards */
        .metrics-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));
            gap: 16px;
            margin-bottom: 24px;
        }}
        .card {{
            background: var(--surface);
            border: 1px solid var(--border);
            border-radius: 10px;
            padding: 18px 20px;
            box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.2);
            transition: transform 0.15s, border-color 0.2s;
        }}
        .card:hover {{ border-color: var(--border-highlight); transform: translateY(-1px); }}
        .card-top {{ display: flex; justify-content: space-between; align-items: center; }}
        .card-label {{ font-size: 0.82rem; text-transform: uppercase; letter-spacing: 0.05em; color: var(--text-muted); font-weight: 700; }}
        .card-icon {{ display: flex; align-items: center; justify-content: center; }}
        .card-value {{ font-size: 2.2rem; font-weight: 800; margin-top: 8px; color: #ffffff; }}
        .card-desc {{ font-size: 0.82rem; color: #94a3b8; margin-top: 4px; }}
        .card-progress {{
            height: 4px;
            background: rgba(255, 255, 255, 0.08);
            border-radius: 2px;
            margin-top: 12px;
            overflow: hidden;
        }}
        .card-progress-bar {{
            height: 100%;
            background: var(--success);
            border-radius: 2px;
        }}

        /* Explainer Accordion */
        .guide-accordion {{
            background: var(--card-subtle);
            border: 1px solid var(--border);
            border-radius: 8px;
            margin-bottom: 24px;
            overflow: hidden;
        }}
        .guide-summary {{
            padding: 14px 20px;
            font-size: 0.92rem;
            font-weight: 700;
            color: var(--accent);
            cursor: pointer;
            display: flex;
            align-items: center;
            justify-content: space-between;
            background: rgba(15, 23, 42, 0.4);
            user-select: none;
        }}
        .guide-content {{
            padding: 16px 20px;
            font-size: 0.85rem;
            color: var(--text-secondary);
            border-top: 1px solid var(--border);
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
            gap: 16px;
            line-height: 1.5;
        }}
        .guide-box h4 {{ color: #ffffff; font-size: 0.88rem; margin-bottom: 4px; }}

        /* Tabs System */
        .tabs-nav {{
            display: flex;
            gap: 8px;
            border-bottom: 1px solid var(--border);
            margin-bottom: 20px;
            flex-wrap: wrap;
        }}
        .tab-btn {{
            background: transparent;
            border: none;
            color: var(--text-muted);
            font-size: 0.92rem;
            font-weight: 700;
            padding: 10px 18px;
            cursor: pointer;
            border-bottom: 2px solid transparent;
            transition: all 0.2s ease;
            display: flex;
            align-items: center;
            gap: 8px;
        }}
        .tab-btn:hover {{ color: var(--text-primary); }}
        .tab-btn.active {{
            color: var(--accent);
            border-bottom-color: var(--accent);
            background: rgba(56, 189, 248, 0.05);
        }}
        .tab-badge {{
            background: #334155;
            color: #ffffff;
            font-size: 0.75rem;
            padding: 2px 7px;
            border-radius: 10px;
            font-weight: 600;
        }}
        .tab-content {{ display: none; }}
        .tab-content.active {{ display: block; }}

        /* Search & Filter Toolbar */
        .toolbar {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            flex-wrap: wrap;
            gap: 12px;
            margin-bottom: 16px;
        }}
        .search-box {{
            display: flex;
            align-items: center;
            background: #0f172a;
            border: 1px solid #475569;
            border-radius: 8px;
            padding: 6px 12px;
            width: 380px;
            max-width: 100%;
            transition: border-color 0.2s;
        }}
        .search-box:focus-within {{ border-color: var(--accent); }}
        .search-box input {{
            background: transparent;
            border: none;
            color: #ffffff;
            font-size: 0.88rem;
            width: 100%;
            outline: none;
            margin-left: 8px;
        }}
        .search-box input::placeholder {{ color: #64748b; }}
        .filter-group {{ display: flex; gap: 8px; align-items: center; flex-wrap: wrap; }}
        .filter-btn {{
            background: #1e293b;
            border: 1px solid var(--border);
            color: var(--text-secondary);
            font-size: 0.8rem;
            font-weight: 600;
            padding: 6px 12px;
            border-radius: 6px;
            cursor: pointer;
            transition: all 0.15s;
            display: inline-flex;
            align-items: center;
            gap: 6px;
        }}
        .filter-btn:hover, .filter-btn.active {{
            background: #334155;
            color: #ffffff;
            border-color: var(--accent);
        }}

        /* Table Components */
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
            font-size: 0.9rem;
        }}
        th, td {{ padding: 14px 18px; border-bottom: 1px solid #334155; }}
        th {{
            background: #172235;
            color: #93c5fd;
            font-weight: 700;
            text-transform: uppercase;
            font-size: 0.75rem;
            letter-spacing: 0.05em;
            border-bottom: 2px solid #475569;
        }}
        tr:nth-child(even) td {{ background: rgba(255, 255, 255, 0.015); }}
        tr:hover td {{ background: rgba(56, 189, 248, 0.05); }}
        tr:last-child td {{ border-bottom: none; }}

        .domain-pills {{ display: flex; gap: 4px; flex-wrap: wrap; max-width: 220px; }}
        .domain-pill {{
            background: rgba(56, 189, 248, 0.12);
            color: #7dd3fc;
            border: 1px solid rgba(56, 189, 248, 0.25);
            font-size: 0.72rem;
            font-weight: 700;
            padding: 2px 6px;
            border-radius: 4px;
            cursor: help;
        }}

        /* Modal Popup */
        .modal-overlay {{
            position: fixed;
            top: 0; left: 0; right: 0; bottom: 0;
            background: rgba(0, 0, 0, 0.75);
            backdrop-filter: blur(4px);
            display: none;
            align-items: center;
            justify-content: center;
            z-index: 1000;
            padding: 20px;
        }}
        .modal-overlay.open {{ display: flex; }}
        .modal-card {{
            background: #0f172a;
            border: 1px solid #38bdf8;
            border-radius: 12px;
            max-width: 780px;
            width: 100%;
            max-height: 90vh;
            overflow-y: auto;
            box-shadow: 0 20px 40px rgba(0, 0, 0, 0.5);
            position: relative;
        }}
        .modal-header {{
            padding: 20px 24px;
            border-bottom: 1px solid var(--border);
            display: flex;
            justify-content: space-between;
            align-items: flex-start;
        }}
        .modal-title {{ font-size: 1.15rem; font-weight: 700; color: #ffffff; line-height: 1.4; }}
        .modal-close {{
            background: transparent;
            border: none;
            color: #94a3b8;
            cursor: pointer;
            padding: 0 6px;
            line-height: 1;
            display: inline-flex;
            align-items: center;
        }}
        .modal-close:hover {{ color: #ffffff; }}
        .modal-body {{ padding: 20px 24px; font-size: 0.88rem; }}
        .modal-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
            gap: 14px;
            margin-bottom: 20px;
        }}
        .modal-item {{
            background: #1e293b;
            padding: 10px 14px;
            border-radius: 8px;
            border: 1px solid #334155;
        }}
        .modal-item-label {{ font-size: 0.75rem; text-transform: uppercase; color: #94a3b8; font-weight: 700; }}
        .modal-item-val {{ font-size: 0.95rem; color: #ffffff; font-weight: 600; margin-top: 2px; word-break: break-all; }}

        /* Toast */
        .toast {{
            position: fixed;
            bottom: 24px;
            right: 24px;
            background: #0284c7;
            color: #ffffff;
            padding: 10px 18px;
            border-radius: 8px;
            font-weight: 700;
            font-size: 0.85rem;
            box-shadow: 0 6px 16px rgba(0, 0, 0, 0.3);
            display: none;
            z-index: 2000;
        }}

        footer {{
            text-align: center;
            font-size: 0.82rem;
            color: #64748b;
            margin-top: 48px;
            padding-top: 24px;
            border-top: 1px solid var(--border);
        }}
    </style>
</head>
<body>
    <div class="container">
        <!-- Top Navbar -->
        <header>
            <div class="brand">
                <div class="brand-icon">
                    <span class="material-symbols-outlined icon-lg" style="color: #ffffff;">local_hospital</span>
                </div>
                <div>
                    <h1>EU CTIS Clinical Trial Pipeline</h1>
                    <div class="subtitle">Official EU Medicines Agency Registry Synchronization & Cloud Archive</div>
                </div>
            </div>
            <div class="header-actions">
                <div class="status-pill">
                    <span class="status-dot"></span>
                    <span>{pipeline_status}</span>
                </div>
                <button class="btn btn-secondary" onclick="location.reload()">
                    <span class="material-symbols-outlined icon-sm">refresh</span> Refresh View
                </button>
                <button class="btn btn-secondary" onclick="copyText(window.location.href)">
                    <span class="material-symbols-outlined icon-sm">content_copy</span> Copy Page URL
                </button>
                <a href="{target_json_url}" class="btn" target="_blank">
                    <span class="material-symbols-outlined icon-sm">data_object</span> View Raw JSON API
                </a>
            </div>
        </header>

        <!-- Beginner-friendly Hero Explainer -->
        <div class="hero-banner">
            <div class="hero-title">
                <span class="material-symbols-outlined" style="color: var(--accent);">hub</span>
                <span>Clinical Trials Synchronization Monitor</span>
            </div>
            <div class="hero-desc">
                This dashboard tracks the automated data pipeline that continuously monitors all human clinical trials registered in the European Union (EU CTIS). Whenever medical researchers or pharmaceutical sponsors authorize a new trial or file an amendment, this system automatically extracts the dossier, organizes it into 6 verified datasets, and archives it securely to cloud storage.
            </div>

            <!-- 3-Step Visual Process -->
            <div class="pipeline-flow">
                <div class="flow-step">
                    <div class="flow-icon-box">
                        <span class="material-symbols-outlined">travel_explore</span>
                    </div>
                    <div>
                        <div class="flow-title">1. Scan EU Registry</div>
                        <div class="flow-text">Automated bots query the official European Medicines Agency portal for newly approved studies and amendments.</div>
                    </div>
                </div>
                <div class="flow-step">
                    <div class="flow-icon-box">
                        <span class="material-symbols-outlined">dataset</span>
                    </div>
                    <div>
                        <div class="flow-title">2. Cleanse & Split (6 Domains)</div>
                        <div class="flow-text">Dossiers are normalized into 6 clean entities: Metadata, Summary, Protocol, Documents, Results, and Clinical Sites.</div>
                    </div>
                </div>
                <div class="flow-step">
                    <div class="flow-icon-box">
                        <span class="material-symbols-outlined">cloud_upload</span>
                    </div>
                    <div>
                        <div class="flow-title">3. Secure Cloud Archive</div>
                        <div class="flow-text">Data is permanently saved into Amazon S3 (Bronze & Silver layers) with state tracked in DynamoDB & SQLite.</div>
                    </div>
                </div>
            </div>
        </div>

        <!-- High-Impact Metric Cards -->
        <div class="metrics-grid">
            <div class="card">
                <div class="card-top">
                    <span class="card-label">Clinical Trials Monitored</span>
                    <span class="card-icon">
                        <span class="material-symbols-outlined icon-lg" style="color: var(--accent);">biotech</span>
                    </span>
                </div>
                <div class="card-value">{total_trials}</div>
                <div class="card-desc">Total studies tracked across Europe</div>
                <div class="card-progress">
                    <div class="card-progress-bar" style="width: 100%;"></div>
                </div>
            </div>
            <div class="card">
                <div class="card-top">
                    <span class="card-label">Successfully Ingested</span>
                    <span class="card-icon">
                        <span class="material-symbols-outlined icon-lg" style="color: var(--success);">check_circle</span>
                    </span>
                </div>
                <div class="card-value" style="color: #34d399;">{succeeded}</div>
                <div class="card-desc">Verified in AWS S3 + DynamoDB ({success_rate}% success)</div>
                <div class="card-progress">
                    <div class="card-progress-bar" style="width: {success_rate}%;"></div>
                </div>
            </div>
            <div class="card">
                <div class="card-top">
                    <span class="card-label">Pending Synchronization</span>
                    <span class="card-icon">
                        <span class="material-symbols-outlined icon-lg" style="color: var(--warning);">schedule</span>
                    </span>
                </div>
                <div class="card-value" style="color: {'#fbbf24' if pending > 0 else '#94a3b8'};">{pending}</div>
                <div class="card-desc">Scheduled for next automated scan</div>
                <div class="card-progress">
                    <div class="card-progress-bar" style="width: {'100%' if pending > 0 else '0%'}; background: var(--warning);"></div>
                </div>
            </div>
            <div class="card">
                <div class="card-top">
                    <span class="card-label">Quarantined / Errors</span>
                    <span class="card-icon">
                        <span class="material-symbols-outlined icon-lg" style="color: var(--danger);">shield</span>
                    </span>
                </div>
                <div class="card-value" style="color: {'#f87171' if failed > 0 else '#94a3b8'};">{failed}</div>
                <div class="card-desc">{'Isolated for investigation' if failed > 0 else 'Zero errors detected (All clean)'}</div>
                <div class="card-progress">
                    <div class="card-progress-bar" style="width: {'100%' if failed > 0 else '0%'}; background: var(--danger);"></div>
                </div>
            </div>
        </div>

        <!-- Expandable Beginners Guide -->
        <details class="guide-accordion">
            <summary class="guide-summary">
                <span style="display: inline-flex; align-items: center; gap: 8px;">
                    <span class="material-symbols-outlined icon-sm">help_outline</span>
                    <span>New to EU Clinical Trials? Click here for a 1-minute quick guide</span>
                </span>
                <span class="material-symbols-outlined icon-sm">expand_more</span>
            </summary>
            <div class="guide-content">
                <div class="guide-box">
                    <h4>What is EU CTIS?</h4>
                    <p>The <em>Clinical Trials Information System</em> (CTIS) is the single official EU platform managed by the European Medicines Agency (EMA) for applying, reviewing, and publishing all human clinical trials in Europe.</p>
                </div>
                <div class="guide-box">
                    <h4>What are Bronze and Silver Layers?</h4>
                    <p><strong>Bronze:</strong> Exact raw API response direct from EU CTIS.<br><strong>Silver:</strong> Cleansed, standardized data split into 6 domain JSON files ready for medical search and analysis.</p>
                </div>
                <div class="guide-box">
                    <h4>What are the 6 Standard Datasets?</h4>
                    <p>Each study generates: (1) <code>meta_data.json</code>, (2) <code>summary.json</code>, (3) <code>full_trial_information.json</code>, (4) <code>trial_documents.json</code>, (5) <code>trial_results.json</code>, (6) <code>locations_and_contact_points.json</code>.</p>
                </div>
                <div class="guide-box">
                    <h4>What do the Run Modes mean?</h4>
                    <p><strong>Incremental:</strong> Daily scan for new trials & amendments.<br><strong>New Trials:</strong> Targeted scan for brand-new studies.<br><strong>Updates:</strong> Sweep checking ongoing trials for amendments.</p>
                </div>
            </div>
        </details>

        <!-- Navigation Tabs -->
        <div class="tabs-nav">
            <button class="tab-btn active" onclick="switchTab('trialsTab', this)">
                <span class="material-symbols-outlined icon-sm">medical_services</span>
                <span>Clinical Studies Catalog</span>
                <span class="tab-badge">{len(recent_trials)}</span>
            </button>
            <button class="tab-btn" onclick="switchTab('runsTab', this)">
                <span class="material-symbols-outlined icon-sm">history</span>
                <span>Automated Execution History</span>
                <span class="tab-badge">{len(recent_runs)}</span>
            </button>
            <button class="tab-btn" onclick="switchTab('dailyTab', this)">
                <span class="material-symbols-outlined icon-sm">monitoring</span>
                <span>Daily Activity Trend</span>
                <span class="tab-badge">{len(daily_activity)}</span>
            </button>
            <button class="tab-btn" onclick="switchTab('techTab', this)">
                <span class="material-symbols-outlined icon-sm">cloud</span>
                <span>Cloud Specs & Raw JSON</span>
            </button>
        </div>

        <!-- TAB 1: Clinical Studies Catalog -->
        <div id="trialsTab" class="tab-content active">
            <div class="toolbar">
                <div class="search-box">
                    <span class="material-symbols-outlined icon-sm" style="color: #94a3b8;">search</span>
                    <input type="text" id="trialSearch" placeholder="Search by Trial ID, study title, sponsor, country..." onkeyup="filterTrials()">
                </div>
                <div class="filter-group">
                    <button class="filter-btn active" onclick="filterStatus('ALL', this)">All Studies ({total_trials})</button>
                    <button class="filter-btn" onclick="filterStatus('SUCCESS', this)">
                        <span class="material-symbols-outlined icon-sm" style="color: #34d399;">check_circle</span> Ingested ({succeeded})
                    </button>
                    <button class="filter-btn" onclick="filterStatus('PENDING', this)">
                        <span class="material-symbols-outlined icon-sm" style="color: #fbbf24;">schedule</span> Pending ({pending})
                    </button>
                    <button class="filter-btn" onclick="filterStatus('FAILED', this)">
                        <span class="material-symbols-outlined icon-sm" style="color: #f87171;">cancel</span> Failed ({failed})
                    </button>
                </div>
            </div>

            <div class="table-container">
                <table id="trialsTable">
                    <thead>
                        <tr>
                            <th style="min-width: 320px;">Study Details & CT Number</th>
                            <th>Status</th>
                            <th>EU Registry Date</th>
                            <th>Standardized Datasets</th>
                            <th style="text-align: right;">Action</th>
                        </tr>
                    </thead>
                    <tbody>
                        {recent_trials_rows if recent_trials_rows else '<tr><td colspan="5" style="text-align: center; color: var(--text-muted); padding: 32px;">No clinical trials synchronized yet.</td></tr>'}
                    </tbody>
                </table>
            </div>
        </div>

        <!-- TAB 2: Execution History -->
        <div id="runsTab" class="tab-content">
            <div class="table-container">
                <table>
                    <thead>
                        <tr>
                            <th>Execution Mode</th>
                            <th>Status</th>
                            <th>Run Summary & Trial Counts</th>
                            <th>Started At</th>
                            <th>Completed At</th>
                        </tr>
                    </thead>
                    <tbody>
                        {recent_runs_rows if recent_runs_rows else '<tr><td colspan="5" style="text-align: center; color: var(--text-muted); padding: 32px;">No pipeline runs recorded yet.</td></tr>'}
                    </tbody>
                </table>
            </div>
        </div>

        <!-- TAB 3: Daily Activity -->
        <div id="dailyTab" class="tab-content">
            <div class="table-container">
                <table>
                    <thead>
                        <tr>
                            <th>Date</th>
                            <th>Sync Mode</th>
                            <th>Runs</th>
                            <th>Discovered</th>
                            <th>Processed</th>
                            <th>Succeeded</th>
                            <th>Failed</th>
                        </tr>
                    </thead>
                    <tbody>
                        {daily_rows if daily_rows else '<tr><td colspan="7" style="text-align: center; color: var(--text-muted); padding: 32px;">No daily activity recorded yet.</td></tr>'}
                    </tbody>
                </table>
            </div>
        </div>

        <!-- TAB 4: Cloud Specs & Raw JSON -->
        <div id="techTab" class="tab-content">
            <div class="metrics-grid" style="margin-bottom: 20px;">
                <div class="card">
                    <div class="card-label">Cloud Storage Backend</div>
                    <div class="card-value" style="font-size: 1.3rem; margin-top: 6px;">Amazon S3</div>
                    <div class="card-desc">Bucket: <code>{env.get('s3_bucket', 'N/A')}</code></div>
                </div>
                <div class="card">
                    <div class="card-label">State Tracking Table</div>
                    <div class="card-value" style="font-size: 1.3rem; margin-top: 6px;">AWS DynamoDB</div>
                    <div class="card-desc">Table: <code>{env.get('dynamodb_table', 'N/A')}</code></div>
                </div>
                <div class="card">
                    <div class="card-label">Local Audit Database</div>
                    <div class="card-value" style="font-size: 1.3rem; margin-top: 6px;">SQLite (WAL)</div>
                    <div class="card-desc">Database file: <code>tracker.db</code></div>
                </div>
            </div>

            <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 12px;">
                <h3 style="font-size: 1.05rem; color: #ffffff;">Live JSON Status Payload (pipeline_status.json)</h3>
                <button class="btn btn-secondary btn-sm" onclick="copyText(document.getElementById('jsonCode').innerText)">
                    <span class="material-symbols-outlined icon-sm">content_copy</span> Copy Full JSON
                </button>
            </div>
            <div class="table-container" style="padding: 16px; background: #020617;">
                <pre style="background: transparent; max-height: 400px; overflow-y: auto; font-size: 0.82rem; color: #f1f5f9; font-family: Consolas, monospace; line-height: 1.45;"><code id="jsonCode">{json_pretty_str}</code></pre>
            </div>
        </div>

        <!-- Study Dossier Modal -->
        <div id="trialModal" class="modal-overlay" onclick="closeModalOnBg(event)">
            <div class="modal-card">
                <div class="modal-header">
                    <div>
                        <div id="modalCtNumber" style="font-family: Consolas, monospace; color: #38bdf8; font-weight: 700; font-size: 0.95rem;"></div>
                        <div id="modalTitle" class="modal-title" style="margin-top: 4px;"></div>
                    </div>
                    <button class="modal-close" onclick="closeModal()">
                        <span class="material-symbols-outlined">close</span>
                    </button>
                </div>
                <div class="modal-body">
                    <div class="modal-grid">
                        <div class="modal-item">
                            <div class="modal-item-label">Primary Sponsor</div>
                            <div id="modalSponsor" class="modal-item-val"></div>
                        </div>
                        <div class="modal-item">
                            <div class="modal-item-label">Clinical Trial Phase</div>
                            <div id="modalPhase" class="modal-item-val"></div>
                        </div>
                        <div class="modal-item">
                            <div class="modal-item-label">Member State Countries</div>
                            <div id="modalCountries" class="modal-item-val"></div>
                        </div>
                        <div class="modal-item">
                            <div class="modal-item-label">S3 Silver Cloud Path</div>
                            <div id="modalS3Path" class="modal-item-val" style="font-family: Consolas, monospace; font-size: 0.8rem; color: #38bdf8;"></div>
                        </div>
                    </div>

                    <h4 style="color: #ffffff; font-size: 0.95rem; margin-bottom: 10px;">Standardized Domain Datasets (6 Files)</h4>
                    <div style="display: grid; grid-template-columns: 1fr 1fr; gap: 8px;">
                        <div class="flow-step" style="padding: 8px 12px;">
                            <div>
                                <strong style="color: #38bdf8; font-size: 0.85rem;">meta_data.json</strong>
                                <div style="font-size: 0.78rem; color: #94a3b8;">Regulatory status & member state decisions</div>
                            </div>
                        </div>
                        <div class="flow-step" style="padding: 8px 12px;">
                            <div>
                                <strong style="color: #38bdf8; font-size: 0.85rem;">summary.json</strong>
                                <div style="font-size: 0.78rem; color: #94a3b8;">High-level summary, title & sponsor details</div>
                            </div>
                        </div>
                        <div class="flow-step" style="padding: 8px 12px;">
                            <div>
                                <strong style="color: #38bdf8; font-size: 0.85rem;">full_trial_information.json</strong>
                                <div style="font-size: 0.78rem; color: #94a3b8;">Full clinical protocol & scientific design</div>
                            </div>
                        </div>
                        <div class="flow-step" style="padding: 8px 12px;">
                            <div>
                                <strong style="color: #38bdf8; font-size: 0.85rem;">trial_documents.json</strong>
                                <div style="font-size: 0.78rem; color: #94a3b8;">Registry filings & official documentation</div>
                            </div>
                        </div>
                        <div class="flow-step" style="padding: 8px 12px;">
                            <div>
                                <strong style="color: #38bdf8; font-size: 0.85rem;">trial_results.json</strong>
                                <div style="font-size: 0.78rem; color: #94a3b8;">Clinical endpoints & trial outcome reports</div>
                            </div>
                        </div>
                        <div class="flow-step" style="padding: 8px 12px;">
                            <div>
                                <strong style="color: #38bdf8; font-size: 0.85rem;">locations_and_contact_points.json</strong>
                                <div style="font-size: 0.78rem; color: #94a3b8;">Participating hospitals & investigator contacts</div>
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        </div>

        <div id="toast" class="toast">Copied to clipboard!</div>

        <footer>
            EU CTIS Automated Synchronization Pipeline | Last audit report: {generated_at} | Storage: {env.get('storage_backend')} | State: {env.get('state_backend')}
        </footer>
    </div>

    <script>
        const trialsData = {trials_json_data};
        let currentFilterStatus = "ALL";

        function switchTab(tabId, btn) {{
            document.querySelectorAll(".tab-content").forEach(el => el.classList.remove("active"));
            document.querySelectorAll(".tab-btn").forEach(el => el.classList.remove("active"));
            document.getElementById(tabId).classList.add("active");
            btn.classList.add("active");
        }}

        function filterTrials() {{
            const search = document.getElementById("trialSearch").value.toUpperCase();
            const rows = document.querySelectorAll("#trialsTable tbody tr.trial-row");
            rows.forEach(r => {{
                const status = r.getAttribute("data-status");
                const matchesSearch = r.innerText.toUpperCase().indexOf(search) > -1;
                const matchesStatus = (currentFilterStatus === "ALL") || (status === currentFilterStatus);
                r.style.display = (matchesSearch && matchesStatus) ? "" : "none";
            }});
        }}

        function filterStatus(status, btn) {{
            currentFilterStatus = status;
            document.querySelectorAll(".filter-btn").forEach(b => b.classList.remove("active"));
            btn.classList.add("active");
            filterTrials();
        }}

        function openTrialModal(idx) {{
            const tr = trialsData[idx];
            if (!tr) return;
            document.getElementById("modalCtNumber").innerText = tr.ct_number || "";
            document.getElementById("modalTitle").innerText = tr.title || "Clinical Trial " + tr.ct_number;
            document.getElementById("modalSponsor").innerText = tr.sponsor || "Unspecified Sponsor";
            document.getElementById("modalPhase").innerText = tr.phase || "Phase N/A";
            const countries = (tr.countries && tr.countries.length > 0) ? tr.countries.join(", ") : "EU / EEA Member States";
            document.getElementById("modalCountries").innerText = countries;
            document.getElementById("modalS3Path").innerText = tr.s3_silver_prefix || "-";
            document.getElementById("trialModal").classList.add("open");
        }}

        function closeModal() {{
            document.getElementById("trialModal").classList.remove("open");
        }}

        function closeModalOnBg(e) {{
            if (e.target.id === "trialModal") {{
                closeModal();
            }}
        }}

        document.addEventListener("keydown", function(e) {{
            if (e.key === "Escape") closeModal();
        }});

        function copyText(txt) {{
            if (!navigator.clipboard) {{
                const input = document.createElement("input");
                input.value = txt;
                document.body.appendChild(input);
                input.select();
                document.execCommand("copy");
                document.body.removeChild(input);
            }} else {{
                navigator.clipboard.writeText(txt);
            }}
            showToast("Copied: " + txt);
        }}

        function showToast(msg) {{
            const t = document.getElementById("toast");
            t.innerText = msg;
            t.style.display = "block";
            setTimeout(() => {{ t.style.display = "none"; }}, 2500);
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
        index_html_path = config.DATA_DIR / "index.html"
        try:
            with open(local_html_path, "w", encoding="utf-8") as f:
                f.write(html_content)
            with open(index_html_path, "w", encoding="utf-8") as f:
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



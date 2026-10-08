# Antigravity Agent Guidelines: EU CTIS Clinical Trial Pipeline

This workspace contains a production ETL pipeline extracting EU CTIS clinical trial records, splitting them into 6 domain JSON payloads, and persisting them to local disk / AWS S3 with state tracking in SQLite / DynamoDB.

---

## 1. Core Principles & Guardrails

1. **Bug Prevention (Strict Rule):**
   * Never modify code without verifying that there are zero syntax, import, or logical errors.
   * Always verify that all 6 JSON files are validated and verified on write/upload.
   * Maintain idempotency: Any operation must be safely re-runnable without state corruption.

2. **CTIS API Behavioral Rules:**
   * **1-Indexed Pagination:** Always use `"page": 1` for the first page of `POST /ctis-public-api/search`.
   * **Mandatory Search Parameter:** Always supply `"searchCriteria": {}` in search POST bodies.
   * **Missing Trials:** `GET /retrieve/{ctNumber}` returns `200 OK` with an empty dict `{}` for nonexistent trials; always verify `ctNumber` exists.
   * **Rate Limiting:** Never exceed 5 concurrent worker threads without explicit backoff.

3. **Data Normalization Contracts (Bronze & Silver):**
   * **Bronze Layer:** Exact unmodified API response: `raw.json` in `bronze/{ctNumber}/`.
   * **Silver Layer:** 6 standardized domain JSON entities in `silver/{ctNumber}/` (and `data/{ctNumber}/`):
     * `meta_data.json`
     * `summary.json`
     * `full_trial_information.json`
     * `trial_documents.json`
     * `trial_results.json`
     * `locations_and_contact_points.json`

4. **Multi-Process Concurrency:**
   * Local SQLite operates with WAL mode (`PRAGMA journal_mode=WAL;`).
   * Never disable WAL mode, as it allows concurrent reads and writes between the background historical backfill and scheduled cron tasks.

---

## 2. CLI Command Dispatches

* **Check System Health:** `python -m ctis_etl.main --mode check`
* **Historical Backfill:** `python -m ctis_etl.main --mode historical --workers 5`
* **New Trials Only:** `python -m ctis_etl.main --mode new --lookback-days 7`
* **Updated Trials Only:** `python -m ctis_etl.main --mode updates --lookback-days 7`
* **Combined Incremental:** `python -m ctis_etl.main --mode incremental --lookback-days 7`
* **Single Trial:** `python -m ctis_etl.main --mode single --ct-number <ID>`
* **Retry Failed Queue:** `python -m ctis_etl.main --mode retry-failed`

---

## 3. Key File Locations

* Configuration: `ctis_etl/config.py` (reads `.env`)
* HTTP Client: `ctis_etl/api_client.py`
* Models & Validation: `ctis_etl/models.py`
* JSON Splitter: `ctis_etl/parser.py`
* Storage (S3 / Local): `ctis_etl/storage.py`
* Database (SQLite / DynamoDB): `ctis_etl/database.py`
* Orchestrator: `ctis_etl/main.py`
* Docker Entrypoint: `entrypoint.sh`
* Cron Schedule: `crontab`
* Docs: `docs/`

# EU CTIS Clinical Trial Data Pipeline (ETL)

A Python ETL (Extract, Transform, Load) pipeline designed to ingest clinical trial data from the **European Union Clinical Trials Information System (CTIS)** public API, normalize and split each trial into 6 domain-specific JSON files, and store them locally and/or in **Amazon S3** with state tracking in **Amazon DynamoDB** and local **SQLite**.

---

## 1. Architecture Overview

```
                               ┌─────────────────────────────────┐
                               │   EU CTIS Public REST API       │
                               │  - POST /ctis-public-api/search │
                               │  - GET  /retrieve/{ctNumber}    │
                               └────────────────┬────────────────┘
                                                │
                                                ▼
                                    ┌───────────────────────┐
                                    │     ctis_etl Client   │
                                    │ (Pagination & Backoff)│
                                    └───────────┬───────────┘
                                                │
                       ┌────────────────────────┴────────────────────────┐
                       ▼                                                 ▼
             ┌───────────────────┐                             ┌───────────────────┐
             │ SQLite / DynamoDB │                             │  JSON Splitter    │
             │ (State Tracking)  │                             │ (6 Domain Files)  │
             └───────────────────┘                             └─────────┬─────────┘
                                                                         │
                                       ┌─────────────────────────────────┴─────────────────────────────────┐
                                       ▼                                                                   ▼
                         ┌───────────────────────────┐                                       ┌───────────────────────────┐
                         │   Local ./data/ Folder    │                                       │   Amazon S3 Bucket        │
                         │   ./data/{ctNumber}/*.json│                                       │   s3://{bucket}/{ctis}/.. │
                         └───────────────────────────┘                                       └───────────────────────────┘
```

---

## 2. Target Output Structure

For every ingested clinical trial (`ctNumber`), 6 discrete JSON entities are created:

```text
data/
└── 2026-527084-15-00/
    ├── meta_data.json                     # Status, publication dates, trial region, ingestion timestamp
    ├── summary.json                       # Identifiers, titles, sponsors, trial phase, medical conditions
    ├── full_trial_information.json        # Full scientific Part I protocol dossier & eligibility criteria
    ├── trial_documents.json               # Public document catalog & UUID metadata
    ├── trial_results.json                 # Trial outcome summaries & clinical study reports
    └── locations_and_contact_points.json  # Participating countries, recruitment sites, & sponsor contacts
```

---

## 3. Quick Start & Setup

### 3.1 Prerequisites
* Python 3.10+
* An AWS Account with S3 and DynamoDB permissions (if using AWS backends)

### 3.2 Installation

```bash
# Clone the repository
git clone <repo-url>
cd eu_clinical_trial

# Create and activate virtual environment
python -m venv .venv

# Windows:
.venv\Scripts\activate
# Linux/macOS:
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### 3.3 Configure Environment Variables

Copy the provided template and fill in your AWS credentials:

```bash
cp .env.example .env
```

Ensure the following variables are defined in `.env`:

```dotenv
AWS_ACCESS_KEY_ID=AKIAXT6...
AWS_SECRET_ACCESS_KEY=3Jb1Azj7...
AWS_REGION=us-east-1

S3_BUCKET_NAME=aascent-mindgram
S3_PREFIX=ctis/

DYNAMODB_TABLE_NAME=EU_Clinical
DYNAMODB_PARTITION_KEY=euc

# Storage & State Tracking Backends:
# STORAGE_BACKEND options: 's3', 'local', 'both'
# STATE_BACKEND options:   'dynamodb', 'sqlite', 'both'
STORAGE_BACKEND=s3
STATE_BACKEND=dynamodb
```

---

## 4. CLI Execution Commands & Processing Modes

The pipeline provides 3 dedicated processes along with utility commands to manage extraction, incremental sync, and backfilling.

### 4.1 CLI Commands Quick Reference Table

| Objective | Command | Description |
| :--- | :--- | :--- |
| **Process 1: Historical Backfill** | `python -m ctis_etl.main --mode historical` | Ingests all ~12,500+ trials page-by-page, skipping previously saved trials. |
| **Process 2: Brand New Trials** | `python -m ctis_etl.main --mode new --lookback-days 7` | Checks recently published trials and ingests only ones not yet in the DB. |
| **Process 3: Updates & Amendments** | `python -m ctis_etl.main --mode updates --lookback-days 7` | Detects modified/amended trials and updates their dossiers in S3 & DynamoDB. |
| **Combined Incremental Sync** | `python -m ctis_etl.main --mode incremental --lookback-days 7` | Runs both Process 2 (New) and Process 3 (Updates) in a single pass. |
| **Single Trial Ingestion** | `python -m ctis_etl.main --mode single --ct-number <ID>` | Ingests a single specified trial dossier immediately. |
| **Retry Failed Trials** | `python -m ctis_etl.main --mode retry-failed` | Re-queues and retries trials previously flagged with `FAILED` status. |
| **Diagnostic Pre-flight Check** | `python -m ctis_etl.main --mode check` | Validates CTIS API, S3 bucket, DynamoDB, SQLite, and local disk permissions. |

---

### 4.2 Detailed Command Examples

#### Process 1: Historical Data Collector (Full Backfill)
Streams through the complete CTIS database (~12,500+ trials) page by page with checkpointing:
```bash
# Standard backfill (5 worker threads, saves to S3 and DynamoDB from .env)
python -m ctis_etl.main --mode historical --workers 5

# Local-only backfill (saves directly into ./data/ without cloud upload)
python -m ctis_etl.main --mode historical --workers 5 --storage local
```

#### Process 2: Brand New Trials Ingestion
Scans recent trials and ingests strictly new ones:
```bash
# Ingest new trials added in the last 7 days
python -m ctis_etl.main --mode new --lookback-days 7

# Ingest new trials added in the last 30 days
python -m ctis_etl.main --mode new --lookback-days 30
```

#### Process 3: Trial Updates & Amendments
Scans recently updated trials and synchronizes dossiers that changed on the CTIS portal:
```bash
# Check and update amended trials from the last 7 days
python -m ctis_etl.main --mode updates --lookback-days 7
```

#### Combined Incremental Sync
Runs both Process 2 (New) and Process 3 (Updates) sequentially:
```bash
python -m ctis_etl.main --mode incremental --lookback-days 7 --workers 5
```

#### Single Trial Extraction (Testing & Ad-hoc)
```bash
# Extract trial 2026-527084-15-00 to S3 and DynamoDB
python -m ctis_etl.main --mode single --ct-number 2026-527084-15-00

# Extract trial to local ./data/ folder only
python -m ctis_etl.main --mode single --ct-number 2026-527084-15-00 --storage local
```

#### Re-run Previously Failed Trials
```bash
python -m ctis_etl.main --mode retry-failed --workers 5
```

---

### 4.3 CLI Options & Flags Reference

| Option | Values | Default | Description |
| :--- | :--- | :--- | :--- |
| `--mode` | `historical`, `new`, `updates`, `incremental`, `single`, `retry-failed` | `incremental` | Pipeline execution mode. |
| `--lookback-days` | Integer (e.g. `1`, `7`, `30`) | `7` | Days to look back on Search API for new/updated trials. |
| `--ct-number` | String (e.g. `2026-527084-15-00`) | None | Required when `--mode single` is specified. |
| `--workers` | Integer (e.g. `1` to `10`) | `5` | Concurrent worker threads for download & parsing. |
| `--storage` | `s3`, `local`, `both` | from `.env` | Override destination storage backend. |

---

### 4.4 Running CLI Commands inside Docker

If running via Docker Compose, you can trigger any of the CLI commands on-demand without stopping background jobs:

```bash
# Run historical backfill in the running container
docker compose exec ctis-etl python -m ctis_etl.main --mode historical

# Run new trial check
docker compose exec ctis-etl python -m ctis_etl.main --mode new --lookback-days 7

# Run updates check
docker compose exec ctis-etl python -m ctis_etl.main --mode updates --lookback-days 7

# Ingest a single trial
docker compose exec ctis-etl python -m ctis_etl.main --mode single --ct-number 2026-527084-15-00
```

---

## 5. Docker Deployment & Automated Scheduling

The Docker container runs all 3 processes together:
1. **Background Cron Daemon:** Runs on boot to execute scheduled checks:
   * **Every 6 hours:** Runs **Process 2** (`--mode new`) to detect and ingest new trials.
   * **Daily at 02:00 AM UTC:** Runs **Process 3** (`--mode updates`) to check for amendments and update existing trials.
2. **Startup Historical Backfill:** Automatically runs **Process 1** (`--mode historical`) on container start to continuously ingest all historical trials in the background.

### 5.1 Quick Start with Docker Compose

Ensure your `.env` contains your AWS credentials and settings, then launch:

```bash
# Build and start container in detached mode
docker compose up -d

# View live streaming logs
docker compose logs -f
```

### 5.2 Customizing Cron Schedule & Startup Behavior

In `docker-compose.yml` or via `.env`, configure:

* `CRON_SCHEDULE`: Standard cron expression. Examples:
  * `0 2 * * *` — Daily at 02:00 UTC (default)
  * `0 */12 * * *` — Every 12 hours
  * `0 0 * * *` — Midnight daily
* `RUN_ON_STARTUP`: `true` to immediately trigger extraction when container boots.

---

## 6. Quality Controls & Resilience

* **Rate Limiting & Backoff:** Uses `ThreadPoolExecutor` capped at 5 workers with exponential backoff and jitter on HTTP 429 and 5xx responses.
* **Non-existent Trial Detection:** Detects `200 OK` empty JSON payloads (`{}`) and marks them accordingly.
* **Quarantine:** Any malformed response or schema failure is routed to `./quarantine/{ctNumber}_{timestamp}.json` alongside an error log for post-mortem debugging.
* **Audit Trail:** Every pipeline run logs execution metrics to the `pipeline_runs` table in SQLite (`tracker.db`) and to `./logs/ctis_etl.log`.

---

## 7. Directory Structure

```text
eu_clinical_trial/
├── .env                              # Environment configuration & AWS credentials (git-ignored)
├── .env.example                      # Sanitized configuration template
├── .gitignore                        # Git ignore patterns
├── .dockerignore                     # Docker build exclusion rules
├── Dockerfile                        # Multi-stage production container with cron
├── docker-compose.yml                # Container orchestration & volume mapping
├── entrypoint.sh                     # Container startup & cron daemon script
├── crontab                           # Crontab schedule configuration
├── README.md                         # Documentation
├── requirements.txt                  # Python package dependencies
├── tracker.db                        # SQLite state database (created automatically)
├── data/                             # Local trial JSON dossiers (created automatically)
├── quarantine/                       # Malformed or failed payloads (created automatically)
├── logs/                             # Application logs (created automatically)
├── docs/
│   └── EU CTIS Data Pipeline - ETL Implementation Guide.md
└── ctis_etl/
    ├── __init__.py
    ├── config.py                     # Configuration & environment loader
    ├── models.py                     # Pydantic schema models
    ├── api_client.py                 # CTIS HTTP client with retries & pagination
    ├── parser.py                     # Splits trial data into 6 discrete JSON entities
    ├── storage.py                    # Storage handlers (Local disk, S3, Quarantine)
    ├── database.py                   # State management (DynamoDB & SQLite)
    └── main.py                       # CLI orchestrator
```

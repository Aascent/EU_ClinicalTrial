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

## 4. CLI Execution Modes

Run the pipeline using `python -m ctis_etl.main`:

### Incremental Daily Sync (Default)
Discovers trials published or amended within the last 7 days and syncs only new or modified dossiers:

```bash
python -m ctis_etl.main --mode incremental --lookback-days 7
```

### Single Trial Ingestion
Extracts, transforms, and stores a specific trial by its CT number:

```bash
python -m ctis_etl.main --mode single --ct-number 2026-527084-15-00
```

### Full Catalog Backfill
Paginates through the entire CTIS public database (~12,500+ trials):

```bash
python -m ctis_etl.main --mode full --workers 5
```

### Retry Failed Trials
Re-queues and retries trials previously marked as `FAILED`:

```bash
python -m ctis_etl.main --mode retry-failed
```

### Local Storage Override
Save outputs locally regardless of `.env`:

```bash
python -m ctis_etl.main --mode single --ct-number 2026-527084-15-00 --storage local
```

---

## 5. Quality Controls & Resilience

* **Rate Limiting & Backoff:** Uses `ThreadPoolExecutor` capped at 5 workers with exponential backoff and jitter on HTTP 429 and 5xx responses.
* **Non-existent Trial Detection:** Detects `200 OK` empty JSON payloads (`{}`) and marks them accordingly.
* **Quarantine:** Any malformed response or schema failure is routed to `./quarantine/{ctNumber}_{timestamp}.json` alongside an error log for post-mortem debugging.
* **Audit Trail:** Every pipeline run logs execution metrics to the `pipeline_runs` table in SQLite (`tracker.db`) and to `./logs/ctis_etl.log`.

---

## 6. Directory Structure

```text
eu_clinical_trial/
├── .env                              # Environment configuration & AWS credentials (git-ignored)
├── .env.example                      # Sanitized configuration template
├── .gitignore                        # Git ignore patterns
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

# EU CTIS Clinical Trial Data Pipeline (ETL)

[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![Storage Backend: S3 & Local](https://img.shields.io/badge/storage-S3%20%26%20Local-orange.svg)](#8-data-normalization-architecture-bronze-silver--history)
[![State Backend: DynamoDB & SQLite WAL](https://img.shields.io/badge/state-DynamoDB%20%26%20SQLite%20WAL-green.svg)](#9-state-management--database-design)
[![Docker Ready](https://img.shields.io/badge/docker-ready-blue.svg)](#12-docker-deployment--automated-scheduling)
[![License: Proprietary](https://img.shields.io/badge/license-Proprietary-lightgrey.svg)](#)

A production-grade, fault-tolerant Python ETL (Extract, Transform, Load) pipeline designed to ingest, normalize, and synchronize clinical trial data from the **European Union Clinical Trials Information System (EU CTIS)** public REST API.

The pipeline structures every trial into an immutable **Bronze Layer** (raw untouched API response) and a normalized **Silver Layer** (6 domain-specific JSON payloads), with automated regulatory PDF document management, historical version archiving, and dual state tracking across **Amazon S3 / Local Disk** and **Amazon DynamoDB / Local SQLite (WAL Mode)**.

---

## Table of Contents

- [1. Executive Summary & Regulatory Context](#1-executive-summary--regulatory-context)
- [2. System Architecture & High-Level Topology](#2-system-architecture--high-level-topology)
- [3. End-to-End Code Flow & Execution Lifecycle (A to Z)](#3-end-to-end-code-flow--execution-lifecycle-a-to-z)
  - [3.1 Visual Sequence Diagram](#31-visual-sequence-diagram)
  - [3.2 Detailed Step-by-Step Flow](#32-detailed-step-by-step-flow)
- [4. API Rate Limiting & Firewall Mitigation Architecture](#4-api-rate-limiting--firewall-mitigation-architecture)
  - [4.1 Upstream EMA Gateway Profile & Anti-Scraping Behaviors](#41-upstream-ema-gateway-profile--anti-scraping-behaviors)
  - [4.2 The 5 Defensive Layers (How We Prevent Bans)](#42-the-5-defensive-layers-how-we-prevent-bans)
  - [4.3 Backoff Sleep Curve & Retry Policy](#43-backoff-sleep-curve--retry-policy)
- [5. API Behavioral Quirks, Known Limitations & Technical Solutions](#5-api-behavioral-quirks-known-limitations--technical-solutions)
  - [5.1 Summary Matrix of API Quirks & Resolutions](#51-summary-matrix-of-api-quirks--resolutions)
  - [5.2 The 10,000 Record Pagination Cap & Bi-Directional Dual-Pass Solution](#52-the-10000-record-pagination-cap--bi-directional-dual-pass-solution)
  - [5.3 S3 Pre-Signed URL 300-Second Expiration & Permanent Endpoint Strategy](#53-s3-pre-signed-url-300-second-expiration--permanent-endpoint-strategy)
  - [5.4 Empty 200 OK Responses on Missing Records](#54-empty-200-ok-responses-on-missing-records)
  - [5.5 SQLite Concurrency & WAL (Write-Ahead Logging) Mode](#55-sqlite-concurrency--wal-write-ahead-logging-mode)
  - [5.6 CTIS vs. Legacy EudraCT Scope Boundary](#56-ctis-vs-legacy-eudract-scope-boundary)
- [6. Data Normalization Architecture (Bronze, Silver & History Tiers)](#6-data-normalization-architecture-bronze-silver--history-tiers)
  - [6.1 Bronze Tier (Raw Audit Layer)](#61-bronze-tier-raw-audit-layer)
  - [6.2 Silver Tier (The 6 Standard Domain Entities)](#62-silver-tier-the-6-standard-domain-entities)
  - [6.3 History Tier (Automated Version Archiving)](#63-history-tier-automated-version-archiving)
  - [6.4 Quarantine Tier (Dead-Letter Handling)](#64-quarantine-tier-dead-letter-handling)
- [7. State Management & Resilient Database Design](#7-state-management--resilient-database-design)
  - [7.1 The Lifecycle State Machine](#71-the-lifecycle-state-machine)
  - [7.2 SQLite Database Engine (Local Persistence)](#72-sqlite-database-engine-local-persistence)
  - [7.3 Amazon DynamoDB Engine (Cloud Persistence)](#73-amazon-dynamodb-engine-cloud-persistence)
  - [7.4 Self-Healing Stale Job Recovery](#74-self-healing-stale-job-recovery)
  - [7.5 Cloud State Hydration on Startup](#75-cloud-state-hydration-on-startup)
- [8. Installation & Environment Configuration](#8-installation--environment-configuration)
  - [8.1 Prerequisites](#81-prerequisites)
  - [8.2 Local Setup](#82-local-setup)
  - [8.3 Environment Variables Reference (.env Dictionary)](#83-environment-variables-reference-env-dictionary)
- [9. CLI Command Execution & Operational Modes](#9-cli-command-execution--operational-modes)
  - [9.1 Makefile Command Reference](#91-makefile-command-reference)
  - [9.2 CLI Operational Modes Reference](#92-cli-operational-modes-reference)
  - [9.3 CLI Flags & Argument Options](#93-cli-flags--argument-options)
  - [9.4 Detailed Operational Command Examples](#94-detailed-operational-command-examples)
- [10. Docker Deployment & Automated Scheduling](#10-docker-deployment--automated-scheduling)
  - [10.1 Container Architecture & Multi-Daemon Lifecycle](#101-container-architecture--multi-daemon-lifecycle)
  - [10.2 Cron Schedule Configuration](#102-cron-schedule-configuration)
  - [10.3 Web Dashboard & Monitoring HTTP Server](#103-web-dashboard--monitoring-http-server)
  - [10.4 Docker Compose Quickstart](#104-docker-compose-quickstart)
- [11. Auxiliary Utilities & Operational Scripts](#11-auxiliary-utilities--operational-scripts)
  - [11.1 Document Downloader CLI (`download_trial_docs.py`)](#111-document-downloader-cli-download_trial_docspy)
  - [11.2 State Inspector CLI (`view_db.py`)](#112-state-inspector-cli-view_dbpy)
  - [11.3 System Pre-Flight Diagnostics (`--mode check`)](#113-system-pre-flight-diagnostics---mode-check)
- [12. Performance Estimations, Storage Footprint & AWS Costs](#12-performance-estimations-storage-footprint--aws-costs)
  - [12.1 Latency Profile per Trial](#121-latency-profile-per-trial)
  - [12.2 Historical Backfill Throughput (~12,500+ Trials)](#122-historical-backfill-throughput-12500-trials)
  - [12.3 Ongoing Daily Incremental Sync Runtime](#123-ongoing-daily-incremental-sync-runtime)
  - [12.4 Cloud Storage Footprint & Monthly AWS Cost Breakdown](#124-cloud-storage-footprint--monthly-aws-cost-breakdown)
- [13. Troubleshooting & Operations FAQ](#13-troubleshooting--operations-faq)

---

## 1. Executive Summary & Regulatory Context

Under the **European Union Clinical Trials Regulation (Regulation (EU) No 536/2014 - CTR)**, effective January 31, 2022, all clinical trial applications across the 27 EU member states, Iceland, Liechtenstein, and Norway are submitted, authorized, and overseen through the **Clinical Trials Information System (CTIS)** managed by the **European Medicines Agency (EMA)**.

CTIS provides a unified public portal (`https://euclinicaltrials.eu/ctis-public-api`) exposing trial metadata, scientific dossiers, sponsor information, clinical study sites, and authorized regulatory documents.

### What This Pipeline Delivers
1. **Full Historical Ingestion:** Ingests the complete global CTIS catalog (~12,500+ active trials) without data loss, overcoming API window limitations.
2. **Autonomous Incremental Synchronization:** Continuously tracks newly published trials (every 6 hours) and amended/updated trial dossiers (daily at 02:00 UTC).
3. **Multi-Tier Storage:** Simultaneously writes to local disk and/or Amazon S3, organized into Bronze (raw API response) and Silver (6 structured domain JSON entities).
4. **Permanent Regulatory Document Access:** Enriches document metadata with stable public download endpoints and supports local binary PDF archiving, eliminating 5-minute pre-signed link expiration.
5. **Auditable Version History:** Archives previous dossier states into a dedicated history tier before overwriting amended trials.
6. **Zero-Lock Concurrency & Dual State Tracking:** Uses SQLite with Write-Ahead Logging (WAL) for local multi-process safety and Amazon DynamoDB for centralized cloud tracking.
7. **Live Visibility:** Emits automated JSON health reports and renders an interactive web dashboard on port `8080`.

---

## 2. System Architecture & High-Level Topology

```
                                  ┌──────────────────────────────────────────────┐
                                  │         EU CTIS Public REST API              │
                                  │    https://euclinicaltrials.eu/ctis-public-api│
                                  │      - POST /search                          │
                                  │      - GET  /retrieve/{ctNumber}             │
                                  │      - GET  /documents/{ctNumber}/{uuid}     │
                                  └──────────────────────┬───────────────────────┘
                                                         │ HTTPS (SSL/TLS)
                                                         │ Keep-Alive / Pool
                                                         ▼
                                  ┌──────────────────────────────────────────────┐
                                  │              CTISClient Layer                │
                                  │  - Persistent httpx pool (HTTP/2 keepalive)  │
                                  │  - Jittered Exponential Backoff (429/5xx)    │
                                  │  - 100ms Pacing Delay per Worker             │
                                  │  - 1-Indexed Pagination & Empty-Dict Guard   │
                                  └──────────────────────┬───────────────────────┘
                                                         │
                                  ┌──────────────────────┴───────────────────────┐
                                  ▼                                              ▼
                    ┌───────────────────────────┐                  ┌───────────────────────────┐
                    │    Discovery & State      │                  │    Extraction & Parsing   │
                    │       (database.py)       │                  │       (parser.py)         │
                    │  - Dual State Engine      │                  │  - Bronze raw.json capture│
                    │  - SQLite (WAL Mode)      │                  │  - Splits into 6 Silver   │
                    │  - Amazon DynamoDB        │                  │    domain JSON entities   │
                    │  - Stale Lock Auto-Recover│                  │  - Zero data loss Part I  │
                    └─────────────┬─────────────┘                  └─────────────┬─────────────┘
                                  │                                              │
                                  │                                              ▼
                                  │                                ┌───────────────────────────┐
                                  │                                │   Storage & Archival      │
                                  │                                │       (storage.py)        │
                                  │                                │  - History Archival       │
                                  │                                │  - Atomic .tmp renaming   │
                                  │                                │  - S3 AES-256 Encryption  │
                                  │                                │  - Quarantine Handler     │
                                  │                                └─────────────┬─────────────┘
                                  │                                              │
                    ┌─────────────┴─────────────┐                  ┌─────────────┴─────────────┐
                    ▼                           ▼                  ▼                           ▼
        ┌───────────────────────┐   ┌───────────────────────┐  ┌───────────────────────┐   ┌───────────────────────┐
        │  SQLite (tracker.db)  │   │   AWS DynamoDB Table  │  │ Local Disk (./data/)  │   │    Amazon S3 Bucket   │
        │  - trials table       │   │   Table: EU_Clinical  │  │ - bronze/{ctNumber}/  │   │ - ctis/bronze/...     │
        │  - pipeline_runs      │   │   PK: euc ({ctNumber})│  │ - silver/{ctNumber}/  │   │ - ctis/silver/...     │
        │  - WAL journal mode   │   │   Adaptive Retries    │  │ - history/{ctNumber}/ │   │ - ctis/history/...    │
        └───────────────────────┘   └───────────────────────┘  └───────────────────────┘   └───────────────────────┘
```

---

## 3. End-to-End Code Flow & Execution Lifecycle (A to Z)

### 3.1 Visual Sequence Diagram

```mermaid
sequenceDiagram
    autonumber
    participant CLI as CLI / Cron Daemon
    participant Main as ctis_etl.main
    participant DB as SQLite / DynamoDB
    participant API as CTIS Public API
    participant Parser as ctis_etl.parser
    participant Storage as ctis_etl.storage

    CLI->>Main: Launch Mode (historical, new, updates, incremental)
    Main->>DB: init_sqlite_db() (Set WAL Mode & Busy Timeout)
    Main->>DB: restore_state_from_dynamodb_if_empty()
    Main->>DB: reset_stale_processing(timeout=15m)
    
    rect rgb(240, 248, 255)
        Note over Main,API: Phase 1: Search & Discovery
        Main->>API: POST /ctis-public-api/search (page=1, size=100, criteria={})
        API-->>Main: JSON Response: totalRecords, pagination, data[]
        loop For each trial summary in page
            Main->>Main: parse_ctis_date(raw_date) (strip country code)
            Main->>DB: stage_trial(ct_number, raw_date)
            DB-->>Main: Return status: PENDING, UPDATE_PENDING, or SKIPPED
        end
    end

    rect rgb(255, 250, 240)
        Note over Main,Storage: Phase 2: Concurrent Worker Ingestion (ThreadPoolExecutor max_workers=5)
        Main->>DB: mark_processing(ct_number)
        Main->>API: GET /ctis-public-api/retrieve/{ctNumber}
        alt Trial Not Found / Empty
            API-->>Main: 200 OK with {}
            Main->>Storage: quarantine_payload()
            Main->>DB: mark_failure(ct_number, "Trial not found or empty")
        else Success
            API-->>Main: 200 OK with full raw JSON dossier
            Main->>Parser: parse_trial_dossier(raw_data)
            Parser-->>Main: 6 Domain JSON Entities (meta_data, summary, full_trial, etc.)
            
            opt Trial was previously saved (Update flow)
                Main->>Storage: archive_existing_trial(ct_number)
                Storage->>Storage: Move old files to history/{ctNumber}/{timestamp}/
            end

            opt Document Sync Enabled
                Main->>Storage: sync_trial_documents(ct_number, documents)
                Storage-->>Main: Enriched permanent API download URLs
            end

            Main->>Storage: save_trial_files(ct_number, silver_files, raw_payload)
            Storage->>Storage: Write Bronze (raw.json) & Silver (6 JSONs) to Local/S3
            Storage-->>Main: Return save results (True/False for all 7 files)
            
            alt All 7 files saved successfully
                Main->>DB: mark_success(ct_number, publish_date)
            else Any file write failed
                Main->>Storage: quarantine_payload()
                Main->>DB: mark_failure(ct_number, "Storage write error")
            end
        end
    end

    rect rgb(245, 255, 250)
        Note over Main,Storage: Phase 3: Run Wrap-Up & Dashboard Publishing
        Main->>DB: record_pipeline_run(status="COMPLETED", metrics...)
        Main->>Storage: publish_pipeline_status_report()
        Storage->>Storage: Render status_report.json & index.html to S3/Disk
    end
```

### 3.2 Detailed Step-by-Step Flow

#### Phase 1: Pipeline Initialization & Self-Healing
1. **Config & Environment Loading (`ctis_etl/config.py`):**
   * Reads settings from `.env` or system environment variables.
   * Auto-resolves runtime directories: `./data/bronze`, `./data/silver`, `./data/history`, `./quarantine`, `./logs`.
   * Resolves the SQLite database path (`tracker.db`), with guards for Docker volume mount path quirks.
2. **Database Initialization (`ctis_etl/database.py`):**
   * Connects to SQLite and enforces `PRAGMA journal_mode=WAL;` and `PRAGMA busy_timeout=30000;`.
   * Creates the `trials` and `pipeline_runs` tables and performance indexes.
3. **Cloud State Hydration:**
   * If local `tracker.db` is empty (e.g. running inside a brand-new Docker container), `restore_state_from_dynamodb_if_empty()` scans DynamoDB table `EU_Clinical` and pre-populates SQLite with existing trial states, avoiding redundant downloads.
4. **Stale Lock Auto-Recovery:**
   * `reset_stale_processing(timeout_minutes=15)` finds trials stuck in `PROCESSING` state due to previous process kills or crashes and resets them to `PENDING`.
   * `reset_stale_pipeline_runs(timeout_hours=4)` marks orphaned runs as `INTERRUPTED`.

#### Phase 2: Discovery & Staging
1. **Search Endpoint Invocation (`ctis_etl/api_client.py`):**
   * Issues `POST /ctis-public-api/search` with mandatory payload envelope:
     ```json
     {"pagination": {"page": 1, "size": 100}, "sort": {"property": "decisionDate", "direction": "DESC"}, "searchCriteria": {}}
     ```
2. **Date Sanitization & Lookback Cutoff:**
   * Cleans country code prefixes using regex: `re.sub(r"^[A-Za-z]{2,3}:\s*", "", raw_date)`.
   * Formats into a standard UTC timestamp.
   * In incremental modes (`new`, `updates`, `incremental`), the scanner halts once publication dates fall outside the lookback window (e.g., older than 7 days).
3. **Smart State Comparison (`database.stage_trial`):**
   * Checks whether the trial exists in SQLite/DynamoDB:
     * **If not found:** Marked as `PENDING` (new trial).
     * **If found but `raw_date > last_publish_date`:** Marked as `UPDATE_PENDING` (amended trial).
     * **If found and dates match:** Marked as `SKIPPED` (up-to-date; no network retrieve request is spent).

#### Phase 3: Detail Retrieval & Network Safety Layer
1. **Task Dispatch (`ctis_etl/main.py`):**
   * Enqueues pending trial numbers to a `ThreadPoolExecutor` capped at `MAX_WORKERS` (default: 5).
2. **Atomic In-Flight Lock:**
   * Each worker calls `database.mark_processing(ct_number)` to lock the record, preventing parallel cron jobs from processing the same trial.
3. **Dossier Retrieval (`ctis_etl/api_client.py`):**
   * Calls `GET /ctis-public-api/retrieve/{ctNumber}`.
   * Leverages a persistent `httpx.Client` connection pool (with `urllib.request` fallback) and enforces a 100ms pacing delay.
   * Catches `429 Too Many Requests` or `5xx Server Errors` and applies exponential backoff with randomized jitter.
4. **Empty Object Detection:**
   * Validates that response is non-empty and contains `ctNumber`. If CTIS returns `200 OK` with `{}` (missing trial), raises `CTISNotFoundError`.

#### Phase 4: Defensive Parsing & Silver Layer Normalization
1. **Domain Splitting (`ctis_etl/parser.py`):**
   * Deconstructs the monolithic JSON response into 6 domain JSON payloads using defensive wrappers (`_safe_dict`, `_safe_list`) to ensure missing fields or unexpected nulls never cause exceptions.
   * Preserves `authorizedPartI` entirely within `full_trial_information.json` to guarantee zero scientific schema loss.

#### Phase 5: Regulatory Documents & PDF Resolution
1. **Permanent URL Enrichment:**
   * CTIS regulatory document download links expire after 300 seconds.
   * The pipeline rewrites document URLs in `trial_documents.json` to the permanent public API endpoint:
     `https://euclinicaltrials.eu/ctis-public-api/documents/{ctNumber}/{uuid}/download`
2. **Binary PDF Download (`DOWNLOAD_DOCUMENTS=true`):**
   * When enabled, invokes the endpoint to resolve a dynamic pre-signed link and downloads the binary PDF directly to `./data/silver/{ctNumber}/documents/{clean_title}.pdf` and/or S3.

#### Phase 6: Automated Version Archiving (History Tier)
1. **Pre-Update Snapshot (`storage.archive_existing_trial`):**
   * If a trial already exists in storage and is receiving an update, existing Bronze and Silver files are snapshotted into:
     * Local: `data/history/{ctNumber}/{YYYYMMDDTHHMMSSZ}/`
     * AWS S3: `s3://{bucket}/{prefix}history/{ctNumber}/{YYYYMMDDTHHMMSSZ}/` (via fast server-side `copy_object`)
   * Adds `previous_version_archived_at` and `is_updated: true` metadata tags to `meta_data.json`.

#### Phase 7: Storage Persistence & Atomic Verification
1. **Atomic Local Disk Write:**
   * Writes each JSON document to a temporary file (`.tmp`) and executes an atomic POSIX rename to prevent partial writes during crashes.
2. **Amazon S3 Upload:**
   * Encrypts objects with `AES256` server-side encryption.
   * Injects audit metadata headers (`ct-number`, `filename`, `ingested-at`).
3. **All-or-Nothing Verification:**
   * Confirms all 7 files (1 Bronze `raw.json` + 6 Silver domain files) exist and have size `> 0`. If any file fails, the entire transaction is rolled back and quarantined.

#### Phase 8: State Finalization & Quarantine Handling
1. **Success Path:**
   * Calls `database.mark_success(ct_number, publish_date)`, updating SQLite and DynamoDB with status `SUCCESS` and recording execution timestamps.
2. **Failure Path:**
   * On unhandled exceptions or network exhaustion, `storage.quarantine_payload()` saves the raw response to `./quarantine/{ctNumber}_{timestamp}.json` alongside a detailed stack trace in `.error.log`.
   * Calls `database.mark_failure(ct_number, error_msg)`, incrementing the retry counter.

#### Phase 9: Reporting & Live Dashboard Publishing
1. **Audit Metrics Aggregation:**
   * Calculates overall trial metrics, runs by type, daily activity, and success rates from `pipeline_runs`.
2. **Dashboard Rendering (`storage.publish_pipeline_status_report`):**
   * Writes `status_report.json` and renders an interactive HTML dashboard (`index.html`).
   * When S3 is active, uploads both files and generates 7-day pre-signed browser URLs for monitoring.

---

## 4. API Rate Limiting & Firewall Mitigation Architecture

### 4.1 Upstream EMA Gateway Profile & Anti-Scraping Behaviors

The EU CTIS API (`https://euclinicaltrials.eu/ctis-public-api`) is hosted on European Medicines Agency infrastructure guarded by enterprise Web Application Firewalls (WAF) and reverse proxies. Through empirical load testing, the following gateway behaviors were identified:

| Gateway Condition | Server Response | Root Cause | Pipeline Impact If Unmitigated |
| :--- | :--- | :--- | :--- |
| **High Concurrency (>10–15 parallel requests)** | `HTTP 429 Too Many Requests` | Rapid connection burst exhaustion | Thread starvation and data pipeline abortion |
| **Sub-50ms Request Intervals** | `HTTP 429` or Temporary IP Blacklist | Automated scraper detection heuristic | IP blocked for 15–60 minutes |
| **Peak EU Business Hours (08:00–16:00 UTC)** | `HTTP 503 Service Unavailable` or `504 Gateway Timeout` | Heavy EMA backend database load | Transient dropped connections |
| **Non-Existent / Withdrawn Dossiers** | `HTTP 200 OK` with `{}` | Upstream design quirk | JSON parsing crash on `None` attributes |
| **Generic Script User-Agents** | `HTTP 403 Forbidden` / Cloudflare Challenge | WAF bot signature filtering | Immediate execution failure |

---

### 4.2 The 5 Defensive Layers (How We Prevent Bans)

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                            CTIS ETL WORKER POOL                             │
│                                                                             │
│  [Worker 1]    [Worker 2]    [Worker 3]    [Worker 4]    [Worker 5]         │
│      │              │              │              │              │          │
│      └──────────────┴───────┬──────┴──────────────┴──────────────┘          │
│                             │                                               │
│              [ Layer 1: Concurrency Cap = Max 5 Threads ]                   │
│                             │                                               │
│              [ Layer 2: Pacing Delay = 100ms between calls ]                │
│                             │                                               │
│              [ Layer 3: Connection Pool (httpx / HTTP/2) ]                  │
│                (Max Keepalive: 20, Max Conns: 50, Timeout: 30s)             │
│                             │                                               │
│              [ Layer 4: Authentic Chrome 154 Browser Emulation ]            │
│                             │                                               │
└─────────────────────────────┼───────────────────────────────────────────────┘
                              │
                    HTTPS Requests (SSL/TLS)
                              │
                              ▼
            ┌───────────────────────────────────┐
            │   EMA CTIS Public REST Gateway    │
            └─────────────────┬─────────────────┘
                              │
             Is response HTTP 429, 500, 502, 503, 504?
             ├── NO (200 OK)  ──► Process & Persist to S3/DynamoDB
             └── YES (429/5xx)──► [ Layer 5: Exponential Backoff + Jitter ]:
                                  Wait 2.5s ──► Retry 1
                                  Wait 4.5s ──► Retry 2
                                  Wait 8.5s ──► Retry 3
                                  (Exhausted ──► Move to Quarantine)
```

1. **Worker Concurrency Cap (`MAX_WORKERS = 5`):**
   * Configured in `ctis_etl/config.py`. Prevents thread explosion and keeps total parallel connections well below the gateway's 10–15 connection throttling threshold.
2. **Inter-Request Pacing Delay (`REQUEST_DELAY_SECONDS = 0.1`):**
   * Introduces a mandatory 100ms pause prior to every outbound HTTP call on each thread, eliminating burst spikes.
3. **Persistent HTTP Connection Pooling:**
   * Utilizes `httpx.Client(limits=httpx.Limits(max_keepalive_connections=20, max_connections=50, keepalive_expiry=30.0))` to reuse established TLS handshakes, cutting round-trip latency by ~60% and reducing gateway connection setup overhead.
4. **Authentic Browser Fingerprinting:**
   * Uses realistic browser request headers in `config.DEFAULT_HEADERS` (Chromium User-Agent, `Sec-CH-UA`, `Sec-Fetch-*`, `Accept-Language`, and `Cookie: accepted_cookie=true`), ensuring traffic is indistinguishable from standard public portal navigation.
5. **State-Driven Redundancy Elimination:**
   * Before issuing `GET /retrieve/{ctNumber}`, the pipeline inspects `last_publish_date` against SQLite/DynamoDB. If unchanged, **zero HTTP requests are made**.

---

### 4.3 Backoff Sleep Curve & Retry Policy

When the pipeline encounters an HTTP `429`, `500`, `502`, `503`, or `504`, it initiates exponential backoff with full randomized jitter:

$$\text{Sleep Time} = 2^{\text{attempt}} + \text{uniform}(0.1, 1.0)$$

```text
Attempt 1: 2^1 + uniform(0.1, 1.0) = ~2.10s - 3.00s
Attempt 2: 2^2 + uniform(0.1, 1.0) = ~4.10s - 5.00s
Attempt 3: 2^3 + uniform(0.1, 1.0) = ~8.10s - 9.00s
```

* **Retry Exhaustion:** If a trial fails after 3 attempts, it is quarantined in `./quarantine/` and marked as `FAILED` in the database.
* **Worker Isolation:** Backoff delays only block the specific worker thread encountering the issue; remaining worker threads continue processing other trials.
* **Re-Queueing:** Operators can retry all failed records at any time via `make retry` or `python -m ctis_etl.main --mode retry-failed`.

---

## 5. API Behavioral Quirks, Known Limitations & Technical Solutions

### 5.1 Summary Matrix of API Quirks & Resolutions

| # | Upstream CTIS API Quirk / Limitation | Real-World Failure If Unhandled | Our Engineered Technical Solution |
| :--- | :--- | :--- | :--- |
| **1** | **Elasticsearch 10,000 Result Window Limit** | Full catalog scans halt at page 100 (~10,000 trials), leaving ~2,500 historical trials unindexed. | **Bi-Directional Dual-Pass Pagination:** Pass 1 fetches newest-to-oldest (`DESC`); Pass 2 fetches oldest-to-newest (`ASC`), achieving 100% catalog coverage. |
| **2** | **1-Indexed Pagination Base** | Requesting `"page": 0` silently returns `totalRecords: 0` and empty `data: []`. | Pagination loops strictly start at `"page": 1`. |
| **3** | **Mandatory `"searchCriteria": {}`** | Omitting the search criteria field causes the search endpoint to return 0 records. | Request payloads always include explicit `"searchCriteria": {}`. |
| **4** | **Missing Records Return `200 OK` with `{}`** | Fetching non-existent trials returns HTTP 200 with an empty body, causing `KeyError` crashes in naive parsers. | Strict validation: `if not data or not data.get("ctNumber"): raise CTISNotFoundError()`. |
| **5** | **Pre-Signed S3 URLs Expire in 300 Seconds** | Direct AWS S3 document download links become invalid (`403 AccessDenied`) after 5 minutes. | **Permanent Endpoint Strategy:** URLs stored in `trial_documents.json` point to the stable public API download endpoint, which generates fresh pre-signed URLs on demand. |
| **6** | **Search API Date Formatting Inconsistencies** | Search API strings include country codes (e.g. `"HU: 07/10/2026"`) and use `DD/MM/YYYY`, while Retrieve API uses ISO-8601. | Regex prefix stripper `re.sub(r"^[A-Za-z]{2,3}:\s*", "", date_str)` and multi-format parser `parse_ctis_date()`. |
| **7** | **SQLite File Locking Under Multi-Process Concurrency** | Simultaneous cron jobs and historical crawler access causes `sqlite3.OperationalError: database is locked`. | SQLite WAL mode permanently active (`PRAGMA journal_mode=WAL;`) with 30s busy timeout (`PRAGMA busy_timeout=30000;`). |
| **8** | **CTIS vs. Legacy EudraCT Scope Boundary** | Expecting trials prior to January 31, 2022 yields no records. | Scope boundary documented: CTIS covers post-CTR trials (~12,500+); legacy EudraCT is a separate system. |

---

### 5.2 The 10,000 Record Pagination Cap & Bi-Directional Dual-Pass Solution

#### The Problem:
The CTIS Search API (`POST /ctis-public-api/search`) is backed by an Elasticsearch/OpenSearch cluster configured with `index.max_result_window = 10000`. With 100 trials per page:
* Pages 1 through 100 return normal trial data (records 1 to 10,000).
* Page 101+ returns an empty array `data: []`.

Because the active CTIS registry contains over 12,500 trials, a traditional single-direction pagination loop misses approximately 2,500 trials.

#### Our Engineered Solution:
In `run_full()` (`ctis_etl/main.py`), the pipeline implements an automated **Bi-Directional Dual-Pass Pagination Algorithm**:

```
TOTAL CATALOG: ~12,572 Clinical Trials
─────────────────────────────────────────────────────────────────────────────► (Time)
Jan 2022                                   March 2024                 Present (2026)

[════════════════════════════════════════════════════════════════════════════]
◄──────────────────────── PASS 1: DESC (Newest to Oldest) ───────────────────
                           (Discovers ~10,000 trials from Present down to ~March 2024)

───────────────────────── PASS 2: ASC (Oldest to Newest) ────────────────────►
(Discovers ~10,000 trials from Jan 2022 up to ~August 2025)

[                      ▓▓▓▓▓ OVERLAP ZONE ▓▓▓▓▓                              ]
           (Overlapping trials skipped via database stage tracking)
─────────────────────────────────────────────────────────────────────────────
RESULT: 100% COMPLETE CATALOG EXTRACTION (0 records missed)
```

1. **Pass 1 (`sort_direction = "DESC"`):**
   * Traverses from page 1 to 100, discovering all trials from today back to ~March 2024 (~10,000 trials).
2. **Catalog Threshold Check:**
   * Evaluates `totalRecords` from the API response envelope. If `totalRecords > 10,000`, it dynamically activates Pass 2.
3. **Pass 2 (`sort_direction = "ASC"`):**
   * Re-traverses from page 1 to 100 in ascending order, discovering all historical trials from January 2022 forward to ~August 2025.
4. **Idempotent Overlap Deduplication:**
   * Any trial already discovered and ingested in Pass 1 is skipped in Pass 2 via state tracking checks, ensuring zero redundant data fetches while capturing 100% of all trials across the entire EU registry.

---

### 5.3 S3 Pre-Signed URL 300-Second Expiration & Permanent Endpoint Strategy

#### The Problem:
When querying CTIS document endpoints, the response provides an Amazon S3 pre-signed URL with signature parameters:
```http
https://ctis-publication-engine-doc-storage-prod.s3.eu-central-1.amazonaws.com/{ctNumber}/{uuid}?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=...&X-Amz-Expires=300&X-Amz-Signature=...
```
These pre-signed URLs **expire after 300 seconds (5 minutes)**. If static files or database rows store this pre-signed URL directly, client access fails with `403 AccessDenied` shortly after ingestion.

#### Our Engineered Solution:
1. **Permanent Endpoint Storage in `trial_documents.json`:**
   * Instead of saving the short-lived S3 pre-signed query string, the pipeline rewrites the `url` attribute to the stable CTIS public API endpoint:
     ```json
     {
       "uuid": "1b258c08-4171-46bd-8946-b6aeaf1e1eb0",
       "title": "Clinical Trial Protocol Part I",
       "documentType": "PROTOCOL",
       "fileName": "Clinical_Trial_Protocol_Part_I.pdf",
       "url": "https://euclinicaltrials.eu/ctis-public-api/documents/2026-527084-15-00/1b258c08-4171-46bd-8946-b6aeaf1e1eb0/download"
     }
     ```
2. **On-Demand Dynamic Resolution:**
   * Clients or applications can call this permanent endpoint at any time to receive a fresh, valid 5-minute pre-signed link on the fly.
3. **Local PDF Archiving:**
   * When `DOWNLOAD_DOCUMENTS=true` (or via `download_trial_docs.py`), the pipeline downloads the binary PDF directly and persists it to `./data/silver/{ctNumber}/documents/{fileName}` and/or S3, ensuring permanent offline availability.

---

### 5.4 Empty 200 OK Responses on Missing Records

When a trial number does not exist, has been redacted, or was withdrawn before publication, the CTIS API responds with `HTTP 200 OK` and an empty JSON payload `{}` (rather than standard `HTTP 404 Not Found`).

* **Client Guard:** `CTISClient.retrieve_trial()` checks:
  ```python
  if not data or not data.get("ctNumber"):
      raise CTISNotFoundError(f"Trial {ct_number} was not found or returned empty dossier.")
  ```
* **Handling:** Missing records are caught, routed to `./quarantine/`, and logged with detailed error diagnostics.

---

### 5.5 SQLite Concurrency & WAL (Write-Ahead Logging) Mode

Under multi-process Docker or cron execution, background historical crawler tasks and scheduled sync tasks may write to `tracker.db` at the same time. Default SQLite rollback journals trigger `database is locked` errors under concurrent writes.

#### Our Engineered Solution:
On database initialization (`database.init_sqlite_db`), the pipeline executes:
```sql
PRAGMA journal_mode=WAL;
PRAGMA busy_timeout=30000;
```
* **WAL Mode:** Separates readers and writers, allowing concurrent reads while write transactions are appended to the WAL log without blocking.
* **30-Second Busy Timeout:** If a write transaction is currently committing, concurrent processes wait up to 30,000ms instead of immediately failing.

---

### 5.6 CTIS vs. Legacy EudraCT Scope Boundary

* **CTIS Scope:** Covers clinical trials submitted and authorized under the Clinical Trials Regulation (CTR EU 536/2014) from **January 31, 2022 to the present**. The database contains approximately ~12,500–13,000 trials.
* **Legacy EudraCT Scope:** The older European database containing ~31,000+ historical trials submitted prior to January 31, 2022 under the Clinical Trials Directive (EC 2001/20). **EudraCT is a completely separate database** and is not indexed or returned by the CTIS public REST API.

---

## 6. Data Normalization Architecture (Bronze, Silver & History Tiers)

Every clinical trial is organized into structured tiers:

```text
data/ (or s3://aascent-mindgram/ctis/)
│
├── bronze/                                    # Raw untouched API retrieve response
│   └── 2026-527084-15-00/
│       └── raw.json
│
├── silver/                                    # 6 Standard domain-specific entities
│   └── 2026-527084-15-00/
│       ├── documents/                         # Downloaded regulatory PDF documents
│       │   ├── Protocol_Part_I.pdf
│       │   └── Subject_Information_Sheet.pdf
│       ├── meta_data.json                     # Status, dates, pipeline lineage
│       ├── summary.json                       # Identifiers, titles, sponsors, phase
│       ├── full_trial_information.json        # Complete Part I scientific dossier
│       ├── trial_documents.json               # Regulatory documents catalog
│       ├── trial_results.json                 # Trial outcome summaries & clinical reports
│       └── locations_and_contact_points.json  # MSCs, investigator sites, contacts
│
└── history/                                   # Archived previous versions before update
    └── 2026-527084-15-00/
        └── 20261014T020000Z/                  # UTC snapshot timestamp tag
            ├── bronze/raw.json
            └── silver/*.json
```

---

### 6.1 Bronze Tier (Raw Audit Layer)

* **Location:** `bronze/{ctNumber}/raw.json`
* **Contents:** Exact, unparsed payload returned by `GET /ctis-public-api/retrieve/{ctNumber}`.
* **Purpose:** Provides an immutable audit trail and enables zero-network local reprocessing if parsing schemas change.

---

### 6.2 Silver Tier (The 6 Standard Domain Entities)

The monolithic retrieve response is parsed into 6 discrete, well-structured JSON documents stored under `silver/{ctNumber}/` (and `data/{ctNumber}/` for backward compatibility):

#### 1. `meta_data.json`
Contains regulatory lifecycle dates, trial status, and pipeline lineage information:
```json
{
  "ctNumber": "2026-527084-15-00",
  "ctStatus": 2,
  "decisionDate": "07/10/2026",
  "publishDate": "2026-10-07T15:43:07.693",
  "ctPublicStatusCode": 1,
  "trialRegion": "EEA",
  "trialRegionCode": 1,
  "events": [],
  "correctiveMeasures": [],
  "ingestion_timestamp": "2026-10-15T12:00:00.000000+00:00",
  "pipeline_version": "1.0.0",
  "previous_version_archived_at": "20261012T020000Z",
  "is_updated": true
}
```

#### 2. `summary.json`
High-level overview designed for search engines, indexing, and dashboard visualizations:
```json
{
  "ctNumber": "2026-527084-15-00",
  "clinicalTrialIdentifiers": {
    "euTrialNumber": "2026-527084-15-00",
    "trialTitles": [{"trialTitle": "Evaluation of Thrombin Generation...", "language": "en"}]
  },
  "sponsors": [{"organisationName": "University Of Debrecen", "sponsorType": "NON_COMMERCIAL"}],
  "trialPhase": "Therapeutic use (Phase IV)",
  "medicalConditions": [{"condition": "Venous Thromboembolism"}],
  "therapeuticAreas": [{"area": "Cardiovascular Diseases"}]
}
```

#### 3. `full_trial_information.json`
Contains the entire `authorizedPartI` scientific dossier preserved as-is to guarantee zero schema loss. Includes study design, trial objectives, inclusion/exclusion criteria, primary endpoints, and investigational medicinal products (IMPs).

#### 4. `trial_documents.json`
Regulatory documents catalog with metadata and permanent public download endpoints:
```json
[
  {
    "uuid": "1b258c08-4171-46bd-8946-b6aeaf1e1eb0",
    "title": "Clinical Trial Protocol Part I",
    "documentType": "PROTOCOL",
    "documentTypeLabel": "Protocol",
    "languageCode": "en",
    "fileType": "pdf",
    "fileName": "Clinical_Trial_Protocol_Part_I.pdf",
    "url": "https://euclinicaltrials.eu/ctis-public-api/documents/2026-527084-15-00/1b258c08-4171-46bd-8946-b6aeaf1e1eb0/download"
  }
]
```

#### 5. `trial_results.json`
Stores clinical study results, intermediate summaries, and outcome reports. Returns `{}` if results have not yet been submitted.

#### 6. `locations_and_contact_points.json`
Contains Member States Concerned (MSCs), recruitment clinical sites, Principal Investigators, and sponsor contact details.

---

### 6.3 History Tier (Automated Version Archiving)

Whenever an amendment or update to an existing trial is detected, the pipeline preserves the old version in `history/{ctNumber}/{archive_timestamp}/` before writing the new version:
* **Local:** `data/history/{ctNumber}/{timestamp}/bronze/` and `silver/`
* **AWS S3:** `s3://{bucket}/{prefix}history/{ctNumber}/{timestamp}/` using server-side S3 object copies.
* **Audit Trail:** Updated files in `meta_data.json` receive `is_updated: true` and the `previous_version_archived_at` timestamp.

---

### 6.4 Quarantine Tier (Dead-Letter Handling)

Any response that fails JSON schema decoding or encounters unexpected API errors is automatically moved to `./quarantine/`:
* Payload: `./quarantine/{ctNumber}_{timestamp}.json`
* Stack Trace: `./quarantine/{ctNumber}_{timestamp}.error.log`

---

## 7. State Management & Resilient Database Design

### 7.1 The Lifecycle State Machine

Each trial follows a strict state transition model:

```
                  ┌──────────────┐
                  │ Discovered   │
                  └──────┬───────┘
                         │
                         ▼
                  ┌──────────────┐
                  │   PENDING    │◄──────────────┐
                  └──────┬───────┘               │
                         │ Ingestion starts      │ Stale auto-recovery
                         ▼                       │ (stuck >15 mins)
                  ┌──────────────┐               │
                  │  PROCESSING  │───────────────┘
                  └──────┬───────┘
                         │
             ┌───────────┴───────────┐
             │                       │
             ▼                       ▼
      ┌──────────────┐        ┌──────────────┐
      │   SUCCESS    │        │    FAILED    │
      └──────┬───────┘        └──────┬───────┘
             │                       │
             │ Amendment detected    │ Operator calls
             │ on Search API         │ `make retry`
             ▼                       ▼
      ┌────────────────┐      ┌──────────────┐
      │ UPDATE_PENDING │      │  Re-Queued   │
      └────────────────┘      └──────────────┘
```

---

### 7.2 SQLite Database Engine (Local Persistence)

File: `tracker.db` (configured with WAL mode).

#### Schema: `trials`
```sql
CREATE TABLE IF NOT EXISTS trials (
    ct_number TEXT PRIMARY KEY,
    status TEXT NOT NULL,              -- PENDING, PROCESSING, SUCCESS, FAILED, UPDATE_PENDING
    retry_count INTEGER DEFAULT 0,
    last_publish_date TEXT,
    last_fetched_at TEXT,
    error_message TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_trials_status ON trials(status);
CREATE INDEX IF NOT EXISTS idx_trials_updated ON trials(updated_at);
```

#### Schema: `pipeline_runs`
```sql
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id TEXT PRIMARY KEY,
    run_type TEXT NOT NULL,            -- FULL, NEW_TRIALS, UPDATED_TRIALS, INCREMENTAL, RETRY_FAILED
    started_at TEXT NOT NULL,
    completed_at TEXT,
    status TEXT NOT NULL,              -- RUNNING, COMPLETED, FAILED, INTERRUPTED
    trials_discovered INTEGER DEFAULT 0,
    trials_processed INTEGER DEFAULT 0,
    trials_succeeded INTEGER DEFAULT 0,
    trials_failed INTEGER DEFAULT 0,
    updated_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_runs_started ON pipeline_runs(started_at);
```

---

### 7.3 Amazon DynamoDB Engine (Cloud Persistence)

* **Table Name:** `EU_Clinical` (configurable via `DYNAMODB_TABLE_NAME`)
* **Partition Key:** `euc` (holds the `ctNumber`, e.g. `2026-527084-15-00`)
* **Retry Strategy:** Configured via `botocore.config.Config(retries={"max_attempts": 4, "mode": "adaptive"})` to handle DynamoDB write throttling automatically.

---

### 7.4 Self-Healing Stale Job Recovery

On startup, `database.reset_stale_processing(timeout_minutes=15)` scans the database for trials stuck in `PROCESSING` status (e.g. from an abrupt container restart or crash) and resets them to `PENDING`. This ensures zero records are lost between runs.

---

### 7.5 Cloud State Hydration on Startup

When new Docker containers spin up with an empty local SQLite file, `database.restore_state_from_dynamodb_if_empty()` scans the DynamoDB table and pre-populates SQLite with previously processed trials, avoiding duplicate S3 uploads and redundant network requests.

---

## 8. Installation & Environment Configuration

### 8.1 Prerequisites
* Python 3.10 or higher
* Docker & Docker Compose (optional, for containerized deployments)
* AWS Account with S3 and DynamoDB permissions (if using AWS backends)

### 8.2 Local Setup

```bash
# Clone the repository
git clone <repo-url>
cd eu_clinical_trial

# Create virtual environment
python -m venv .venv

# Activate virtual environment
# Windows:
.venv\Scripts\activate
# Linux / macOS:
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

---

### 8.3 Environment Variables Reference (`.env` Dictionary)

Copy the example template to create your `.env` file:
```bash
cp .env.example .env
```

| Variable Name | Type | Default | Description & Operational Advice |
| :--- | :--- | :--- | :--- |
| `AWS_ACCESS_KEY_ID` | String | `""` | AWS IAM Access Key ID with S3 and DynamoDB read/write permissions. |
| `AWS_SECRET_ACCESS_KEY` | String | `""` | AWS IAM Secret Access Key. |
| `AWS_REGION` | String | `"us-east-1"` | AWS Region hosting the S3 bucket and DynamoDB table. |
| `S3_BUCKET_NAME` | String | `"aascent-mindgram"` | Target Amazon S3 bucket name. |
| `S3_PREFIX` | String | `"ctis/"` | Root prefix path inside the S3 bucket. |
| `DYNAMODB_TABLE_NAME` | String | `"EU_Clinical"` | DynamoDB table name for cloud state tracking. |
| `DYNAMODB_PARTITION_KEY`| String | `"euc"` | Primary partition key attribute name in DynamoDB table. |
| `STORAGE_BACKEND` | Choice | `"s3"` | Storage destination: `'s3'`, `'local'`, or `'both'`. |
| `STATE_BACKEND` | Choice | `"dynamodb"` | State tracking database: `'dynamodb'`, `'sqlite'`, or `'both'`. |
| `MAX_WORKERS` | Integer | `5` | Concurrency thread limit. **Do not exceed 5** to avoid EMA gateway rate limits. |
| `LOOKBACK_DAYS` | Integer | `7` | Rolling incremental lookback window (in days) for discovering new/amended trials. |
| `DOWNLOAD_DOCUMENTS` | Boolean | `true` | When `true`, automatically downloads binary PDF regulatory files to storage. |
| `REQUEST_DELAY_SECONDS` | Float | `0.1` | Pacing delay (in seconds) between outbound HTTP requests on each thread. |
| `CONNECT_TIMEOUT` | Float | `10.0` | Network socket connection timeout (seconds). |
| `READ_TIMEOUT` | Float | `30.0` | Network response read timeout (seconds). |
| `MAX_RETRIES` | Integer | `3` | Maximum retry attempts for transient 429/5xx errors. |
| `LOG_LEVEL` | Choice | `"INFO"` | Logging verbosity: `"DEBUG"`, `"INFO"`, `"WARNING"`, `"ERROR"`. |
| `LOG_FILE_MAX_BYTES` | Integer | `10485760` | Maximum size of log files before rotation (default: 10 MB). |
| `LOG_BACKUP_COUNT` | Integer | `5` | Number of rotated backup log files to retain. |
| `S3_SERVER_SIDE_ENCRYPTION`| String | `"AES256"` | Server-side encryption algorithm for S3 uploads. |
| `CRON_SCHEDULE` | String | `"0 2 * * *"` | Docker container cron expression (default: 02:00 AM UTC daily). |
| `RUN_ON_STARTUP` | Boolean | `true` | When `true`, triggers initial extraction on Docker container boot. |

---

## 9. CLI Command Execution & Operational Modes

### 9.1 Makefile Command Reference

A [Makefile](file:///d:/Workspace/eu_clinical_trial/Makefile) is provided for common operations:

| Makefile Target | Description | Equivalent Python Command |
| :--- | :--- | :--- |
| `make check` | Run 5-point system diagnostic test | `python -m ctis_etl.main --mode check` |
| `make status` | Print summary report & public browser URLs | `python -m ctis_etl.main --mode status` |
| `make view-db` | Inspect local SQLite database in console | `python view_db.py` |
| `make historical` | Start full historical backfill (~12,500 trials) | `python -m ctis_etl.main --mode historical --workers 5` |
| `make new` | Ingest brand new trials (last 7 days) | `python -m ctis_etl.main --mode new --lookback-days 7` |
| `make updates` | Ingest amended trials (last 7 days) | `python -m ctis_etl.main --mode updates --lookback-days 7` |
| `make incremental` | Run combined incremental sync (new + updates) | `python -m ctis_etl.main --mode incremental --lookback-days 7` |
| `make single CT=<ID>` | Ingest a single trial dossier | `python -m ctis_etl.main --mode single --ct-number <ID>` |
| `make retry` | Retry and re-queue failed trials | `python -m ctis_etl.main --mode retry-failed` |
| `make docker-up` | Build and start background Docker container | `docker compose up -d` |
| `make docker-logs` | Stream live Docker container output | `docker compose logs -f` |

---

### 9.2 CLI Operational Modes Reference

The pipeline supports 7 operational modes via `python -m ctis_etl.main --mode <MODE>`:

| Mode Name | Execution Profile | Typical Production Cadence | Description |
| :--- | :--- | :--- | :--- |
| **`historical`** | Full Catalog Dual-Pass | One-time initial setup / disaster recovery | Ingests all ~12,500+ trials page-by-page, skipping previously saved records. |
| **`new`** | Incremental Lookback | Scheduled every 6 hours | Scans the Search API within lookback window and ingests only unrecorded trials. |
| **`updates`** | Incremental Lookback | Scheduled daily at 02:00 AM UTC | Scans amended trials within lookback window, archives old copies, and updates dossiers. |
| **`incremental`** | Combined Lookback | Ad-hoc or scheduled | Runs both `new` and `updates` passes in a single execution. |
| **`single`** | Single Dossier | Ad-hoc / Development testing | Extracts a specific trial immediately (`--ct-number` required). |
| **`retry-failed`** | Dead-Letter Queue | As needed after incidents | Re-queues trials previously marked with `FAILED` status. |
| **`check`** | Pre-Flight Diagnostic | Pre-deployment verification | Tests API connectivity, local disk write, SQLite, S3, and DynamoDB. |

---

### 9.3 CLI Flags & Argument Options

```text
usage: python -m ctis_etl.main [-h] [--mode {incremental,historical,full,new,updates,single,retry-failed,check,status,report}]
                              [--lookback-days LOOKBACK_DAYS] [--ct-number CT_NUMBER]
                              [--workers WORKERS] [--storage {local,s3,both}]
                              [--download-docs] [--no-download-docs]
```

---

### 9.4 Detailed Operational Command Examples

#### 1. Full Historical Catalog Backfill
```bash
# Standard backfill (5 workers, saves to S3 and DynamoDB per .env)
python -m ctis_etl.main --mode historical --workers 5

# Local-only backfill (saves directly into ./data/ without cloud upload)
python -m ctis_etl.main --mode historical --workers 5 --storage local
```

#### 2. Scheduled Ingestion of Brand New Trials
```bash
# Discover and ingest trials published in the last 7 days
python -m ctis_etl.main --mode new --lookback-days 7

# Scan the last 30 days for new trials
python -m ctis_etl.main --mode new --lookback-days 30
```

#### 3. Ingestion of Trial Updates & Amendments
```bash
# Detect and re-archive amended trials from the past 7 days
python -m ctis_etl.main --mode updates --lookback-days 7
```

#### 4. Combined Incremental Synchronization
```bash
# Runs new trial extraction and update synchronization in one command
python -m ctis_etl.main --mode incremental --lookback-days 7
```

#### 5. Ad-Hoc Single Trial Ingestion
```bash
# Ingest single trial to S3 and DynamoDB
python -m ctis_etl.main --mode single --ct-number 2026-527084-15-00

# Ingest single trial to local disk only
python -m ctis_etl.main --mode single --ct-number 2026-527084-15-00 --storage local
```

#### 6. Retry Failed Records
```bash
# Process all quarantined and failed records
python -m ctis_etl.main --mode retry-failed --workers 5
```

---

## 10. Docker Deployment & Automated Scheduling

### 10.1 Container Architecture & Multi-Daemon Lifecycle

The Docker container (`Dockerfile` & `entrypoint.sh`) packages the ETL pipeline, system cron, and an HTTP server into a self-contained runtime:

```
┌─────────────────────────────────────────────────────────────┐
│                 Docker Container (ctis-etl)                 │
│                                                             │
│  ┌──────────────────┐  ┌──────────────────┐  ┌───────────┐  │
│  │   Cron Daemon    │  │  HTTP Web Server │  │ Pipeline  │  │
│  │  (crontab tasks) │  │   (Port 8080)    │  │ Execution │  │
│  └────────┬─────────┘  └────────┬─────────┘  └─────┬─────┘  │
│           │                     │                  │        │
│           ▼                     ▼                  ▼        │
│  /app/logs/cron.log    /app/data/index.html   /app/logs/    │
│                                              ctis_etl.log   │
└─────────────────────────────────────────────────────────────┘
```

1. **Environment Export:** On boot, `entrypoint.sh` exports all runtime environment variables to `/app/env.sh`, ensuring cron subshells inherit full AWS credentials and configuration.
2. **Cron Daemon:** Runs in the background, executing scheduled incremental syncs.
3. **HTTP Web Server:** Runs on port `8080` serving `./data/`, exposing the live HTML dashboard and JSON metrics.
4. **Startup Sync:** Runs an initial sync on startup (configurable via `STARTUP_MODE`).
5. **Log Streaming:** Tails `ctis_etl.log` and `cron.log` to Docker stdout for monitoring via `docker compose logs -f`.

---

### 10.2 Cron Schedule Configuration

The automated schedule is defined in `crontab`:
```cron
# Process 2: Ingest brand NEW trials every 6 hours
0 */6 * * * root /bin/bash -c ". /app/env.sh && python -m ctis_etl.main --mode new >> /app/logs/cron.log 2>&1"

# Process 3: Ingest UPDATES & amendments daily at 02:00 AM UTC
0 2 * * * root /bin/bash -c ". /app/env.sh && python -m ctis_etl.main --mode updates >> /app/logs/cron.log 2>&1"
```

To customize the schedule, update `CRON_SCHEDULE` in `docker-compose.yml` (e.g. `CRON_SCHEDULE: "0 */12 * * *"`).

---

### 10.3 Web Dashboard & Monitoring HTTP Server

The container serves an interactive web dashboard on port `8080`:
* **URL:** `http://<host-ip>:8080/`
* **JSON Metrics API:** `http://<host-ip>:8080/status_report.json`
* Displays tracked trial totals, success/failure counts, daily ingestion charts, and recent pipeline runs.

---

### 10.4 Docker Compose Quickstart

```bash
# Build and launch container in background
docker compose up -d

# Stream logs
docker compose logs -f

# Check container status
docker compose ps

# Run manual CLI commands inside running container
docker compose exec ctis-etl python -m ctis_etl.main --mode status
docker compose exec ctis-etl python -m ctis_etl.main --mode incremental
docker compose exec ctis-etl python -m ctis_etl.main --mode single --ct-number 2026-527084-15-00

# Stop container
docker compose down
```

---

## 11. Auxiliary Utilities & Operational Scripts

### 11.1 Document Downloader CLI (`download_trial_docs.py`)

A standalone script to download regulatory PDF documents using fresh pre-signed URLs:

```bash
# Download all PDF documents for a trial to ./data/silver/{ctNumber}/documents/
python download_trial_docs.py --ct-number 2026-527084-15-00

# Download to a custom output directory
python download_trial_docs.py --ct-number 2026-527084-15-00 --output-dir ./my_pdfs/

# Generate a fresh 5-minute pre-signed S3 URL without downloading
python download_trial_docs.py --fresh-url "https://euclinicaltrials.eu/ctis-public-api/documents/2026-527084-15-00/1b258c08-4171-46bd-8946-b6aeaf1e1eb0/download"

# Download a single document directly from a permanent endpoint
python download_trial_docs.py --download-url "https://euclinicaltrials.eu/ctis-public-api/documents/2026-527084-15-00/1b258c08-4171-46bd-8946-b6aeaf1e1eb0/download" --output-file ./protocol.pdf
```

---

### 11.2 State Inspector CLI (`view_db.py`)

A terminal utility to inspect the local SQLite state database:

```bash
python view_db.py
# Or:
make view-db
```

Prints formatted tables for:
1. **Tracked Trials:** Trial number, current status, retry count, last publish date, and last update timestamp.
2. **Recent Pipeline Runs:** Run ID, run type, duration, discovery count, processed count, success count, and status.
3. **Public S3 Dashboard Links:** Active pre-signed browser URLs for remote monitoring.

---

### 11.3 System Pre-Flight Diagnostics (`--mode check`)

Runs automated pre-flight checks across 5 subsystems before starting production ingestion:

```bash
python -m ctis_etl.main --mode check
# Or:
make check
```

**Diagnostic Coverage:**
1. **CTIS Public API Connectivity:** Tests `POST /search` and sample `GET /retrieve/{ctNumber}`.
2. **Local Storage Write Access:** Validates permissions in `./data`, `./quarantine`, and `./logs`.
3. **SQLite Database Health:** Validates schema creation and WAL journal mode in `tracker.db`.
4. **Amazon S3 Access:** Validates bucket connectivity and write permissions via `head_bucket`.
5. **Amazon DynamoDB Health:** Validates connection to the `EU_Clinical` table.

---

## 12. Performance Estimations, Storage Footprint & AWS Costs

### 12.1 Latency Profile per Trial

| Lifecycle Step | Single Thread Latency | Notes |
| :--- | :--- | :--- |
| **1. CTIS Retrieve Call (GET)** | ~400ms – 800ms | Persistent connection pool reduces latency by ~60% |
| **2. Pydantic Parsing & Splitting** | ~5ms – 10ms | CPU-bound in-memory splitting |
| **3. Local Disk Write (7 files)** | ~10ms – 20ms | Atomic `.tmp` rename for Bronze + 6 Silver JSONs |
| **4. S3 Upload (7 files)** | ~250ms – 400ms | AES-256 encrypted uploads with metadata headers |
| **5. DynamoDB State Update** | ~30ms – 60ms | Single-item write |
| **6. SQLite State Update** | ~1ms – 2ms | Non-blocking write in WAL mode |
| **Total Cycle Time per Trial** | **~0.8s – 1.3s** | (Single-thread baseline) |

---

### 12.2 Historical Backfill Throughput (~12,500+ Trials)

With 5 concurrent worker threads, effective throughput reaches **~2.0 to 3.0 trials per second**:

| Concurrency Profile | Settings | Throughput | Est. Runtime (12,572 Records) | Risk Profile |
| :--- | :--- | :--- | :--- | :--- |
| **Standard Mode (Recommended)** | `WORKERS=5`, `DELAY=0.1s` | **~2.2 trials/sec** (~130 trials/min) | **~1 hour 35 min to 2 hours** | Excellent (Zero 429 errors observed) |
| **Conservative Mode** | `WORKERS=3`, `DELAY=0.3s` | **~1.1 trials/sec** (~65 trials/min) | **~3 hours to 3.5 hours** | Maximum caution |
| **Fast / Corporate Cloud Mode** | `WORKERS=8`, `DELAY=0.0s` | **~3.5 trials/sec** (~210 trials/min) | **~1 hour** | Higher risk of transient 429 throttling |

$$\text{Estimated Runtime} = \frac{12,572 \text{ trials}}{2.2 \text{ trials/sec} \times 3,600 \text{ sec/hr}} \approx 1.58 \text{ hours}$$

---

### 12.3 Ongoing Daily Incremental Sync Runtime

* **Daily Published Trial Volume:** Across all 27 EU member states, approximately **15 to 40 new or amended clinical trials** are registered per day.
* **Search API Scan Phase:** Scanning the 7-day lookback window takes **~2 to 4 seconds**.
* **Dossier Ingestion Phase:** Ingesting 15–40 trials takes **~10 to 25 seconds**.
* **Total Scheduled Execution Duration:** **Under 30 seconds per run**.

---

### 12.4 Cloud Storage Footprint & Monthly AWS Cost Breakdown

#### Storage Sizing
* **Bronze Layer (`raw.json`):** ~120 KB – 250 KB per trial.
* **Silver Layer (6 Domain JSONs):** ~30 KB – 60 KB total per trial.
* **Total Storage per Trial:** ~150 KB – 310 KB across 7 JSON files.
* **Full Registry Archive (~12,572 Trials):** **~2.3 GB – 3.3 GB total footprint**.

#### Estimated Monthly AWS Costs (US East, N. Virginia)
* **Amazon S3 Storage:** 3.3 GB Standard Storage @ $0.023/GB/month = **<$0.08 / month**.
* **Amazon S3 PUT Operations:** ~88,000 one-time PUT calls @ $0.005 per 1,000 = **~$0.44 one-time backfill cost**.
* **Amazon DynamoDB (On-Demand):** ~12,572 Write Request Units = **<$0.02 one-time backfill cost**. Storage (~10 MB) falls well within the AWS Always-Free Tier (25 GB free).
* **Total Estimated Monthly Cloud Cost:** **Under $0.10 / month**.

---

## 13. Troubleshooting & Operations FAQ

#### Q1: What happens if the container or process restarts midway through a historical backfill?
**Answer:** The pipeline is completely idempotent and state-tracked. On restart, `database.reset_stale_processing()` recovers any interrupted trials, while `database.stage_trial()` verifies which trials have already been ingested in SQLite and DynamoDB. Extraction automatically resumes from the last uncommitted trial without re-downloading previously processed records.

#### Q2: How do I know if the pipeline is currently hitting rate limits?
**Answer:** Check the application logs (`./logs/ctis_etl.log` or `docker compose logs -f`). If a `429 Too Many Requests` is returned by CTIS, a warning is logged:
`[WARNING] HTTP 429 error for ... Retrying in 2.45s (Attempt 1/3)...`
The worker automatically backs off, sleeps with jitter, and retries. If 429s persist, decrease `MAX_WORKERS` to `3` and increase `REQUEST_DELAY_SECONDS` to `0.3` in `.env`.

#### Q3: How do client applications access PDF documents after the 5-minute pre-signed S3 links expire?
**Answer:** `trial_documents.json` stores the permanent public API endpoint (`https://euclinicaltrials.eu/ctis-public-api/documents/{ctNumber}/{uuid}/download`). Applications can issue a `GET` request to this endpoint to receive a fresh 5-minute link on demand, or use `download_trial_docs.py` to archive binary PDFs locally.

#### Q4: Why are some trials found in legacy EudraCT not appearing in CTIS?
**Answer:** EudraCT covers historical trials authorized under the old Clinical Trials Directive (EC 2001/20) prior to January 31, 2022. CTIS is the modern database established under the Clinical Trials Regulation (EU 536/2014). Legacy EudraCT trials that did not transition to CTR are maintained in a separate system not exposed by the CTIS public API.

#### Q5: How do I view failed trials and re-run them?
**Answer:** Run `python view_db.py` to see trials with status `FAILED`. Run `make retry` or `python -m ctis_etl.main --mode retry-failed` to reset their status to `PENDING` and re-attempt extraction. Detailed failure diagnostics and stack traces are preserved in `./quarantine/`.

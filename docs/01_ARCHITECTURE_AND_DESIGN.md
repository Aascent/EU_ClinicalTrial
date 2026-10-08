# EU CTIS Pipeline - Architecture & System Design

**Document ID:** ARCH-001  
**Version:** 1.1 (Production Hardened)  
**Scope:** Architecture, data flow, normalization schemas, and storage abstractions for the EU CTIS Data Pipeline.

---

## 1. Domain Background & Regulatory Context

The European Union Clinical Trials Information System (CTIS) is the single entry point for submitting and managing clinical trial applications across EU/EEA member states under the **Clinical Trials Regulation (Regulation (EU) No 536/2014 - CTR)**.

### CTIS vs. EudraCT Key Distinctions
* **CTIS:** Holds all trials authorized or transitioned after January 31, 2022. It represents the modern unified regulatory framework with approximately ~12,500–13,000 active dossiers.
* **EudraCT:** The legacy database hosting ~31,000+ historical clinical trials submitted prior to CTR. EudraCT is separate and is not indexed or exposed by the CTIS public REST API.

---

## 2. Component Architecture

```
                       ┌──────────────────────────────────────────────┐
                       │     EU CTIS Public REST API                  │
                       │   POST /ctis-public-api/search               │
                       │   GET  /ctis-public-api/retrieve/{ctNumber}  │
                       └──────────────────────┬───────────────────────┘
                                              │
                                              ▼
                                 ┌─────────────────────────┐
                                 │     CTISClient          │
                                 │  - Persistent httpx pool│
                                 │  - Jittered Backoff     │
                                 │  - 1-Indexed Pagination │
                                 │  - Non-existent Handling│
                                 └────────────┬────────────┘
                                              │
                      ┌───────────────────────┴───────────────────────┐
                      ▼                                               ▼
         ┌─────────────────────────┐                     ┌─────────────────────────┐
         │     database.py         │                     │       parser.py         │
         │  - SQLite (WAL Mode)    │                     │  - Type-safe navigation │
         │  - PROCESSING Lock      │                     │  - Zero data-loss Part I│
         │  - Stale Auto-Recovery  │                     │  Splits raw dossier     │
         │  - Amazon DynamoDB      │                     │  into 6 domain entities │
         │  - Run Metrics Log      │                     └────────────┬────────────┘
         └─────────────────────────┘                                  │
                                                                      ▼
                                                         ┌─────────────────────────┐
                                                         │       storage.py        │
                                                         │  - Local Disk (./data/) │
                                                         │  - S3 (AES-256 + Meta)  │
                                                         │  - Atomic .tmp Rename   │
                                                         │  - Quarantine Handler   │
                                                         └─────────────────────────┘
```

---

## 3. Medallion Architecture: Bronze, Silver & Gold Tiers

The pipeline processes each retrieved trial across the three Medallion architecture tiers:

```
┌─────────────────────────────────┐
│     🥉 BRONZE TIER (RAW)        │  raw.json
│  Exact, untouched EMA payload   │  Stored in bronze/{ctNumber}/
└────────────────┬────────────────┘
                 │
                 ▼
┌─────────────────────────────────┐
│    🥈 SILVER TIER (CURATED)     │  6 Domain JSON Entities
│  Normalized domain-specific     │  Stored in silver/{ctNumber}/
│  entity files                   │  (meta_data, summary, full_trial, etc.)
└────────────────┬────────────────┘
                 │
                 ▼
┌─────────────────────────────────┐
│     🥇 GOLD TIER (ANALYTICS)    │  trial_analytics.json
│  Flattened dimensional record   │  Stored in gold/{ctNumber}/
│  for BI, analytics & dashboards │  (Single queryable table row)
└─────────────────────────────────┘
```

### 3.1 Silver Tier: The 6 Domain Entities
The monolithic API response from `GET /retrieve/{ctNumber}` is parsed into 6 discrete, well-structured JSON documents stored under `silver/{ctNumber}/` (and `data/{ctNumber}/` for backward compatibility):

| File Name | Primary Source Fields | Purpose |
| :--- | :--- | :--- |
| **`meta_data.json`** | Root: `ctNumber`, `ctStatus`, `decisionDate`, `publishDate`, `ctPublicStatusCode`, `trialRegion`, `events`, `correctiveMeasures` | Regulatory status, lineage timestamps, pipeline versioning, and lifecycle events. |
| **`summary.json`** | `authorizedPartI.trialDetails.clinicalTrialIdentifiers`, `sponsors`, `trialCategory.trialPhase`, `medicalConditions`, `therapeuticAreas` | High-level overview for fast indexing, search engines, and summary dashboards. |
| **`full_trial_information.json`** | Entire `authorizedApplication.authorizedPartI` object | Deep scientific dossier: protocol design, inclusion/exclusion criteria, objectives, and investigational products. Preserved as-is to guarantee zero schema loss. |
| **`trial_documents.json`** | Root: `documents` list | Attached regulatory documents catalog (titles, UUIDs, languages, document types). |
| **`trial_results.json`** | Root: `results` object | Clinical study results and outcome reports (empty `{}` if not yet submitted). |
| **`locations_and_contact_points.json`** | Merged from `authorizedPartsII` (MSCs and recruitment sites) and `sponsors[*].publicContacts`/`scientificContacts` | Geographic coverage across EU countries, investigator sites, and regulatory contact points. |

### 3.2 Gold Tier: Dimensional Business Analytics (`trial_analytics.json`)
The Gold tier provides an un-nested, flattened tabular record stored under `gold/{ctNumber}/trial_analytics.json`. It combines the most vital fields from all 6 Silver entities into a single, high-performance analytical row ready for ingestion into Snowflake, BigQuery, Parquet, or BI tools:
* **Identifiers & Lineage:** `ct_number`, `full_title`, `status`, `status_code`, `trial_region`, `decision_date`, `publish_date`, `ingestion_timestamp`
* **Trial Classification:** `trial_phase_code`, `trial_phase_label` (e.g. `Phase IV (Therapeutic use)`), `is_low_intervention`
* **Pharmacology:** `active_substances` (deduplicated array), `product_names`, `products_count`
* **Sponsorship:** `sponsors`, `sponsor_types`
* **Geography & Scale:** `participating_countries`, `trial_sites_count`, `total_recruitment_subjects`
* **Clinical Scope:** `medical_conditions`, `therapeutic_areas`, `primary_endpoints`
* **Metrics & Counts:** `inclusion_criteria_count`, `exclusion_criteria_count`, `documents_count`, `has_results`
* **Timeline:** `estimated_start_date`, `estimated_end_date`

---

## 4. Defensive Schema Parsing (`parser.py`)

To ensure that sudden upstream CTIS schema changes, renamed keys, or unexpected nulls never crash the pipeline:
1. **Type-Safe Navigation Helpers (`_safe_dict`, `_safe_list`):** Every nested access verifies object types and falls back to empty `{}` or `[]` without raising `KeyError` or `AttributeError`.
2. **Whole-Object Preservation:** `full_trial_information.json` stores the complete Part I scientific dictionary directly. If EMA introduces new regulatory attributes, they are preserved immediately without requiring code edits.
3. **Pydantic Model Tolerance (`extra = "allow"`):** Upstream attribute additions are accepted transparently.

---

## 5. State Management & Multi-Process Concurrency

State tracking guarantees idempotency, resumability, and collision-free concurrency:

### 5.1 SQLite State Storage (`tracker.db`)
* **WAL Mode (`PRAGMA journal_mode=WAL;`):** Enables non-blocking concurrent reads and serialized writes. The background historical crawler and recurring cron tasks execute simultaneously without hitting `database is locked` errors.
* **Busy Timeout (`PRAGMA busy_timeout=30000;`):** Waits up to 30 seconds for concurrent writes to commit.
* **Enterprise State Machine:**
  * `PENDING`: Discovered, awaiting ingestion.
  * `PROCESSING`: In-flight; locked so concurrent processes or cron runs will not double-process it.
  * `UPDATE_PENDING`: Existing trial whose publication date is newer than the recorded date.
  * `SUCCESS`: Fully extracted, verified, and saved to S3/disk.
  * `FAILED`: Exceeded retry threshold (quarantined).
* **Self-Healing Stale Job Recovery:** On every startup, `database.reset_stale_processing(timeout_minutes=15)` scans for trials stuck in `PROCESSING` (e.g. from container restarts or worker crashes) and resets them to `PENDING`.

### 5.2 Amazon DynamoDB (`EU_Clinical`)
* Primary Partition Key: `euc` (holds `ctNumber`, e.g. `2026-527084-15-00`).
* Configured with adaptive botocore retries (`max_attempts=4, mode="adaptive"`) to prevent write capacity throttling.

---

## 6. Storage Security & Atomic Verification (`storage.py`)

1. **Atomic File Persistence:** Local files are written to `.tmp` files and atomically renamed.
2. **All-or-Nothing Upload Verification:** All 6 target files must succeed and be non-empty (`file_size > 0`). If any file fails to save or upload to S3, the trial is moved to retry/quarantine.
3. **S3 Server-Side Encryption:** Configured with `ServerSideEncryption="AES256"`.
4. **S3 Metadata Headers:** Injects audit tags (`ct-number`, `ingested-at`, `filename`) directly into object metadata for automated governance.
5. **Quarantine Directory (`./quarantine/`):** Malformed payloads or unrecoverable API errors are saved as:
   * `./quarantine/{ctNumber}_{timestamp}.json` (Raw response)
   * `./quarantine/{ctNumber}_{timestamp}.error.log` (Full stack trace)

---

## 7. Networking, Connection Pooling & Lifecycle

1. **Persistent Connection Pool:** Uses `httpx.Client` with HTTP keepalive, HTTP/2, and pooled connections (up to 50 concurrent connections), falling back to `urllib.request` when `httpx` is unavailable.
2. **Graceful Shutdown (`SIGTERM`/`SIGINT`):** When container engines terminate the process, active worker tasks drain cleanly and commit their database records before exiting.
3. **Rotating Log Handlers (`RotatingFileHandler`):** Limits log files to 10 MB with 5 backups to prevent disk exhaustion.

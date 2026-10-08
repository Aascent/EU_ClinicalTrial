# EU CTIS Pipeline - Architecture & System Design

**Document ID:** ARCH-001  
**Version:** 1.0  
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
                                 │  - Jittered Backoff     │
                                 │  - 1-Indexed Pagination │
                                 │  - Non-existent Handling│
                                 └────────────┬────────────┘
                                              │
                      ┌───────────────────────┴───────────────────────┐
                      ▼                                               ▼
         ┌─────────────────────────┐                     ┌─────────────────────────┐
         │     database.py         │                     │       parser.py         │
         │  - SQLite (WAL Mode)    │                     │  Splits raw dossier     │
         │  - Amazon DynamoDB      │                     │  into 6 domain entities │
         │  - Run Metrics Log      │                     └────────────┬────────────┘
         └─────────────────────────┘                                  │
                                                                      ▼
                                                         ┌─────────────────────────┐
                                                         │       storage.py        │
                                                         │  - Local Disk (./data/) │
                                                         │  - Amazon S3 Bucket     │
                                                         │  - Quarantine Handler   │
                                                         └─────────────────────────┘
```

---

## 3. Data Normalization: The 6 Output Entities

The monolithic API response from `GET /retrieve/{ctNumber}` is parsed into 6 discrete, well-structured JSON documents stored under `./data/{ctNumber}/` (or `s3://{bucket}/{prefix}{ctNumber}/`):

| File Name | Primary Source Fields | Purpose |
| :--- | :--- | :--- |
| **`meta_data.json`** | Root: `ctNumber`, `ctStatus`, `decisionDate`, `publishDate`, `ctPublicStatusCode`, `trialRegion`, `events`, `correctiveMeasures` | Regulatory status, lineage timestamps, pipeline versioning, and lifecycle events. |
| **`summary.json`** | `authorizedPartI.trialDetails.clinicalTrialIdentifiers`, `sponsors`, `trialCategory.trialPhase`, `medicalConditions`, `therapeuticAreas` | High-level overview for fast indexing, search engines, and summary dashboards. |
| **`full_trial_information.json`** | Entire `authorizedApplication.authorizedPartI` object | Deep scientific dossier: protocol design, inclusion/exclusion criteria, objectives, and investigational products. |
| **`trial_documents.json`** | Root: `documents` list | Attached regulatory documents catalog (titles, UUIDs, languages, document types). |
| **`trial_results.json`** | Root: `results` object | Clinical study results and outcome reports (empty `{}` if not yet submitted). |
| **`locations_and_contact_points.json`** | Merged from `authorizedPartsII` (MSCs and recruitment sites) and `sponsors[*].publicContacts`/`scientificContacts` | Geographic coverage across EU countries, investigator sites, and regulatory contact points. |

---

## 4. State Management & Multi-Process Concurrency

State tracking guarantees idempotency, resumability, and avoids duplicate downloads:

### 4.1 SQLite State Storage (`tracker.db`)
* **WAL Mode (`PRAGMA journal_mode=WAL;`):** Enables non-blocking concurrent reads and serialized writes. The background historical crawler and recurring cron tasks can execute simultaneously without hitting `database is locked` errors.
* **Trial States:**
  * `PENDING`: New trial discovered, awaiting ingestion.
  * `UPDATE_PENDING`: Existing trial whose publication date is newer than the recorded date.
  * `SUCCESS`: Ingested, validated, and persisted.
  * `FAILED`: Exceeded retry threshold (quarantined).

### 4.2 Amazon DynamoDB (`EU_Clinical`)
* Primary Partition Key: `euc` (holds `ctNumber`, e.g. `2026-527084-15-00`).
* Stores synchronization metadata, publication timestamps, and retry audit logs.

---

## 5. Fault Tolerance & Quarantine Strategy

1. **Atomic File Persistence:** Local files are written to `.tmp` files and atomically renamed to prevent partial writes.
2. **All-or-Nothing Upload Verification:** All 6 target files must succeed before a trial is marked as `SUCCESS`. If any file fails to save or upload to S3, the trial is moved to retry/quarantine.
3. **Quarantine Directory (`./quarantine/`):** Malformed payloads or unrecoverable API errors are saved as:
   * `./quarantine/{ctNumber}_{timestamp}.json` (Raw response)
   * `./quarantine/{ctNumber}_{timestamp}.error.log` (Full stack trace)

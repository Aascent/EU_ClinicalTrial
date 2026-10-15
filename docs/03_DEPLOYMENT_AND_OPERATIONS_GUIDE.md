# EU CTIS Pipeline - Deployment & Operations Guide

**Document ID:** OPS-001  
**Version:** 1.1 (Production Hardened)  
**Scope:** Production containerization, AWS setup, scheduled operations, monitoring, and runbooks.

---

## 1. Environment Configuration Reference

The pipeline reads configuration from `.env` or system environment variables:

| Variable | Default Value | Description |
| :--- | :--- | :--- |
| `AWS_ACCESS_KEY_ID` | Required for AWS | AWS access key for S3 and DynamoDB. |
| `AWS_SECRET_ACCESS_KEY` | Required for AWS | AWS secret access key. |
| `AWS_REGION` | `us-east-1` | AWS region where resources reside (N. Virginia). |
| `S3_BUCKET_NAME` | `aascent-mindgram` | Destination Amazon S3 bucket name. |
| `S3_PREFIX` | `ctis/` | Root prefix inside S3 bucket. |
| `DYNAMODB_TABLE_NAME` | `EU_Clinical` | DynamoDB table name for state tracking. |
| `DYNAMODB_PARTITION_KEY`| `euc` | Attribute name of the primary partition key. |
| `CTIS_API_BASE_URL` | `https://euclinicaltrials.eu/ctis-public-api` | Base URL of the CTIS public REST API. |
| `STORAGE_BACKEND` | `s3` | Where to write JSON files: `s3`, `local`, or `both`. |
| `STATE_BACKEND` | `dynamodb` | Where to track state: `dynamodb`, `sqlite`, or `both`. |
| `MAX_WORKERS` | `5` | Concurrency thread limit for retrieve requests. |
| `LOOKBACK_DAYS` | `7` | Default search lookback window for incremental runs. |
| `LOG_LEVEL` | `INFO` | Logging verbosity (`DEBUG`, `INFO`, `WARNING`, `ERROR`). |
| `RUN_ON_STARTUP` | `true` | In Docker: trigger immediate sync on container boot. |
| `STARTUP_MODE` | `historical` | Mode executed on container startup (`historical`, `incremental`, `new`). |
| `CRON_SCHEDULE` | `0 2 * * *` | Cron schedule expression. |

---

## 2. AWS Setup & IAM Permissions

Ensure the IAM User or Role has the following minimum IAM policy permissions:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "S3Access",
      "Effect": "Allow",
      "Action": [
        "s3:PutObject",
        "s3:GetObject",
        "s3:ListBucket",
        "s3:HeadBucket"
      ],
      "Resource": [
        "arn:aws:s3:::aascent-mindgram",
        "arn:aws:s3:::aascent-mindgram/*"
      ]
    },
    {
      "Sid": "DynamoDBAccess",
      "Effect": "Allow",
      "Action": [
        "dynamodb:PutItem",
        "dynamodb:GetItem",
        "dynamodb:UpdateItem",
        "dynamodb:DescribeTable"
      ],
      "Resource": "arn:aws:dynamodb:us-east-1:*:table/EU_Clinical"
    }
  ]
}
```

> [!TIP]
> **DynamoDB Billing Mode:** Ensure table `EU_Clinical` uses **On-Demand (Pay-per-request)** capacity mode during the initial 12,500 historical backfill to avoid throughput throttling.

---

## 3. Docker Deployment & Orchestration

The pipeline is containerized using multi-process coordination:

### 3.1 Quick Start
```bash
# 1. Build and start in detached background mode
docker compose up -d

# 2. Monitor live streaming logs
docker compose logs -f

# 3. Check container status
docker compose ps
```

### 3.2 Automated Scheduling (Crontab)
The container starts `cron` on boot:
* **Every 6 hours (`0 */6 * * *`):** Runs `Process 2` (`--mode new`) to capture new trial approvals.
* **Daily at 02:00 AM UTC (`0 2 * * *`):** Runs `Process 3` (`--mode updates`) to capture trial amendments.
* **On Container Startup:** Runs `Process 1` (`--mode historical`) in the background to backfill the full catalog.

### 3.3 Running Ad-Hoc Commands in Container
```bash
# Trigger immediate diagnostic check
docker compose exec ctis-etl python -m ctis_etl.main --mode check

# Ingest single trial
docker compose exec ctis-etl python -m ctis_etl.main --mode single --ct-number 2026-527084-15-00

# Retry failed trials
docker compose exec ctis-etl python -m ctis_etl.main --mode retry-failed
```

---

## 4. Operational Runbook

### Runbook 1: Pre-Flight Health Check
Before launching backfills or deployments:
```bash
python -m ctis_etl.main --mode check
```
Verifies CTIS API, S3 bucket permissions, DynamoDB table connectivity, and local disk write access.

### Runbook 2: Quarantine Triage
When an unrecoverable schema change or API issue occurs:
1. Check the quarantine directory:
   ```bash
   ls -la ./quarantine/
   ```
2. Read the corresponding error log:
   ```bash
   cat ./quarantine/{ctNumber}_{timestamp}.error.log
   ```
3. Inspect raw JSON: `./quarantine/{ctNumber}_{timestamp}.json`.
4. After resolving code or upstream data issue, re-process with:
   ```bash
   python -m ctis_etl.main --mode retry-failed
   ```

### Runbook 3: Disaster Recovery & State Backup
To back up the local tracking database:
```bash
# Safely snapshot SQLite with WAL mode enabled
sqlite3 tracker.db ".backup tracker_backup.db"
```

### Runbook 4: Stale In-Flight Job Recovery
If a node unexpectedly terminates while processing trials, jobs may remain marked as `PROCESSING`:
* The pipeline automatically self-heals by running `reset_stale_processing(timeout_minutes=15)` at the beginning of each run.
* Any job stuck in `PROCESSING` longer than 15 minutes is automatically rolled back to `PENDING` so it will be retried without manual intervention.

### Runbook 5: Log Rotation & Maintenance
* Execution logs are written to `logs/etl.log` using `RotatingFileHandler`.
* Maximum size is set to **10 MB** with **5 backup files** (`etl.log.1` through `etl.log.5`), preventing disk bloat during 24/7 continuous operations.
* Console stdout logs are mirrored to Docker daemon logging drivers (e.g. `json-file` with `max-size: "100m"`).

### Runbook 6: Graceful Process Termination
When initiating container restarts or node maintenance:
* Docker sends `SIGTERM`, which is trapped by `ctis_etl/main.py`.
* Active workers complete their current trial write and commit the database transaction before stopping.
* No partial or corrupted files are left in storage because all writes use `.tmp` atomic renames and all S3 uploads are verified before committing.


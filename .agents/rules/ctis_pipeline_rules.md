---
description: Pipeline operational rules, error handling policies, and testing guidelines
globs: ctis_etl/**, docs/**, Dockerfile, entrypoint.sh, crontab
---

# CTIS Pipeline Engineering Rules

1. **Code Modification Rules:**
   - Always run pre-flight check (`python -m ctis_etl.main --mode check`) or module import test after modifying any Python file.
   - When writing to storage, verify `all(save_results.values())` rather than `any()`.
   - Never remove or mutate existing fields in `parse_trial_dossier` without updating `docs/01_ARCHITECTURE_AND_DESIGN.md`.

2. **API & Networking Safety:**
   - Use `re.sub(r"^[A-Za-z]{2,3}:\s*", "", date_str)` when parsing search dates that may have country code prefixes (e.g., `HU: 07/10/2026`).
   - Keep `MAX_WORKERS <= 5` to respect EMA CTIS API bandwidth limits.
   - Respect exponential backoff with jitter on HTTP 429 and 5xx responses.

3. **Storage & Database Policies:**
   - In SQLite, keep WAL mode enabled with `PRAGMA busy_timeout=15000` to prevent database locks.
   - Quarantine malformed or failed payloads into `./quarantine/{ctNumber}_{timestamp}.json` with error logs.
   - Keep `.env` strictly ignored by Git (`.gitignore`).

4. **Storage Architecture Consistency:**
   - Persist Bronze (`raw.json`) and Silver (6 domain JSON files) across storage backends without injecting internal layer tags into the JSON output.


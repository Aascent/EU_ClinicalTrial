# ==============================================================================
# EU CTIS Clinical Trial Pipeline - Automation Makefile
# ==============================================================================

# Detect Python interpreter (prefers active or local virtual environment)
ifeq ($(OS),Windows_NT)
    PYTHON ?= .venv\Scripts\python.exe
    ifeq ($(wildcard $(PYTHON)),)
        PYTHON = python
    endif
else
    PYTHON ?= .venv/bin/python
    ifeq ($(wildcard $(PYTHON)),)
        PYTHON = python3
    endif
endif

# Default variables
DAYS ?= 7
WORKERS ?= 5
CT ?= 2026-527084-15-00

.PHONY: help check status incremental new updates historical single retry view-db docker-up docker-down docker-logs clean

# Default target when running just `make`
help:
	@echo "=============================================================================="
	@echo "               EU CTIS Clinical Trial Pipeline - Make Commands                "
	@echo "=============================================================================="
	@echo "  make check              Run pre-flight diagnostic check (5/5 tests)"
	@echo "  make status             Display status & publish browser URLs (S3/JSON/HTML)"
	@echo "  make view-db            Inspect local SQLite tracker.db in formatted table"
	@echo ""
	@echo "  make incremental        Run combined incremental sync (new + updates, 7 days)"
	@echo "  make new                Run new trials sync only (default: DAYS=7)"
	@echo "  make updates            Run updated trials sync only (default: DAYS=7)"
	@echo "  make historical         Run full catalog historical backfill (WORKERS=5)"
	@echo "  make single CT=<ID>     Process a single trial by ID (e.g. make single CT=2026-527084-15-00)"
	@echo "  make retry              Re-queue and retry failed trials"
	@echo ""
	@echo "  make docker-up          Start background daemon container with cron"
	@echo "  make docker-down        Stop docker daemon container"
	@echo "  make docker-logs        Tail live logs from docker container"
	@echo "  make clean              Remove temporary python cache files"
	@echo "=============================================================================="

# Pre-flight diagnostic check
check:
	$(PYTHON) -m ctis_etl.main --mode check

# Live Status & Dashboard generation
status:
	$(PYTHON) -m ctis_etl.main --mode status

# Pretty view of SQLite tracker.db
view-db:
	$(PYTHON) view_db.py

# Incremental Sync (Default 7-day lookback)
incremental:
	$(PYTHON) -m ctis_etl.main --mode incremental --lookback-days $(DAYS) --workers $(WORKERS)

# Ingest brand new trials published recently
new:
	$(PYTHON) -m ctis_etl.main --mode new --lookback-days $(DAYS) --workers $(WORKERS)

# Ingest updates and amendments to existing trials
updates:
	$(PYTHON) -m ctis_etl.main --mode updates --lookback-days $(DAYS) --workers $(WORKERS)

# Historical full catalog backfill
historical:
	$(PYTHON) -m ctis_etl.main --mode historical --workers $(WORKERS)

# Process a single specific trial ID
single:
	$(PYTHON) -m ctis_etl.main --mode single --ct-number $(CT)

# Retry failed trials queue
retry:
	$(PYTHON) -m ctis_etl.main --mode retry-failed --workers $(WORKERS)

# Docker management shortcuts
docker-up:
	docker compose up -d

docker-down:
	docker compose down

docker-logs:
	docker compose logs -f

# Clean up Python __pycache__ and scratch files
clean:
	@powershell -Command "Get-ChildItem -Recurse -Filter '__pycache__' | Remove-Item -Recurse -Force -ErrorAction SilentlyContinue"
	@echo "Cleaned __pycache__ directories."

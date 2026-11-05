# ==============================================================================
# Clinical Trial Multi-Container Platform - Makefile
# ==============================================================================

.PHONY: help build up down restart ps logs logs-eu logs-anz status check sync-eu sync-anz report

help:
	@echo "Available commands:"
	@echo "  make build      - Build or rebuild Docker images for all services"
	@echo "  make up         - Start all containers in detached mode"
	@echo "  make down       - Stop and remove all containers"
	@echo "  make restart    - Restart all containers"
	@echo "  make ps         - Show status of running containers"
	@echo "  make logs       - Tail logs from all containers"
	@echo "  make logs-eu    - Tail logs from the EU CTIS container"
	@echo "  make logs-anz   - Tail logs from the ANZCTR container"
	@echo "  make status     - Check pipeline status for both services"
	@echo "  make check      - Pre-flight connection checks for both services"
	@echo "  make sync-eu    - Trigger incremental sync on EU CTIS"
	@echo "  make sync-anz   - Trigger incremental sync on ANZCTR"
	@echo "  make report     - Refresh and publish live web dashboards"

build:
	docker-compose build

up:
	docker-compose up -d

down:
	docker-compose down

restart:
	docker-compose restart

ps:
	docker-compose ps

logs:
	docker-compose logs -f

logs-eu:
	docker-compose logs -f eu_clinical_trial

logs-anz:
	docker-compose logs -f anz_clinical_trial

status:
	@echo "=== EU CTIS Status ==="
	@docker-compose exec eu_clinical_trial python -m ctis_etl.main --mode status || true
	@echo ""
	@echo "=== ANZCTR Status ==="
	@docker-compose exec anz_clinical_trial python -m anz_etl.main --mode status || true

check:
	@echo "=== EU CTIS Check ==="
	@docker-compose exec eu_clinical_trial python -m ctis_etl.main --mode check || true
	@echo ""
	@echo "=== ANZCTR Check ==="
	@docker-compose exec anz_clinical_trial python -m anz_etl.main --mode check || true

sync-eu:
	docker-compose exec eu_clinical_trial python -m ctis_etl.main --mode incremental

sync-anz:
	docker-compose exec anz_clinical_trial python -m anz_etl.main --mode incremental

report:
	@docker-compose exec eu_clinical_trial python -m ctis_etl.main --mode report || true
	@docker-compose exec anz_clinical_trial python -m anz_etl.main --mode report || true


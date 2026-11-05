# ==============================================================================
# Clinical Trial Multi-Container Platform - Makefile
# ==============================================================================

.PHONY: help build up down restart ps logs logs-eu logs-anz status

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


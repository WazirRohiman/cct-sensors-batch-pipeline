.PHONY: help setup lint test data-dirs airflow-up airflow-down airflow-reset fix-permissions

help:
	@echo "Targets: setup | lint | test | data-dirs | airflow-up | airflow-down | airflow-reset | fix-permissions"
	@echo "Services: Airflow (8080) | Jupyter (8888)"
	@echo "DuckDB: Access via Jupyter notebooks at http://localhost:8888"
	@echo "Permissions: Run 'make fix-permissions' if Jupyter can't access DuckDB files"

setup:
	uv venv .venv && . .venv/bin/activate && uv pip install -r requirements.txt
	pre-commit install
	$(MAKE) data-dirs

lint:
	ruff check .
	black --check .

test:
	@echo "Add tests later"

data-dirs:
	mkdir -p data/raw data/quarantine data/staged data/normalised data/duckdb data/logs notebooks progress_reports

airflow-up:
	cd docker/airflow && cp -n .env.example .env || true && \
		docker compose up -d postgres && \
		docker compose up -d airflow-init && \
		docker compose up -d airflow-webserver airflow-scheduler airflow-triggerer viewer

airflow-down:
	cd docker/airflow && docker compose down

airflow-reset:
	cd docker/airflow && docker compose down -v && docker compose up -d postgres && docker compose up -d airflow-init && docker compose up -d airflow-webserver airflow-scheduler airflow-triggerer viewer

fix-permissions:
	@echo "🔧 Fixing DuckDB permissions for Jupyter access..."
	docker exec -u root airflow-viewer-1 chown -R 1000:100 /home/jovyan/work/data/duckdb/ 2>/dev/null || \
		(echo "⚠️  Viewer container not running, fixing host permissions..." && \
		sudo chown -R 1000:1000 data/duckdb/ 2>/dev/null || \
		./scripts/fix_duckdb_permissions.sh)
	@echo "✅ DuckDB permissions fixed! Jupyter can now access the database."


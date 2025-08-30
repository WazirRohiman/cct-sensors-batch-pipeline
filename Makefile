.PHONY: help setup lint test data-dirs airflow-up airflow-down airflow-reset db-init

help:
	@echo "Targets: setup | lint | test | data-dirs | airflow-up | airflow-down | airflow-reset | db-init"
	@echo "Services: Airflow (8080) | Jupyter (8888)"
	@echo "DuckDB: Access via Jupyter notebooks at http://localhost:8888"

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
	mkdir -p data/raw data/quarantine data/staged data/duckdb data/logs notebooks progress_reports

airflow-up:
	cd docker/airflow && cp -n .env.example .env || true && \
		docker compose up -d postgres && \
		docker compose up -d airflow-init && \
		docker compose run --rm duckdb-init && \
		docker compose up -d airflow-webserver airflow-scheduler airflow-triggerer viewer

airflow-down:
	cd docker/airflow && docker compose down

airflow-reset:
	cd docker/airflow && docker compose down -v && docker compose up -d postgres && docker compose up -d airflow-init && docker compose run --rm duckdb-init && docker compose up -d airflow-webserver airflow-scheduler airflow-triggerer viewer

db-init:
	cd docker/airflow && docker compose run --rm duckdb-init


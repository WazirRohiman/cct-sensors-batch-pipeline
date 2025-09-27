.PHONY: help setup lint test data-dirs setup-env airflow-up airflow-down airflow-build airflow-reset fix-permissions

help:
	@echo "Targets: setup | lint | test | data-dirs | setup-env | airflow-up | airflow-down | airflow-build | airflow-reset | fix-permissions"
	@echo "Services: Airflow (8080) | Jupyter (8888)"
	@echo "DuckDB: Access via Jupyter notebooks at http://localhost:8888"
	@echo "Permissions: Run 'make fix-permissions' if Jupyter can't access DuckDB files"

setup:
	python3 -m venv .venv
	. .venv/bin/activate && pip install -r requirements.txt
	. .venv/bin/activate && pre-commit install
	$(MAKE) data-dirs
	$(MAKE) setup-env

lint:
	ruff check .
	black --check .

test:
	@echo "Add tests later"

data-dirs:
	mkdir -p data/raw data/quarantine data/staged data/normalised data/duckdb data/logs notebooks progress_reports

setup-env:
	@echo "🔧 Setting up Airflow environment..."
	@if [ ! -f docker/airflow/.env ]; then \
		echo "📄 Creating .env from template..."; \
		cp docker/airflow/.env.example docker/airflow/.env; \
		echo "🔑 Generating Airflow secret key..."; \
		SECRET_KEY=$$(openssl rand -hex 32); \
		sed -i.bak "s/<insert-your-secret-key-here>/$$SECRET_KEY/" docker/airflow/.env && rm docker/airflow/.env.bak; \
		echo "✅ Environment configured with secret key"; \
	else \
		echo "✅ .env file already exists"; \
	fi

airflow-up:
	cd docker/airflow && \
		docker compose build && \
		docker compose up -d postgres && \
		docker compose up -d airflow-init && \
		docker compose up -d airflow-webserver airflow-scheduler airflow-triggerer viewer

airflow-down:
	cd docker/airflow && docker compose down

airflow-build:
	cd docker/airflow && docker compose build --no-cache

airflow-reset:
	cd docker/airflow && docker compose down -v && docker compose build && docker compose up -d postgres && docker compose up -d airflow-init && docker compose up -d airflow-webserver airflow-scheduler airflow-triggerer viewer

fix-permissions:
	@echo "🔧 Fixing DuckDB permissions for Jupyter access..."
	docker compose --project-directory docker/airflow -f docker/airflow/docker-compose.yml exec -T --user root viewer chown -R 1000:100 /home/jovyan/work/data/duckdb/ 2>/dev/null || \
		(echo "⚠️  Viewer container not running, fixing host permissions..." && \
		sudo chown -R 1000:1000 data/duckdb/ 2>/dev/null || \
		./scripts/fix_duckdb_permissions.sh)
	@echo "✅ DuckDB permissions fixed! Jupyter can now access the database."


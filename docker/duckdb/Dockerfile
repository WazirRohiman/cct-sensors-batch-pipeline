FROM python:3.11-slim

RUN pip install --no-cache-dir duckdb>=1.2.1

WORKDIR /app

ENV PYTHONUNBUFFERED=1
# expects DUCKDB_PATH and DATA_ROOT to be provided at runtime
# src directory will be mounted at runtime
ENTRYPOINT ["python", "-m", "src.pipelines.db_init"]


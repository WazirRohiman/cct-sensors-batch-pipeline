FROM apache/airflow:2.10.2-python3.11

COPY --chown=airflow:root requirements.txt /tmp/requirements.txt

USER airflow

RUN pip install --no-cache-dir -r /tmp/requirements.txt \
    && rm /tmp/requirements.txt

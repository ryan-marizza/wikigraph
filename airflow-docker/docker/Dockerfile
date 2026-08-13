#Match the Airflow version your existing compose file uses.
#Check with docker compose exec airflow-scheduler airflow version
ARG AIRFLOW_BASE=apache/airflow:3.3.0
FROM ${AIRFLOW_BASE}

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
    libxml2-dev libxslt1-dev \
    && rm -rf /var/lib/apt/lists/*

USER airflow

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
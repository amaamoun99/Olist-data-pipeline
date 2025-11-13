# syntax=docker/dockerfile:1
FROM apache/airflow:2.8.1-python3.11

# 👑 Become root only to install system packages
USER root

RUN mkdir -p /var/lib/apt/lists/partial && \
    apt-get update && \
    apt-get install -y --no-install-recommends \
        build-essential \
        git \
        libpq-dev && \
    rm -rf /var/lib/apt/lists/*

# 👤 Switch back to airflow user before running pip (required!)
USER airflow

# 🐍 Install dbt and airflow-dbt safely
RUN pip install --no-cache-dir --upgrade pip setuptools wheel && \
    pip install --no-cache-dir \
        dbt-core==1.9.2 \
        dbt-bigquery==1.9.2 \
        airflow-dbt==0.4.0 \
        google-auth==2.29.0



# ✅ Ensure folders exist
RUN mkdir -p /opt/airflow/dags /opt/airflow/logs /opt/airflow/plugins

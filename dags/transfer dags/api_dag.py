from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from datetime import datetime, timedelta
import requests
import pandas as pd
import os

# ----------------------------
# CONFIGURATION
# ----------------------------
ENDPOINTS = ["sellers", "payments"]  # endpoints to fetch
GCS_BUCKET = "abdelrahman_project_bucket"
BIGQUERY_DATASET = "ready-de27.abdelrahman_olist_landing"
TEMP_DIR = "/tmp/api_data"  # local temp dir for CSVs

# Email and retry configuration
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["your_email@gmail.com"],  # 👈 replace with your address
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=0),
    "start_date": datetime(2024, 1, 1),
}

# ----------------------------
# DAG DEFINITION
# ----------------------------
with DAG(
    "api_csv_to_bigquery_dag",
    default_args=default_args,
    description="Fetch CSV API data → Upload to GCS → Load into BigQuery",
    schedule_interval=None,  # runs every day at midnight
    catchup=False,
    tags=["api", "csv", "gcs", "bigquery, transfer, abdelrahman"],
) as dag:

    # Ensure local directory exists
    os.makedirs(TEMP_DIR, exist_ok=True)

    # ----------------------------
    # STEP 1: Fetch CSV from API
    # ----------------------------
    def fetch_csv_from_api(endpoint):
        """Fetch CSV file from API endpoint and save locally."""
        url = f"https://{endpoint}-table-924114739925.europe-west1.run.app"
        print(f"📡 Fetching CSV from {url} ...")
        response = requests.get(url, timeout=(30, 300))  # (connect timeout, read timeout)

        if response.status_code != 200:
            raise ValueError(f"❌ Failed to fetch {endpoint}: {response.status_code}")

        # Save the CSV
        csv_path = f"{TEMP_DIR}/{endpoint}.csv"
        with open(csv_path, "wb") as f:
            f.write(response.content)

        # Validate CSV contents
        try:
            df = pd.read_csv(csv_path)
        except Exception as e:
            raise ValueError(f"❌ Failed to parse CSV for {endpoint}: {e}")

        if df.empty:
            raise ValueError(f"⚠️ Empty CSV for {endpoint}")
        print(f"✅ {endpoint} → {len(df)} rows, {len(df.columns)} columns")

        return csv_path

    # ----------------------------
    # TASKS PER ENDPOINT
    # ----------------------------
    for endpoint in ENDPOINTS:
        local_file = f"{TEMP_DIR}/{endpoint}.csv"
        gcs_object = f"api_data/{endpoint}.csv"
        bigquery_table = f"{BIGQUERY_DATASET}.{endpoint}"

        # 1️⃣ Fetch CSV from API
        fetch_task = PythonOperator(
            task_id=f"fetch_{endpoint}_csv",
            python_callable=fetch_csv_from_api,
            op_args=[endpoint],
            execution_timeout=timedelta(minutes=10),
        )

        # 2️⃣ Upload CSV to GCS
        upload_to_gcs = LocalFilesystemToGCSOperator(
            task_id=f"upload_{endpoint}_to_gcs",
            src=local_file,
            dst=gcs_object,
            bucket=GCS_BUCKET,
            mime_type="text/csv",
            execution_timeout=timedelta(minutes=10),
        )

        # 3️⃣ Load into BigQuery
        load_to_bigquery = GCSToBigQueryOperator(
            task_id=f"load_{endpoint}_to_bigquery",
            bucket=GCS_BUCKET,
            source_objects=[gcs_object],
            destination_project_dataset_table=bigquery_table,
            source_format="CSV",
            skip_leading_rows=1,
            write_disposition="WRITE_TRUNCATE",  # full load, replace existing data
            create_disposition="CREATE_IF_NEEDED",
            autodetect=True,
            execution_timeout=timedelta(minutes=15),
        )

        # Chain: Fetch → Upload → Load
        fetch_task >> upload_to_gcs >> load_to_bigquery

from airflow import DAG
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from datetime import datetime

# ---- CONFIG ----
POSTGRES_CONN_ID = "orders_conn"
GCS_BUCKET = "postgres-to-gcs-bucket"
BIGQUERY_DATASET = "ready-de27.public"

# List of table names to transfer
TABLES = ["orders", "order_items", "order_reviews", "products", "product_category_name_translation"]  # ← add more tables here


default_args = {
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
}

with DAG(
    "orders_postgres_to_bigquery_dynamic_transfer",
    default_args=default_args,
    description="Transfer multiple PostgreSQL tables to BigQuery via GCS",
    schedule_interval=None,
    catchup=False,
) as dag:

    for table in TABLES:
        gcs_filename = f"olist_order_data/{table}.json"
        bigquery_table = f"{BIGQUERY_DATASET}.{table}"

        # Step 1: Export from Postgres → GCS
        export_to_gcs = PostgresToGCSOperator(
            task_id=f"export_{table}_to_gcs",
            postgres_conn_id=POSTGRES_CONN_ID,
            sql=f"SELECT * FROM {table}",
            bucket=GCS_BUCKET,
            filename=gcs_filename,
            export_format="json",
        )

        # Step 2: Load from GCS → BigQuery
        load_to_bq = GCSToBigQueryOperator(
            task_id=f"load_{table}_to_bigquery",
            bucket=GCS_BUCKET,
            source_objects=[gcs_filename],
            destination_project_dataset_table=bigquery_table,
            source_format="NEWLINE_DELIMITED_JSON",
            write_disposition="WRITE_TRUNCATE",
            create_disposition="CREATE_IF_NEEDED",
        )

        export_to_gcs >> load_to_bq

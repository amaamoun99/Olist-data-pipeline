from airflow import DAG
from datetime import datetime
from airflow_dbt.operators.dbt_operator import DbtRunOperator, DbtTestOperator, DbtDepsOperator

# Directory inside Airflow container where dbt is located 
DBT_PROJECT_DIR = "/opt/airflow/dbt"

with DAG(
    dag_id="dbt_run_dag",
    description="Run dbt models and tests using DbtRunOperator",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["dbt", "bigquery"]
) as dag:


    #  Run DBT
    dbt_run = DbtRunOperator(
        task_id="dbt_run",
        dir=DBT_PROJECT_DIR,
        profiles_dir=DBT_PROJECT_DIR,
    )

    dbt_run

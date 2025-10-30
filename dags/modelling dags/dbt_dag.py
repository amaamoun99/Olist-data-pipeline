from airflow import DAG
from datetime import datetime
from airflow_dbt.operators.dbt_operator import DbtRunOperator, DbtTestOperator, DbtDepsOperator

# Directory inside your Airflow container or VM where dbt project lives
DBT_PROJECT_DIR = "/opt/airflow/dbt"

with DAG(
    dag_id="dbt_run_dag",
    description="Run dbt models and tests using DbtRunOperator",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["dbt", "bigquery"]
) as dag:

    # # Step 1 - Install dependencies
    # dbt_deps = DbtDepsOperator(
    #     task_id="dbt_deps",
    #     dir=DBT_PROJECT_DIR,
    #     profiles_dir=DBT_PROJECT_DIR,
    # )

    # Step 2 - Run all models
    dbt_run = DbtRunOperator(
        task_id="dbt_run",
        dir=DBT_PROJECT_DIR,
        profiles_dir=DBT_PROJECT_DIR,
    )

    # Step 3 - Test models
    # dbt_test = DbtTestOperator(
    #     task_id="dbt_test",
    #     dir=DBT_PROJECT_DIR,
    #     profiles_dir=DBT_PROJECT_DIR,
    # )

    # dbt_deps >> dbt_run >> dbt_test
    dbt_run

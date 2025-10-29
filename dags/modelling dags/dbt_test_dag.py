from airflow import DAG
from airflow.utils.dates import days_ago
from airflow_dbt.operators.dbt_operator import DbtRunOperator

default_args = {"owner": "airflow", "start_date": days_ago(1)}

with DAG(
    dag_id="dbt_test_dag",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    dbt_run = DbtRunOperator(
        task_id="dbt_run",
        dir="/opt/airflow/dbt",
        profiles_dir="/opt/airflow/dbt",
    )

    dbt_run
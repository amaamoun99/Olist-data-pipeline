from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.dummy import DummyOperator

# --------------------------------------------------------------------
# CONFIGURATION
# --------------------------------------------------------------------
TRANSFER_DAGS = [
    "api_csv_to_bigquery_dag",
    "customers_postgres_to_bigquery_dynamic_transfer",
    "orders_postgres_to_bigquery_dynamic_transfer",
]

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2024, 1, 1),
}

# --------------------------------------------------------------------
# MASTER DAG DEFINITION
# --------------------------------------------------------------------
with DAG(
    dag_id="transfer_master_dag",
    description="Master DAG that triggers all Postgres→GCS→BigQuery transfer DAGs in parallel",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["master", "transfer", "bigquery"],
) as dag:

    trigger_tasks = []
    wait_tasks = []

    # Loop through each sub-DAG
    for transfer_dag_id in TRANSFER_DAGS:

        # Task 1: Trigger the sub-DAG
        trigger_task = TriggerDagRunOperator(
            task_id=f"trigger_{transfer_dag_id}",
            trigger_dag_id=transfer_dag_id,
            reset_dag_run=True,
            wait_for_completion=False,
        )

        # Task 2: Wait until the sub-DAG finishes
        wait_task = ExternalTaskSensor(
            task_id=f"wait_{transfer_dag_id}",
            external_dag_id=transfer_dag_id,
            external_task_id=None,
            execution_date_fn=lambda dt: dt,  # 🩵 Key fix line
            poke_interval=30,
            timeout=60 * 60,
            mode="reschedule",
        )

        # Chain trigger → wait
        trigger_task >> wait_task

        trigger_tasks.append(trigger_task)
        wait_tasks.append(wait_task)

    # Final join step
    finish = DummyOperator(task_id="all_transfers_finished")

    wait_tasks >> finish

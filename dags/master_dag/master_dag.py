from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup
import logging

# --------------------------------------------------------------------
# CONFIGURATION
# --------------------------------------------------------------------
TRANSFER_DAGS = [
  "api_csv_to_bigquery_dag",
  "customers_postgres_to_bigquery_dynamic_transfer",
  "orders_postgres_to_bigquery_dynamic_transfer",
]

DBT_DAG = "dbt_run_dag"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2024, 1, 1),
}

# --------------------------------------------------------------------
# MASTER DAG DEFINITION
# --------------------------------------------------------------------
with DAG(
    dag_id="master_data_pipeline_dag",
    description="Master DAG that orchestrates Postgres→GCS→BigQuery transfers followed by dbt transformations.",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["master", "transfer", "dbt", "bigquery"],
) as dag:

    start = DummyOperator(task_id="start_pipeline")

    # ----------------------------------------------------------------
    # TRANSFER PHASE: Trigger and wait for each transfer DAG
    # ----------------------------------------------------------------
    with TaskGroup("transfer_phase", tooltip="Trigger all transfer DAGs") as transfer_phase:

        for dag_id in TRANSFER_DAGS:
            trigger = TriggerDagRunOperator(
                task_id=f"trigger_{dag_id}",
                trigger_dag_id=dag_id,
                reset_dag_run=True,
                wait_for_completion=True,
                conf={"triggered_by": "master_data_pipeline_dag"},
                on_success_callback=lambda context: logging.info(f"{dag_id} triggered successfully."),
                on_failure_callback=lambda context: logging.error(f"Failed to trigger {dag_id}."),
            )

            # wait = ExternalTaskSensor(
            #     task_id=f"wait_for_{dag_id}_completion",
            #     external_dag_id=dag_id,
            #     external_task_id=None,  # Wait for entire DAG
            #     execution_date_fn=lambda dt: dt,
            #     poke_interval=10,       # check every 1 min
            #     timeout=60 * 60,        # timeout after 1 hour
            #     mode="reschedule",
            #     soft_fail=False,
            #     doc_md=f"""
            #     #### ExternalTaskSensor
            #     Waits for the DAG `{dag_id}` to complete before continuing.
            #     """,
            # )

            trigger 

    # ----------------------------------------------------------------
    # DBT PHASE: Trigger and wait for the modeling DAG
    # ----------------------------------------------------------------
    with TaskGroup("dbt_phase", tooltip="Trigger and wait for dbt modeling") as dbt_phase:

        trigger_dbt = TriggerDagRunOperator(
            task_id="trigger_dbt_run",
            trigger_dag_id=DBT_DAG,
            reset_dag_run=True,
            wait_for_completion=True,
            execution_date="{{ execution_date }}",   # 👈 align logical time
            conf={"triggered_by": "master_data_pipeline_dag"},
        )

        # wait_dbt = ExternalTaskSensor(
        #     task_id="wait_for_dbt_completion",
        #     external_dag_id=DBT_DAG,
        #     external_task_id=None,
        #     execution_date_fn=lambda dt: dt,
        #     poke_interval=10,
        #     timeout=60 * 60,
        #     mode="reschedule",
        #     doc_md="Waits for the dbt_run_dag to finish all transformations.",
        # )

        trigger_dbt 

    finish = DummyOperator(task_id="end_pipeline")

    # ----------------------------------------------------------------
    # SEQUENCE
    # ----------------------------------------------------------------
    start >> transfer_phase >> dbt_phase >> finish

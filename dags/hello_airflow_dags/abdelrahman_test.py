# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from datetime import datetime
# import logging

# def error_task():
#     logging.error("This is an error message from the error_task.")
#     logging.info("This is an info message from the error_task.")
#     logging.warning("This is a warning message from the error_task.")
#     logging.debug("This is a debug message from the error_task.")
#     logging.critical("This is a critical message from the error_task.") 

# with DAG(
#     dag_id="example_airflow_dag_abdelrahman",
#     schedule=None,
#     start_date=datetime(2025, 5, 15),
#     catchup=False,
#     tags=["example"],
# ) as dag:
#     error_task = PythonOperator(
#         task_id="error_task",
#         python_callable=error_task,
#     )

#     error_task
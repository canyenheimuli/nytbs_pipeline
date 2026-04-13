# Packages
from __future__ import annotations
from datetime import datetime
import json

from airflow import DAG
from airflow.operators.python import PythonOperator

# ETL functions
from src.extract import extract
from src.validate import validate
from src.transform import transform
from src.load import load

# Task wrapper functions
def run_extract(**context):
    data = extract()
    context["ti"].xcom_push(key = "raw_data", value = data)

def run_validate(**context):
    raw_data = context["ti"].xcom_pull(key = "raw_data", task_ids = "extract_task")
    validated_data = validate(raw_data)
    context["ti"].xcom_push(key = "validated_data", value = validated_data)

def run_transform(**context):
    validated_data = context["ti"].xcom_pull(key = "validated_data", task_ids = "validate_task")
    transformed_data = transform(validated_data)
    context["ti"].xcom_push(key = "transformed_data", value = transformed_data)

def run_load(**context):
    transformed_data = context["ti"].xcom_pull(key = "transformed_data", task_ids = "transform_task")
    load(transformed_data)

# DAG definition
with DAG(
    dag_id = "etl_pipeline",
    start_date = datetime(2026, 1, 1),
    schedule = None, # TO-DO: EITHER Update to Friday CRON (0 0 * * 5) when testing is done OR keep as "None" if letting GH Actions do the scheduling
    catchup = False,
    tags = ["etl"],
) as dag:

    extract_task = PythonOperator(
        task_id = "extract_task",
        python_callable = run_extract,
		retries = 3,
    	retry_delay=timedelta(seconds = 10),
    	retry_exponential_backoff = True
    )

    validate_task = PythonOperator(
        task_id = "validate_task",
        python_callable = run_validate,
    )

    transform_task = PythonOperator(
        task_id = "transform_task",
        python_callable = run_transform,
    )

    load_task = PythonOperator(
        task_id = "load_task",
        python_callable = run_load,
		retries = 3,
    	retry_delay=timedelta(seconds = 10),
    	retry_exponential_backoff = True
    )

    # Pipeline
    extract_task >> validate_task >> transform_task >> load_task

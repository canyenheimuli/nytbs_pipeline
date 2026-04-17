# Packages
from __future__ import annotations
from datetime import datetime, timedelta
import json
import pandas as pd

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

# ETL functions
from etl.extract import extract
from etl.validate import validate
from etl.transform import transform
from etl.load import load

# Task wrapper functions
def run_extract(**context):
    raw_data = extract()
    context["ti"].xcom_push(key = "raw_data", value = raw_data.to_json())

def run_validate(**context):
    raw_data = context["ti"].xcom_pull(key = "raw_data", task_ids = "extract_task")
    validated_data = validate(pd.read_json(raw_data))
    context["ti"].xcom_push(key="validated_data", value = validated_data.to_json())

def run_transform(**context):
    validated_data = context["ti"].xcom_pull(key = "validated_data", task_ids = "validate_task")
    transformed_data = transform(pd.read_json(validated_data))

	# Serialize each DataFrame in the dictionary individually
    serialized = {key: df.to_json() for key, df in transformed_data.items()}
    context["ti"].xcom_push(key = "transformed_data", value = json.dumps(serialized))

def run_load(**context):
    transformed_data = context["ti"].xcom_pull(key = "transformed_data", task_ids = "transform_task")
    
    # Deserialize back to a dictionary of DataFrames
    deserialized = {key: pd.read_json(df_json) for key, df_json in json.loads(transformed_data).items()}
    load(deserialized)

# DAG definition
with DAG(
    dag_id = "etl_pipeline",
    start_date = datetime(2026, 1, 1),
    schedule = None,
    catchup = False,
    tags = ["etl"],
) as dag:

    extract_task = PythonOperator(
        task_id = "extract_task",
        python_callable = run_extract,
		retries = 3,
    	retry_delay = timedelta(seconds = 10),
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
    	retry_delay = timedelta(seconds = 10),
    	retry_exponential_backoff = True
    )

    # Pipeline
    extract_task >> validate_task >> transform_task >> load_task

# Packages
from __future__ import annotations
from datetime import datetime, timedelta
import json
import pandas as pd
import io
import pickle
import base64

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

# Custom ETL functions
from etl.extract import extract
from etl.validate import validate
from etl.transform import transform
from etl.load import load

# Base64/Pickle serialization and de-serialization helper functions
def serialize_df(df):
    return base64.b64encode(pickle.dumps(df)).decode("utf-8")

def deserialize_df(data):
    return pickle.loads(base64.b64decode(data.encode("utf-8")))

def serialize_dict_of_dfs(d):
    return json.dumps({key: serialize_df(df) for key, df in d.items()})

def deserialize_dict_of_dfs(data):
    return {key: deserialize_df(val) for key, val in json.loads(data).items()}

# Task wrapper functions
def run_extract(**context):
    raw_data = extract()
    context["ti"].xcom_push(key = "raw_data", value = serialize_df(raw_data))

def run_validate(**context):
    raw_data = deserialize_df(context["ti"].xcom_pull(key = "raw_data", task_ids = "extract_task"))
    validated_data = validate(raw_data)
    context["ti"].xcom_push(key = "validated_data", value = serialize_df(validated_data))

def run_transform(**context):
    validated_data = deserialize_df(context["ti"].xcom_pull(key = "validated_data", task_ids = "validate_task"))
    transformed_data = transform(validated_data)
    context["ti"].xcom_push(key = "transformed_data", value = serialize_dict_of_dfs(transformed_data))

def run_load(**context):
    transformed_data = deserialize_dict_of_dfs(context["ti"].xcom_pull(key = "transformed_data", task_ids = "transform_task"))
    load(transformed_data)

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

import os
import json
import pandas as pd
from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from sqlalchemy import create_engine
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime
from airflow.models.param import Param
import requests

# Default arguments for the DAG
default_args = {
    'retries': 1,
}

@dag(
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description="DAG to extract data from CSV/JSON and load into SQLite using config parameters",
    params={
        "url": Param("https://example.com/data.csv", description="URL of the data source"),
        "file_type": Param("csv", description="File type, either 'csv' or 'json'"),
        "table_name": Param("table_name", description="The table name to use in SQLite")
    }
)
def extract_data_dag():

    # Start task
    start_task = EmptyOperator(task_id="start_task")

    # Branching task to choose between CSV and JSON extraction
    def choose_extract_task(**kwargs):
        file_type = kwargs['params']['file_type']
        if file_type == 'csv':
            return 'extract_csv'
        elif file_type == 'json':
            return 'extract_json'
        else:
            raise ValueError(f"Unsupported file type: {file_type}")

    choose_extract_task = BranchPythonOperator(
        task_id='choose_extract_task',
        python_callable=choose_extract_task,
        provide_context=True
    )

    # Extract task for CSV
    @task()
    def extract_csv(url: str, table_name: str):
        df = pd.read_csv(url)
        parquet_file = f"data/{table_name}.parquet"
        df.to_parquet(parquet_file, index=False)
        return parquet_file

    # Extract task for JSON
    @task()
    def extract_json(url: str, table_name: str):
        response = requests.get(url)
        data = response.json()

        # Directly normalize the JSON structure into a flat table
        df = pd.json_normalize(data)
        parquet_file = f"data/{table_name}.parquet"
        df.to_parquet(parquet_file, index=False)
        return parquet_file

    # Load data to SQLite
    @task(trigger_rule=TriggerRule.ONE_SUCCESS)
    def load_to_sqlite(parquet_file: str, table_name: str):
        engine = create_engine(f"sqlite:///data/{table_name}.db")
        df = pd.read_parquet(parquet_file)
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        return f"Data loaded into SQLite DB: {table_name}"

    # End task
    end_task = EmptyOperator(task_id="end_task")

    # Task flow
    start_task >> choose_extract_task \
        >> [extract_csv("{{ params['url'] }}", "{{ params['table_name'] }}"),
            extract_json("{{ params['url'] }}", "{{ params['table_name'] }}")] \
        >> load_to_sqlite("{{ ti.xcom_pull(task_ids='extract_csv', key='return_value') or ti.xcom_pull(task_ids='extract_json', key='return_value') }}",
                          "{{ params['table_name'] }}") \
        >> end_task

# Instantiate the DAG
extract_data_dag = extract_data_dag()

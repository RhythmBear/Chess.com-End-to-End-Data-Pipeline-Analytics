from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.decorators import dag, task
import duckdb
from scripts.python_scripts import *
from datetime import datetime
from pendulum import duration

default_args = {
    'retries': 1,
    "retry_delay": duration(seconds=10)
                }


with DAG(
    "pull_data_from_chess_api",
    start_date=datetime(2023, 1, 1), 
    description='A Dag that extracts data from a chess API and loads it into the azure storage account',    
    default_args=default_args,
    tags=['chess-etl'], 
    schedule='@monthly', 
    catchup=True, 
    max_active_runs=1
    ) as dag:


    # Start Workflow
    start = DummyOperator(task_id='start_workflow')

    # Extract and load the Data from Chess API
    extract_data_from_chess_api = PythonOperator(
        task_id='extract_data_from_chess_api',
        python_callable=extract_and_load_chess_data,
        op_kwargs={
            'username': 'rhythmbear1',
            'year': "{{ execution_date.year }}",  # Pass the year extracted from execution_date
            'month': "{{ execution_date.month }}"  # Pass the month extracted from execution_date
        },
    )

    # End Workflow

    # Transflorm the json data into a fact table using the python operator
    tranform_json_to_fact_table = PythonOperator(
        task_id='transform_json_to_fact_table',
        python_callable=transform_json_to_fact_table,
        op_kwargs={
            'year': "{{ execution_date.year }}",  # Pass the year extracted from execution_date
            'month': "{{ execution_date.month }}"  # Pass the month extracted from execution_date
        },
    )


    end = DummyOperator(task_id='end_workflow')
    
    start >> extract_data_from_chess_api >> tranform_json_to_fact_table >> end

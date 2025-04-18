from datetime import datetime

from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from dotenv import load_dotenv
from pendulum import duration
from scripts.datasets import my_fact_file
from scripts.python_scripts import *

from airflow import DAG

load_dotenv()

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
                }
                
    )

    # Transflorm the json data into a fact table using the python operator
    transform_json_to_fact_table_ = PythonOperator(
        task_id='transform_json_to_fact_table',
        python_callable=transform_json_to_fact_table,
        op_kwargs={
            'year': "{{ execution_date.year }}",  # Pass the year extracted from execution_date
            'month': "{{ execution_date.month }}"  # Pass the month extracted from execution_date
                }
    )

    load_dim_openings_ = PythonOperator(
        task_id = 'load_dim_openings',
        python_callable=load_dim_openings,
        op_kwargs={
            'container_name': "/chess-etl-files",
            'dim_destination': "gold/dim_openings.parquet"
                }
    )

    load_dim_date_ = PythonOperator(
        task_id = 'load_dim_date',
        python_callable=load_dim_date,
        op_kwargs={
            'container_name': "/chess-etl-files",
            'dim_destination': "gold/dim_date.parquet"
                }
    )

    load_dim_time_control_ = PythonOperator(
        task_id = 'load_dim_time_control',
        python_callable=load_dim_time_control,
        op_kwargs={
            'container_name': "/chess-etl-files",
            'dim_destination': "gold/dim_time_control.parquet"
                    }
    )

    load_dim_results_ = PythonOperator(
        task_id = 'load_dim_results',
        python_callable=load_dim_results,
        op_kwargs= {
            'container_name': "/chess-etl-files",
            'dim_destination': "gold/dim_results.parquet"
                }
    )

    load_fact_table_ = PythonOperator(
        task_id = 'load_fact_table',
        python_callable = load_fact_table,
        op_kwargs={
            'container_name': "/chess-etl-files",
            'fact_table_destination': "gold/fact_table.parquet",
            'exec_date': "{{ execution_date }}"
                },
        outlets=[my_fact_file]
    )

    # End Workflow
    end = DummyOperator(task_id='end_workflow')
    
    start >> extract_data_from_chess_api >> transform_json_to_fact_table_ >> [load_dim_openings_, load_dim_date_, load_dim_time_control_, load_dim_results_]  
    [load_dim_openings_, load_dim_date_, load_dim_time_control_, load_dim_results_] >> load_fact_table_  >> end


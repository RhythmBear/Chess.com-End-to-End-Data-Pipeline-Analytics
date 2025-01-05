from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.decorators import dag, task
from include.python_scripts import extract_and_load_chess_data
from datetime import datetime



with DAG(
    "pull_data_from_chess_api",
    start_date=datetime(2023, 1, 1), 
    description='A Dag that extracts data from a chess API and loads it into the azure storage account',    
    default_args={'retries': 2},
    tags=['chess-etl'], schedule='@weekly', catchup=False
    ) as dag:

    # Start Workflow
    start = DummyOperator(task_id='start_workflow')

    # Extract and load the Data from Chess API
    extract_data_from_chess_api = PythonOperator(
        task_id='extract_data_from_chess_api',
        python_callable=extract_and_load_chess_data,
        op_args=['rhythmbear1', 2024, 1]
    )

    # End Workflow
    end = DummyOperator(task_id='end_workflow')
    
    start >> extract_data_from_chess_api >> end

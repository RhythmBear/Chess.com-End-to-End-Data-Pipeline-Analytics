from datetime import datetime

from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from pendulum import duration
from scripts.datasets import my_fact_file
from scripts.python_scripts import *

from airflow import DAG

AZURE_CHESS_DW_ID = "azure_chess_dw"

default_args = {
    'retries': 1,
    "retry_delay": duration(seconds=10)
                }

with DAG(
    "load_data_warehouse",
    start_date=datetime(2023, 1, 1), 
    description='A Dag that collects data from the Gold staging area and loads it into the data warehouse',    
    default_args=default_args,
    tags=['chess-etl'], 
    schedule=my_fact_file, 
    catchup=False, 
    max_active_runs=1
    ) as dag:
    
    create_data_warehouse = PostgresOperator(
        task_id = 'create_data_warehouse',
        postgres_conn_id = AZURE_CHESS_DW_ID,
        sql="sql/create_datawarehouse.sql"
    )

    get_last_updated_date = PostgresOperator(
        task_id = 'get_last_updated_date',
        postgres_conn_id = AZURE_CHESS_DW_ID,
        sql="sql/get_last_updated_date.sql",
        do_xcom_push=True  # Ensure this is enabled to push the result
    )

    load_dim_dates_dw = PythonOperator(
        task_id='load_dim_dates_dw',
        python_callable=load_dim_table_to_postgres,
        op_kwargs={
            'dim_file_name': "gold/dim_date.parquet",
            'table_name': "dim_dates"
        }
    )

    load_dim_openings_dw = PythonOperator(
        task_id='load_dim_openings_dw',
        python_callable=load_dim_table_to_postgres,
        op_kwargs={
            'dim_file_name': "gold/dim_openings.parquet",
            'table_name': "dim_openings"
        }
    )

    load_dim_results_dw = PythonOperator(
        task_id='load_dim_results_dw',
        python_callable=load_dim_table_to_postgres,
        op_kwargs={
            'dim_file_name': "gold/dim_results.parquet",
            'table_name': "dim_results"
        }
    )

    load_dim_time_control_dw = PythonOperator(
        task_id='load_dim_time_control_dw',
        python_callable=load_dim_table_to_postgres,
        op_kwargs={
            'dim_file_name': "gold/dim_time_control.parquet",
            'table_name': "dim_time_control"
        }
    )

    load_fact_table_to_postgres = PythonOperator(
        task_id = 'load_fact_table_to_postgres',
        python_callable=load_fact_to_postgres
    )

    create_data_warehouse >> get_last_updated_date >> [load_dim_dates_dw, load_dim_openings_dw, load_dim_results_dw, load_dim_time_control_dw] >> load_fact_table_to_postgres
    [load_dim_dates_dw, load_dim_openings_dw, load_dim_results_dw, load_dim_time_control_dw] >> load_fact_table_to_postgres
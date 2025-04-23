from airflow.datasets import Dataset

my_fact_file = Dataset("az://rbchesssa.blob.core.windows.net/chess-etl-files/gold/fact-games.parquet")
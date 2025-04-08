# Chess.com End to End ETL Project

## **1.0 Project Overview**

This project aims to build an ETL pipeline to analyze my personal chess games and provide insights into the effectiveness of various chess openings. The data will be visualized on a live dashboard, enabling continuous tracking and analysis of performance over time. This article documents the preparation phase of the project.

### **1.1 Goals and Objectives**

- Analyze chess games(battles) across different time periods to determine:
    - Which openings(strategies & variations) have yielded the highest win rates.
    - Openings & Variations that frequently lead to losses.
    - Trends in performance over time based on opening.
- Generate Videos Of Good Games using PGN file and automate upload to Social Media
- Utilize StockFish API to analyze Game PGN FIles and Generate puzzles for missed wins and Focrced Checkmates
- Provide insights through a live dashboard.

---

## 2.0 Architecture & Workflow
![Pipeline Architecture](Project_Files/Chess_ETL_architecture_.gif)

### Data Flow
- Airflow pulls data from Chess.com API and stages it in the bronze layer of the data lake
- Duckdb Transforms and cleans data in bronze layer and then stores it in Silver layer
- Finally Duckdb Performs final aggregations and stores data in the dimensinoal model format in the gold layer. 
- Airflow loads data from the Gold layer into Data Warehouse.
- PowerBI is used to build dashboards and reports   
### 🛠 Tools & Technologies

| Category | Tools & Technologies Used |
| --- | --- |
| **Orchestration** | Apache Airflow |
| **Data Lake & Warehouse** | Azure Blob Storage, PostgreSQL |
| **Processing Engine** | DuckDB |
| **ETL Development** | Python, SQL |
| **Dashboarding** | Power BI |
| **Infrastructure** | Docker, Azure Services |



## 3.0 Data Sources & Storage

### **Data Sources:**

- **Chess API**: Fetches game records

### **Storage Structure:**

| Layer | Description |
| --- | --- |
| **Bronze** | Raw chess data (JSON) |
| **Silver** | Cleaned and transformed data (Parquet) |
| **Gold** | Data for fact and dimensional tables (PostgreSQL) |


## 4.0  Dimensional Data Model
![Pipeline Architecture](Project_Files/data_model.png)


## 5.0  **Approach & Pipeline Workflow (Airflow DAGS)**

### **Workflow Steps:**

1. **Extract**: Fetch chess game data from API and load unto Data Lake
2. **Transform**: Clean, structure, and 
3. **Load**: Store data into PostgreSQL.
4. **Visualization**: Power BI dashboard for insights

### DAG 1 
![Dag 2 Image](Project_Files/DAG_1.png)
This First Dag 
- Pulls Data from the CHESS.com website 
- Loads the Data into the bronze layer in the data lake in it's raw Json Format
- Once the data arrives, The data is transformed and cleaned using DuckDB using SQL Queries alongside User Defined Functions in the dags/scripts/python_scripts.py folder and then the data is loaded back into the Silver Layer of the Datalake
- The data is then finally converted to suit the data warehouse schema using the Fact table and dimensional table displayed earlier and then loaded into the Gold Layer of the datalake.


### DAG 2
![Dag 2 Image](Project_Files/DAG_2.png)
- This Final DAG is linked via the airflow dataset feature using the fact_table in the Gold layer. 
- Immediately the Fact table in the Gold layer is created, The DAG is created. 
- It First Created the Datawarehouse schema in the Postgres database if it does not already exists.
- Then it loads the data from the dim and fact tables in the gold layer into the postgres database.

## 6.0 Folder Directory
```bash  
├── Data # Directory containing sample data used to test writing DAGS
│   ├── 2024-01
│   └── opening
├── Dockerfile
├── Project_Files # Folder for storing images displayed in readme
│   ├── Chess_ETL_architecture.mp4
│   ├── Chess_ETL_architecture_.gif
│   ├── DAG_1.png
│   ├── DAG_2.png
│   └── data_model.png
├── README.md
├── __pycache__
│   └── utils.cpython-310.pyc
├── airflow_settings.yaml
├── config
├── dags
│   ├── __pycache__
│   ├── collect_chess_data_dag.py # Dag that pulls data from Chess.com API and stores in Data lake
│   ├── load_data_warehouse_dag.py # Dag that loads data into the datwarehouse
│   ├── notebooks # Notebooks used to create and test functions
│   ├── scripts # Folder that holds all the functions used in each dag folder
│   └── sql # SQL queries used in DAG folder
├── docker-compose.yaml
├── include
── logs
│   ├── dag_id=load_data_warehouse
│   ├── dag_id=pull_data_from_chess_api
│   ├── dag_processor_manager
│   ├── dbt.log
│   └── scheduler
├── openings.csv
├── plugins
├── requirements.txt
├── structure.txt
├── tests
│   └── dags
└── venv
    ├── bin
    ├── etc
    ├── include
    ├── lib
    ├── lib64 -> lib
    ├── pyvenv.cfg
    └── share

28 directories, 16 files

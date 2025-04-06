from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from tempfile import NamedTemporaryFile
import duckdb
from duckdb import DuckDBPyRelation
import requests
import json
from datetime import datetime, date
import pyarrow.parquet as pq
import pandas as pd
from dotenv import load_dotenv
import re
from selectolax.parser import HTMLParser
import os


# Load environment variables from .env file
load_dotenv()


# Define Variables
storage_account_name = 'rbchesssa'
container_name = 'chess-etl-files'
az_hook = WasbHook(wasb_conn_id='azure_chess_storage') # Using the Storage Account Connection to Azure.
psql_hook = PostgresHook(postgres_conn_id='azure_chess_dw')



def extract_and_load_chess_data(username: str, year: int, month: int) -> list:
    """
    Fetch chess game data for a specific user and month from Chess.com API.
    :param username: chess.com username
    :param year: Year of the games
    :param month: Month of the games (1-12)
    :return: List of games in JSON format

    """

    # The headers are used to mimic a browser request because it returns a 403 error if it detects that it's a bot
    headers = {
         "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0 Safari/537.36"
        }
    
    print(f"Attempting to fetch data for Year: {year} and Month: {month}, formatted as {int(month):02}")

    url = f"https://api.chess.com/pub/player/{username}/games/{year}/{int(month):02}"
    print(f"Attemping to Fetch Data from: {url}")
    response = requests.get(url, headers=headers)

    pulled_data = []
    
    if response.status_code == 200:
        data = response.json()
        pulled_data = data.get("games", [])
        print(f"Successfully Fetched Data From CHess.com API, Records Pulled: {len(pulled_data)}")
    else:
        print(f"Failed to fetch data: {response.status_code}")
        pulled_data = []

    # Upload the data to Azure Storage
    with NamedTemporaryFile("w") as temp:
        print("Attempting to Upload Data to Azure Storage")
        json.dump(pulled_data, temp)
        temp.flush()
        az_hook = WasbHook(wasb_conn_id='azure_chess_storage')

        blobname = f"bronze/{year}-{int(month):02}-games.json"
        az_hook.load_file(
            file_path=temp.name,
            container_name=container_name,
            blob_name=blobname,
            overwrite=True
        )    

        print(f"Successfully Uploaded Data to Azure Storage as {blobname}")
        return True


# Here I'm defining extra duckdb Functions to transform fact table data.



def extract_opening_data(url):
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/115.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "en-US,en;q=0.9",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Referer": "https://www.google.com",
    }

    try:
        response = requests.get(url, headers=headers, timeout=5)
        if response.status_code == 200:
            tree = HTMLParser(response.text)
            
            # Extract H1 (Opening Name)
            h1_tag = tree.css_first("h1")
            opening_name = h1_tag.text(strip=True) if h1_tag else None
            
            # Extract Moves List
            move_list_div = tree.css_first(".openings-view-move-list")
            moves = move_list_div.text(strip=True) if move_list_div else None

            return {"opening_name": opening_name, "moves": moves}

    except requests.RequestException:
        return {"opening_name": None, "moves": None}  # Handle errors

    return {"opening_name": None, "moves": None}


def add_move_numbers(pgn: list) -> str:
    # THis Function adds the Move number to the beginnig of every move sequence. This is important for when we want to load the pgn into an LIchess
 
    # Remove any existing move numbers and split into individual moves
    moves = pgn
    
    # Reconstruct the PGN with move numbers
    formatted_pgn = []
    move_number = 1
    for i in range(0, len(pgn), 2):
        # Add the move number and the two moves (white and black)
        formatted_pgn.append(f"{move_number}. {moves[i]} {moves[i+1] if i+1 < len(moves) else ''}")
        move_number += 1
    
    # Join the formatted moves into a single string
    return ' '.join(formatted_pgn)

# duckdb.remove_function('add_move_numbers')
# duckdb.create_function('add_move_numbers', add_move_numbers)

def get_opening_family(opening_name: str) -> str:
    # Get the parent name of the move numbers

    if ":" in opening_name:
        name_splitted = opening_name.split(":")
        return name_splitted[0]

    else:
        return opening_name
    
    # duckdb.create_function('get_opening_family', get_opening_family)

def get_pgn_depth(pgn: str) -> int:
    # this function is supposed to define the number of moves from a pgn file. 
    moves = re.findall(r'\d+\.', pgn)  # Find all move numbers (e.g., '1.', '2.', etc.)
    return int(len(moves)) # Return the number of moves

# duckdb.create_function('get_pgn_depth', get_pgn_depth)


def extract_opening_name(url: str) -> str:
    # extract Opening Data
    opening_data = extract_opening_data(url)
    return opening_data['opening_name']
 

def preview_dataframe(data_frame) -> str:

    print("Previewing Dataframe")
    print(data_frame.dtypes)
    con = duckdb.connect(":memory:")
    # Preview the first n rows of a dataframe
    table = con.execute(f"SELECT * FROM data_frame LIMIT 5").fetchall()
    formatted_output = "\n".join(["\t".join(map(str, row)) for row in table])  # Convert to string
    print(formatted_output)

    

def transform_json_to_fact_table(year: int, month: int, **kwargs) -> None:
    # Register User Defined Functions with duckdb
    duckdb.create_function('add_move_numbers', add_move_numbers)
    duckdb.create_function('get_opening_family', get_opening_family)

    # Download the azure file into a temporary file
    with NamedTemporaryFile("w") as json_temp_file:
        az_hook.get_file(container_name=container_name, 
                         file_path=json_temp_file.name, 
                         blob_name=f"bronze/{year}-{int(month):02}-games.json")
        file_path = json_temp_file.name
        print(file_path)
        fct = duckdb.sql("""
                                SELECT url as game_url,
                time_control as time_control,
                rated as rated,
                time_class as time_class,
                rules as rules,
                white.rating as white_rating,
                white.result as white_result,
                black.rating as black_rating,
                black.result as black_result,    
                REGEXP_EXTRACT(pgn, '\[Event "(.*?)"', 1) as pgn_event,
                REGEXP_EXTRACT(pgn, '\[Site "(.*?)"', 1) as pgn_site,
                STRPTIME(REPLACE(REGEXP_EXTRACT(pgn, '\[Date "(.*?)"', 1), '.', '/'), '%Y/%m/%d')::DATE AS game_date, 
                REGEXP_EXTRACT(pgn, '\[White "(.*?)"', 1) as pgn_white_user,   
                REGEXP_EXTRACT(pgn, '\[Black "(.*?)"', 1) as pgn_black_user,
                REGEXP_EXTRACT(pgn, '\[Result "(.*?)"', 1) as pgn_result,
                REGEXP_EXTRACT(pgn, '\[CurrentPosition "(.*?)"', 1) as pgn_current_position,
                REGEXP_EXTRACT(pgn, '\[Timezone "(.*?)"', 1) as pgn_timezone,
                REGEXP_EXTRACT(pgn, '\[ECO "(.*?)"', 1) as pgn_eco,
                REGEXP_EXTRACT(pgn, '\[ECOUrl "(.*?)"', 1) as pgn_eco_url,
                STRPTIME(REGEXP_EXTRACT(pgn, '\[StartTime "(.*?)"', 1), '%H:%M:%S'):: TIME as start_time,
                STRPTIME(REGEXP_EXTRACT(pgn, '\[EndTime "(.*?)"', 1), '%H:%M:%S'):: TIME as end_time,
                                STRPTIME(REPLACE(REGEXP_EXTRACT(pgn, '\[EndDate "(.*?)"', 1), '.', '/'), '%Y/%m/%d')::DATE AS end_game_date,
                ARRAY_TO_STRING(REGEXP_EXTRACT_ALL(pgn, '\. (.*?) {\[', 1), ' ') as pgn_raw,
                add_move_numbers(REGEXP_EXTRACT_ALL(pgn, '\. (.*?) {\[', 1)) as pgn_trans""" + f" FROM read_json_auto('{file_path}')"
           ).fetchdf()
    # 
    # preview_dataframe(fct)

    # Ensure Data accuracy by ensuring that the dates are in the correct format.
    fct['start_time'] = pd.to_datetime(fct['game_date'].astype(str) +  " " + fct['start_time'].astype(str), format='%Y-%m-%d %H:%M:%S')
    fct['end_time'] = pd.to_datetime(fct['end_game_date'].astype(str) + " " + fct['end_time'].astype(str), format='%Y-%m-%d %H:%M:%S')


    # Upload the file to silver layer
    with NamedTemporaryFile(mode="w", suffix=".parquet") as temp_file:
        # Write fact table to parquet after creating arrow table
        fct.to_parquet(temp_file.name, index=False)

        # creating Parquet file
        # fct.to_parquet(temp_file.name)

        blobname = f"silver/fact-{year}-{int(month):02}-games.parquet"
        # load the file to az_storage
        az_hook.load_file(
            file_path=temp_file.name,
            container_name=container_name,
            blob_name=blobname,
            overwrite=True
        )
    # Push File name to xcom
    ti = kwargs['ti']
    ti.xcom_push(key="fact_blob_name", value=blobname)

    print(f"Successfully Transformed Data and Loaded to Azure Storage as {blobname}")



def format_time_control(timecontrol: str)-> str:
    if "+" in timecontrol:
        tc = timecontrol.split("+")
        minute = int(int(tc[0]) / 60)
        return "".join([str(minute), "|", tc[1]])

    else: 
        return str(int(int(timecontrol) / 60))

    


def load_parquet_to_warehouse():
    # Load the parquet file to the warehouse
    conn = duckdb.connect(":memory:")

    # Download the parquet file to be loaded into the warehouse
    with NamedTemporaryFile("w", suffix=".parquet") as parquet_temp_file:
        az_hook.get_file(container_name=container_name, 
                         file_path=parquet_temp_file.name, 
                         blob_name=f"gold/fact-games.parquet")
        file_path = parquet_temp_file.name

        fact_table = conn.execute(f""" SELECT * FROM read_parquet('{file_path}')""").fetchdf()
        
    with NamedTemporaryFile("w", suffix=".csv") as csv_temp_file:
        fact_table.to_csv(csv_temp_file.name, index=False)
        # conn.execute(f"""COPY chess_dw.fact_games FROM '{csv_temp_file.name}' WITH CSV HEADER; """)
        
        psql_hook.copy_expert(f""" COPY chess_dw.fact_games FROM STDIN DELIMITER ',' CSV HEADER; """, csv_temp_file.name)
        print("Successfully Loaded Data to Warehouse")


def initialize_azure_extension():
    # Fetch the azure connection string that allows to connect duckdb directly to azure storage to read files.
    conn_string = Variable.get("AZURE_STORAGE_CONN_STRING_SECRET")

    conn = duckdb.connect(':memory:')
    conn.sql(
    """INSTALL azure; 
        LOAD azure"""
    )

    conn.sql(f"""
        CREATE SECRET azure_adls_secret (
        TYPE azure,
        CONNECTION_STRING '{conn_string}' );
    """)

    # Set the azure_transport_option_type to curl to avoid read error
    conn.sql(
        """SET azure_transport_option_type = 'curl';"""
    )

    return conn

def upload_duckdb_to_azure(duckdb_result: duckdb.DuckDBPyRelation, 
                           container_name: str,
                           blob_name: str) -> None:
    print(f"File To be loaded has {duckdb_result.shape}")
    with NamedTemporaryFile("w", suffix=".parquet") as temp_file:
        df = duckdb_result.fetchdf()
        
        df.to_parquet(temp_file.name, index=False)
        az_hook.load_file(
            file_path=temp_file.name,
            container_name=container_name,
            blob_name=blob_name,
            overwrite=True
        )
    print(f"Successfully Loaded to Azure Storage as {blob_name}")


########################################### Loading Sripts for Gold Layer ##############################
 

def load_dim_openings(**kwargs):
    # Extract filename from airflow xcoms from previous run
    ti = kwargs['ti']
    filename = ti.xcom_pull(
        task_ids="transform_json_to_fact_table",  
        dag_id="pull_data_from_chess_api",  
        key="fact_blob_name"
        )
    # filename  = kwargs['dim_destination']
    print(f"Received File Name: {filename}")

    con = initialize_azure_extension()
    # Add the UDF to duckdb
    con.create_function('extract_opening_name', extract_opening_name)
    con.create_function('get_opening_family', get_opening_family)

    dim_file_name = "gold/dim_openings.parquet"

    source_file_path = f"az://rbchesssa.blob.core.windows.net/chess-etl-files/{filename}"
    destination_file_path = f"az://rbchesssa.blob.core.windows.net/chess-etl-files/{dim_file_name}"
    # Check existence of file in Azure storage
    file_exists = az_hook.check_for_blob(container_name=container_name, blob_name=dim_file_name)
    
    if file_exists:
        cur_dim_openings = con.sql(f"""SELECT DISTINCT pgn_eco_url, 
                                            extract_opening_name(pgn_eco_url) as opening_name,
                                            get_opening_family(opening_name) as opening_family
                                   
                                    FROM '{source_file_path}'
                                    WHERE pgn_eco_url NOT IN (
                                        SELECT pgn_eco_url 
                                        FROM '{destination_file_path}'
                                            )
                                    UNION  -- Simply append existing data

                                    SELECT * FROM '{destination_file_path}';                                 
                                """).fetchdf()
    else: 
        cur_dim_openings = con.sql(f"""SELECT DISTINCT pgn_eco_url, 
                                           extract_opening_name(pgn_eco_url) as opening_name,
                                            get_opening_family(opening_name) as opening_family
                                        FROM '{source_file_path}'; """).fetchdf()

    # Create NamedTemp File and upload to azure
    with NamedTemporaryFile("w", suffix=".parquet") as temp_file:
        cur_dim_openings.to_parquet(temp_file.name, index=False)
        az_hook.load_file(
            file_path=temp_file.name,
            container_name=container_name,
            blob_name=dim_file_name,
            overwrite=True
        )
    print(f"Successfully Loaded Dim Openings to Azure Storage as {dim_file_name}")


    # Install azure extension

def load_dim_date(**kwargs):
    # Extract filename from airflow xcoms from previous run
    ti = kwargs['ti']
    filename = ti.xcom_pull(
        task_ids="transform_json_to_fact_table",  
        dag_id="pull_data_from_chess_api",  
        key="fact_blob_name"
        )
    # filename  = kwargs['dim_destination']
    print(f"Received File Name: {filename}")
    con = initialize_azure_extension()
    dim_file_name = "gold/dim_date.parquet"

    source_file_path = f"az://rbchesssa.blob.core.windows.net/chess-etl-files/{filename}"
    destination_file_path = f"az://rbchesssa.blob.core.windows.net/chess-etl-files/{dim_file_name}"
    # Check existence of file in Azure storage
    file_exists = az_hook.check_for_blob(container_name=container_name, blob_name=dim_file_name)

    if file_exists:
        dim_date = con.sql(f"""
                SELECT DISTINCT game_date,
                        EXTRACT(YEAR FROM game_date) AS year,
                        EXTRACT(MONTH FROM game_date) AS month, 
                        strftime('%B', game_date) AS month_name,
                        EXTRACT(DAY FROM game_date) AS day,
                        strftime('%A', game_date) AS weekday,
                        CASE WHEN CAST(strftime('%m', game_date) AS INTEGER) BETWEEN 1 AND 3 THEN 1
                                             WHEN CAST(strftime('%m', game_date) AS INTEGER) BETWEEN 4 AND 6 THEN 2
                                             WHEN CAST(strftime('%m', game_date) AS INTEGER) BETWEEN 7 AND 9 THEN 3
                                             ELSE 4
                                        END AS quarter
                             
                        FROM '{source_file_path}' 
                        WHERE game_date NOT IN 
                ( SELECT game_date FROM '{destination_file_path}')
                UNION
                SELECT * FROM '{destination_file_path}';
                
                """)

    else:
        dim_date = con.sql(f""" SELECT DISTINCT game_date,
                                            EXTRACT(YEAR FROM game_date) AS year,
                                            EXTRACT(MONTH FROM game_date) AS month,
                                            strftime('%B', game_date) AS month_name,
                                            EXTRACT(DAY FROM game_date) AS day,
                                            strftime('%A', game_date) AS weekday,
                                            CASE WHEN CAST(strftime('%m', game_date) AS INTEGER) BETWEEN 1 AND 3 THEN 1
                                                WHEN CAST(strftime('%m', game_date) AS INTEGER) BETWEEN 4 AND 6 THEN 2
                                                WHEN CAST(strftime('%m', game_date) AS INTEGER) BETWEEN 7 AND 9 THEN 3
                                                ELSE 4
                                            END AS quarter
                                            FROM '{source_file_path}'
                                        ORDER BY game_date; 
    """)
    
    # Upload the file to gold layer
    upload_duckdb_to_azure(dim_date, container_name, dim_file_name)

def load_dim_time_control(**kwargs):
    # Extract filename from airflow xcoms from previous run
    ti = kwargs['ti']
    filename = ti.xcom_pull(
        task_ids="transform_json_to_fact_table",  
        dag_id="pull_data_from_chess_api",  
        key="fact_blob_name"
        )
    print(f"Received File Name: {filename}")
    con = initialize_azure_extension()
    con.create_function('format_time_control', format_time_control)
    dim_file_name = "gold/dim_time_control.parquet"

    source_file_path = f"az://rbchesssa.blob.core.windows.net/chess-etl-files/{filename}"
    destination_file_path = f"az://rbchesssa.blob.core.windows.net/chess-etl-files/{dim_file_name}"
    # Check existence of file in Azure storage
    file_exists = az_hook.check_for_blob(container_name=container_name, blob_name=dim_file_name)

    if file_exists:
        dim_time_control = con.sql(f"""
                SELECT DISTINCT format_time_control(time_control) as time_control, time_class 
                FROM '{source_file_path}' WHERE time_control NOT IN 
                ( SELECT time_control FROM '{destination_file_path}')
                UNION 
                SELECT * FROM '{destination_file_path}';
                """)

    else:
        dim_time_control = con.sql(f"""SELECT format_time_control(time_control) as time_control, time_class
                FROM '{source_file_path}';
    """)
        
    # Upload the file to gold layer
    upload_duckdb_to_azure(dim_time_control, container_name, dim_file_name)

def load_dim_results(**kwargs):
    # Extract filename from airflow xcoms from previous run
    ti = kwargs['ti']
    filename = ti.xcom_pull(
        task_ids="transform_json_to_fact_table",  
        dag_id="pull_data_from_chess_api",  
        key="fact_blob_name"
        )
    
    # filename  = kwargs['dim_destination']
    print(f"Received File Name: {filename}")
    con = initialize_azure_extension()
    dim_file_name = "gold/dim_results.parquet"

    source_file_path = f"az://rbchesssa.blob.core.windows.net/chess-etl-files/{filename}"
    destination_file_path = f"az://rbchesssa.blob.core.windows.net/chess-etl-files/{dim_file_name}"
    # Check existence of file in Azure storage
    file_exists = az_hook.check_for_blob(container_name=container_name, blob_name=dim_file_name)
    if file_exists:
        dim_results = con.sql(f"SELECT * FROM '{destination_file_path}'")

    else:
        dim_results = con.sql("""
                SELECT 'win' AS result_code, 'Win' AS result, 'Win' AS description
        UNION ALL
        SELECT 'checkmated', 'Loss', 'Checkmated'
        UNION ALL
        SELECT 'agreed', 'Draw', 'Draw agreed'
        UNION ALL
        SELECT 'repetition', 'Draw', 'Draw by repetition'
        UNION ALL
        SELECT 'timeout', 'Win', 'Timeout'
        UNION ALL
        SELECT 'resigned', 'Loss', 'Resigned'
        UNION ALL
        SELECT 'stalemate', 'Draw', 'Stalemate'
        UNION ALL
        SELECT 'lose', 'Loss', 'Lose'
        UNION ALL
        SELECT 'insufficient', 'Draw', 'Insufficient material'
        UNION ALL
        SELECT '50move', 'Draw', 'Draw by 50-move rule'
        UNION ALL
        SELECT 'abandoned', 'Draw', 'Abandoned'
        UNION ALL
        SELECT 'kingofthehill', 'Win', 'Opponent king reached the hill'
        UNION ALL
        SELECT 'threecheck', 'Win', 'Checked for the 3rd time'
        UNION ALL
        SELECT 'timevsinsufficient', 'Draw', 'Draw by timeout vs insufficient material'
        UNION ALL
        SELECT 'bughousepartnerlose', 'Loss', 'Bughouse partner lost'
        """)

        upload_duckdb_to_azure(dim_results, container_name, dim_file_name)


def load_fact_table(**kwargs):
    # Fetch the monthly data from the silver layer. 
    ti = kwargs['ti']
    filename = ti.xcom_pull(
        task_ids="transform_json_to_fact_table",  
        dag_id="pull_data_from_chess_api",  
        key="fact_blob_name"
        )
    exec_date = kwargs['exec_date']
    print(exec_date)
    # exec_date = datetime.strptime(kwargs['exec_date'], "%Y-%m-%d").date()
    

    print(f"Received File Name: {filename}")
    # Initailize azure and duckdb instance
    con = initialize_azure_extension()
    con.create_function('format_time_control', format_time_control)

    # Defin Source and destinations
    fact_file_name = "gold/fact-games.parquet"
    source_file_path = f"az://rbchesssa.blob.core.windows.net/chess-etl-files/{filename}"
    destination_file_path = f"az://rbchesssa.blob.core.windows.net/chess-etl-files/{fact_file_name}"
    # Check existence of file in Azure storage
    file_exists = az_hook.check_for_blob(container_name=container_name, blob_name=fact_file_name)

    # Create references for all dimensional files
    dim_openings = "az://rbchesssa.blob.core.windows.net/chess-etl-files/gold/dim_openings.parquet"
    dim_date = "az://rbchesssa.blob.core.windows.net/chess-etl-files/gold/dim_date.parquet"
    dim_results = "az://rbchesssa.blob.core.windows.net/chess-etl-files/gold/dim_results.parquet"
    dim_time_control = "az://rbchesssa.blob.core.windows.net/chess-etl-files/gold/dim_time_control.parquet" 


    fct = con.sql(f"""
                SELECT game_url as game_url,
                       game_date as game_date,
                       start_time as start_time,
                       end_time as end_time,
                       date_diff('seconds', start_time, end_time)::BIGINT as game_duration_secs,
                       format_time_control(time_control) as time_control, 
                       CASE WHEN pgn_white_user = 'Rhythmbear1' THEN 'white'
                            ELSE 'black' END as my_color,
                       CASE 
                            WHEN pgn_white_user = 'Rhythmbear1' THEN white_rating
                            ELSE black_rating END as my_rating,
                       CASE 
                            WHEN pgn_white_user = 'Rhythmbear1' THEN black_rating
                            ELSE white_rating END as opponent_rating,
                       CASE 
                            WHEN pgn_white_user = 'Rhythmbear1' THEN white_result
                            ELSE black_result END as my_result,
                       CASE 
                            WHEN pgn_white_user = 'Rhythmbear1' THEN black_result
                            ELSE white_result END as opponent_result,
                       pgn_current_position as game_fen,
                       pgn_eco_url as opening_url,
                       pgn_trans as game_pgn,
                       '{exec_date}'::TIMESTAMP as last_updated
                       FROM '{source_file_path}' as fact""")
    
    fact_table = con.sql(f"""SELECT fact.* FROM fct AS fact
                        LEFT JOIN '{dim_date}' AS dim_date ON fact.game_date = dim_date.game_date
                        LEFT JOIN '{dim_openings}' AS dim_openings ON fact.opening_url = dim_openings.pgn_eco_url
                        LEFT JOIN '{dim_results}' AS dim_results_my ON fact.my_result = dim_results_my.result_code
                        LEFT JOIN '{dim_results}' AS dim_results_op ON fact.opponent_result = dim_results_op.result_code
                        LEFT JOIN '{dim_time_control}' AS dim_time_control ON fact.time_control = dim_time_control.time_control;
                             """)
    print(f"fact table to be added has: {fact_table.shape}")
    if file_exists:
        prev_fact_table = con.sql(f"""SELECT * FROM '{destination_file_path}' """)
        print(f"The Previous fact table has : {prev_fact_table.shape}")  # Should not be (0, 0)
        
        both_tables = con.sql(f"""SELECT * FROM prev_fact_table
                                                    UNION ALL 
                                                    SELECT * FROM fact_table;""")
        print(f"Both Tables have: {both_tables.shape}")
        new_fact_table = con.sql(f"""SELECT * 
                                    FROM (
                                        SELECT *, 
                                                ROW_NUMBER() OVER ( PARTITION BY game_url ORDER BY last_updated DESC) AS rn
                                                FROM both_tables
                                        )
                                 WHERE rn = 1;
                                    
                                 """).fetchdf()
        
        # Here i'm droping the row number column and converting the query back to a Duckdb.Pyrelation Object so that i can
        # pass it into the upload duckdb to azure function. 
        new_fact_table.drop(columns=['rn'], inplace=True)
        new_fact_table = con.from_df(new_fact_table)
        # preview_dataframe(new_fact_table)
        
    else:
        new_fact_table = fact_table
        
    upload_duckdb_to_azure(new_fact_table, container_name, fact_file_name)
    


#################################### LOAD TO DATABASES ##############################################

def load_fact_to_postgres(**kwargs):
    ti = kwargs['ti']
    last_updated_date = ti.xcom_pull(
        task_ids="get_last_updated_date",  
        dag_id="load_data_warehouse", 
        key='return_value' 
        )[0][0]
    print(f"Fetching data for Last Updated Date: {last_updated_date}")
    con = initialize_azure_extension()

    fact = con.sql(f"""
                        SELECT * 
                        FROM 'az://rbchesssa.blob.core.windows.net/chess-etl-files/gold/fact-games.parquet'
                    """).df()

    print(f"✅ DataFrame Loaded: {fact.shape[0]} rows, {fact.shape[1]} columns")                
    engine = psql_hook.get_sqlalchemy_engine()

    fact.to_sql(name='fact_games', 
                schema='chess_dw',
                con=engine, 
                if_exists='replace', 
                index=False)
    print("Successfully Loaded Data to Postgres")
    
def load_dim_table_to_postgres(
    dim_file_name: str,
    table_name: str
):
    con = initialize_azure_extension()
    file_path = f"az://rbchesssa.blob.core.windows.net/chess-etl-files/{dim_file_name}"
    dim = con.sql(f"""
                    SELECT * 
                    FROM '{file_path}'
                    """).df()
    
    print(f"✅ {dim_file_name} Loaded: {dim.shape[0]} rows, {dim.shape[1]} columns")
    engine = psql_hook.get_sqlalchemy_engine()

    dim.to_sql(name=table_name, 
                schema='chess_dw',
                con=engine, 
                if_exists='replace', 
                index=False)
    print(f"Successfully Loaded {dim_file_name} Data to Postgres table {table_name}")

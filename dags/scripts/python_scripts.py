from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from tempfile import NamedTemporaryFile
import duckdb
import requests
import json
import io


# Define Variables
storage_account_name = 'rbchesssa'
container_name = 'chess-etl-files'
az_hook = WasbHook(wasb_conn_id='azure_chess_storage') # Using the Storage Account Connection to Azure.



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

        blobname = f"raw/{year}-{int(month):02}-games.json"
        az_hook.load_file(
            file_path=temp.name,
            container_name=container_name,
            blob_name=blobname,
            overwrite=True
        )    

        print(f"Successfully Uploaded Data to Azure Storage as {blobname}")
        return True


# Here I'm defining extra duckdb Functions to transform fact table data.

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
    

def transform_json_to_fact_table(year: int, month: int) -> None:
    # Register User Defined Functions with duckdb
    duckdb.create_function('add_move_numbers', add_move_numbers)
    duckdb.create_function('get_opening_family', get_opening_family)

    # Download the azure file into a temporary file
    with NamedTemporaryFile("w") as json_temp_file:
        az_hook.get_file(container_name=container_name, 
                         file_path=json_temp_file.name, 
                         blob_name=f"raw/{year}-{int(month):02}-games.json")
        file_path = json_temp_file.name
        print(file_path)
        fact_table = duckdb.sql("""
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
                STRPTIME(REPLACE(REGEXP_EXTRACT(pgn, '\[Date "(.*?)"', 1), '.', '/'), '%Y/%m/%d') AS game_date, 
                REGEXP_EXTRACT(pgn, '\[White "(.*?)"', 1) as pgn_white_user,   
                REGEXP_EXTRACT(pgn, '\[Black "(.*?)"', 1) as pgn_black_user,
                REGEXP_EXTRACT(pgn, '\[Result "(.*?)"', 1) as pgn_result,
                REGEXP_EXTRACT(pgn, '\[CurrentPosition "(.*?)"', 1) as pgn_current_position,
                REGEXP_EXTRACT(pgn, '\[Timezone "(.*?)"', 1) as pgn_timezone,
                REGEXP_EXTRACT(pgn, '\[ECO "(.*?)"', 1) as pgn_eco,
                REGEXP_EXTRACT(pgn, '\[ECOUrl "(.*?)"', 1) as pgn_eco_url,
                REGEXP_EXTRACT(pgn, '\[StartTime "(.*?)"', 1) as start_time,
                REGEXP_EXTRACT(pgn, '\[EndTime "(.*?)"', 1) as End_time,
                ARRAY_TO_STRING(REGEXP_EXTRACT_ALL(pgn, '\. (.*?) {\[', 1), ' ') as pgn_raw,
                add_move_numbers(REGEXP_EXTRACT_ALL(pgn, '\. (.*?) {\[', 1)) as pgn_trans""" + f" FROM read_json_auto('{file_path}')"
           ).fetchdf()
    # 
    with NamedTemporaryFile(mode="w", suffix=".csv") as temp_file:
        #Write fact table to csv
        fact_table.to_csv(temp_file.name, header=True)

        blobname = f"silver/fact-{year}-{int(month):02}-games.csv"
        # load the file to az_storage
        az_hook.load_file(
            file_path=temp_file.name,
            container_name=container_name,
            blob_name=blobname,
            overwrite=True
        )

    print(f"Successfully Transformed Data and Loaded to Azure Storage as {blobname}")


    
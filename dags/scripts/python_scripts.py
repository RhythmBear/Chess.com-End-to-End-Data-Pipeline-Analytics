from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from tempfile import NamedTemporaryFile
import requests
import json


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

    url = f"https://api.chess.com/pub/player/{username}/games/{year}/{month:02}"
    response = requests.get(url, headers=headers)

    pulled_data = []
    
    if response.status_code == 200:
        data = response.json()
        pulled_data = data.get("games", [])
        print("Successfully Fetched Data From CHess.com API")
    else:
        print(f"Failed to fetch data: {response.status_code}")
        pulled_data = []

    # Upload the data to Azure Storage
    with NamedTemporaryFile("w") as temp:
        print("Attempting to Upload Data to Azure Storage")
        json.dump(pulled_data, temp)
        temp.flush()
        az_hook = WasbHook(wasb_conn_id='azure_chess_storage')
        az_hook.load_file(
            file_path=temp.name,
            container_name=container_name,
            blob_name=f"raw/{year}-{month:02}-games.json",
            overwrite=True
        )    

        print("Successfully Uploaded Data to Azure Storage")
        return True

    
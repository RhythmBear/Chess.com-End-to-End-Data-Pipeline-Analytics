import os
import requests
import json
import pandas as pd
import duckdb

# Defining the Functions for extracting the data


def extract_chess_data(username: str, year: int, month: int) -> list:
    """
    Fetch chess game data for a specific user and month from Chess.com API.
    
    :param username: Chess.com username
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
    
    if response.status_code == 200:
        data = response.json()
        return data.get("games", [])
    else:
        print(f"Failed to fetch data: {response.status_code}")
        return []




def load_data_in_dir(games: list, year: int, month: int):
    """
    Save chess games data into a partitioned directory structure by month.
    
    :param games: List of game data
    :param year: Year of the games
    :param month: Month of the games
    """

    # I'm creating directories for each month and saving the games in a JSON file and also formatting the month to 2 digits
    os.makedirs(f"Data/{year}-{month:02}", exist_ok=True)
    with open(f"Data/{year}-{month:02}/games.json", "w") as f:
        json.dump(games, f, indent=4)

    print(f"Sucessfully loaded {len(games)} games for {year}-{month:02}")
    return len(games)


def transform():
    pass
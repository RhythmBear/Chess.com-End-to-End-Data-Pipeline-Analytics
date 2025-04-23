import re
import duckdb

import requests
from selectolax.parser import HTMLParser


def extract_opening_data(url):
    """Extracts chess opening data, including the opening name and moves,
    from a given URL.
    Args:
        url (str): The URL of the webpage containing chess opening data.
    Returns:
        dict: A dictionary containing:
            - "opening_name" (str or None): `None` if not found.
            - "moves" (str or None): moves.
    Raises:
        None: All exceptions are caught and handled internally, with errors 
        logged to the console.
       """
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

    except requests.RequestException as e:
        print(f"Request failed: {e}")
        return {"opening_name": None, "moves": None}  # Handle errors



def add_move_numbers(pgn: list) -> str:
    """
    Adds move numbers to a list of chess moves in PGN format.
    Args:
        pgn (list): List of chess moves.
    Returns:
        str: PGN string with move numbers added.
    Example:
        Input: ["e4", "e5", "Nf3", "Nc6", "Bb5"]
        Output: "1. e4 e5 2. Nf3 Nc6 3. Bb5"
    """
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


def get_opening_family(opening_name: str) -> str:
    """
    Extracts the family name of a chess opening from its full name.

    If the opening name contains a colon (":"), the function splits the name
    at the colon and returns the part before it, which represents the family
    name of the opening. If no colon is present, the function returns the
    original opening name.

    Args:
        opening_name (str): The full name of the chess opening.

    Returns:
        str: The family name of the chess opening.
    """
    """"""
    # Get the parent name of the move numbers

    if ":" in opening_name:
        name_splitted = opening_name.split(":")
        return name_splitted[0]

    else:
        return opening_name
    
def get_opening_variation(opening_name: str) -> str:
    """
    Extracts and returns the variation part of a chess opening name if it contains a colon.
    Otherwise, returns the full opening name.
    Args:
        opening_name (str): The name of the chess opening.
    Returns:
        str: The variation part of the opening name or the full name if no variation exists.
    """

    # Get the parent name of the move numbers

    if ":" in opening_name:
        name_splitted = opening_name.split(":")
        return name_splitted[1]

    else:
        return opening_name
    

def get_pgn_depth(pgn: str) -> int:
    """
    Counts the number of moves in a PGN string.

    Args:
        pgn (str): PGN data of a chess game.

    Returns:
        int: Total number of moves.
    """
    """"""
    # this function is supposed to define the number of moves from a pgn file. 
    moves = re.findall(r'\d+\.', pgn)  # Find all move numbers (e.g., '1.', '2.', etc.)
    return int(len(moves)) # Return the number of moves

def extract_opening_name(url: str) -> str:
    """
    Extracts the name of the chess opening from the given URL.
    This function utilizes the `extract_opening_data` helper function to retrieve
    opening-related data from the URL and returns the name of the opening.
    Args:
        url (str): The URL containing chess game data.
    Returns:
        str: The name of the chess opening extracted from the URL.
    """

    # extract Opening Data
    opening_data = extract_opening_data(url)
    return opening_data['opening_name']

def format_time_control(timecontrol: str)-> str:
    """
    Converts a time control string into a different format 
    i.e minute|second(2|1) instead of seconds|seconds(120+1) 

    Args:
        timecontrol (str): The time control string in seconds, optionally with an increment.

    Returns:
        str: The formatted time control string in minutes.
    """
    
    if "+" in timecontrol:
        tc = timecontrol.split("+")
        minute = int(int(tc[0]) / 60)
        return "".join([str(minute), "|", tc[1]])

    else: 
        return str(int(int(timecontrol) / 60))
    
def initialize_udfs(connector: duckdb.DuckDBPyConnection)->duckdb.DuckDBPyConnection:
    """
    Registers custom user-defined functions (UDFs) with the provided DuckDB connection.
    Args:
        connector (duckdb.DuckDBPyConnection): The DuckDB connection to register the UDFs with.
    Returns:
        duckdb.DuckDBPyConnection: The DuckDB connection with the UDFs registered.
    """
    connector.create_function('add_move_numbers', add_move_numbers)
    connector.create_function('extract_opening_name', extract_opening_name)
    connector.create_function('get_opening_family', get_opening_family)
    connector.create_function('get_opening_variation', get_opening_variation)
    connector.create_function('get_pgn_depth', get_pgn_depth)
    connector.create_function('format_time_control', format_time_control)

    return connector
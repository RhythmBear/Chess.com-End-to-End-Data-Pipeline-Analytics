from utils import load_data_in_dir, extract_chess_data


def main():
    # Extracting the data
    games = extract_chess_data("rhythmbear1", 2024, 1)
    
    # Loading the data
    load_data_in_dir(games, 2024, 1)
    print("Data loaded successfully!")


main()

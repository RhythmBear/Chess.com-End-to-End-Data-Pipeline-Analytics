-- Fact Tables Creating
CREATE SCHEMA IF NOT EXISTS chess_dw;
CREATE TABLE IF NOT EXISTS chess_dw.fact_games (
    game_url VARCHAR PRIMARY KEY,
    game_date TIMESTAMP,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    game_duration_secs INTEGER,
    time_control VARCHAR,
    my_color VARCHAR,
    my_rating INTEGER,
    opponent_rating INTEGER,
    my_result VARCHAR,
    opponent_result VARCHAR,
    game_fen VARCHAR,
    opening_url VARCHAR,
    game_pgn VARCHAR,
    moves INTEGER,
    last_updated TIMESTAMP
);


-- Dimension tables creation
CREATE TABLE IF NOT EXISTS chess_dw.dim_date (
    date_id INTEGER PRIMARY KEY,
    game_date DATE,
    year INTEGER,
    month INTEGER,
    month_name VARCHAR,
    day INTEGER,
    day_of_the_week VARCHAR,
    quarter INTEGER,
    is_weekend BOOLEAN
);


CREATE TABLE IF NOT EXISTS chess_dw.dim_openings (
    pgn_eco_url VARCHAR PRIMARY KEY,
    opening_name VARCHAR,
    opening_family VARCHAR,
    opening_variation VARCHAR,
    eco_code VARCHAR
);


CREATE TABLE IF NOT EXISTS chess_dw.dim_users (
    user_id VARCHAR PRIMARY KEY,
    full_name VARCHAR,
    nationality VARCHAR
);

CREATE TABLE IF NOT EXISTS chess_dw.dim_time_control (
    time_control VARCHAR PRIMARY KEY,
    time_class VARCHAR
);

CREATE TABLE IF NOT EXISTS chess_dw.dim_results (
    result_code VARCHAR PRIMARY KEY,
    result VARCHAR,
    description VARCHAR
);
import requests
import time
import sqlite3
from tqdm import tqdm
from pandas import json_normalize

conn = sqlite3.connect("/Users/abeljohn/Developer/NFLPlayProject/play_by_play.db")
cursor = conn.cursor()

cursor.execute('''
CREATE TABLE IF NOT EXISTS play_data (
    id TEXT PRIMARY KEY,
    ref TEXT,
    sequence_number INTEGER,
    text TEXT,
    short_text TEXT,
    alternative_text TEXT,
    short_alternative_text TEXT,
    away_score INTEGER,
    home_score INTEGER,
    period_number INTEGER,
    clock_value REAL,
    clock_display TEXT,
    scoring_play BOOLEAN,
    priority BOOLEAN,
    score_value INTEGER,
    modified TEXT,
    start_down INTEGER,
    start_distance INTEGER,
    start_yard_line INTEGER,
    start_yards_to_endzone INTEGER,
    start_down_distance_text TEXT,
    start_short_down_distance_text TEXT,
    start_possession_text TEXT,
    end_down INTEGER,
    end_distance INTEGER,
    end_yard_line INTEGER,
    end_yards_to_endzone INTEGER,
    end_down_distance_text TEXT,
    end_short_down_distance_text TEXT,
    end_possession_text TEXT,
    stat_yardage INTEGER
);
''')

conn.commit()

# 
years = {2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024}

for year in years:
    fourth_quarter_plays = []
    game_ids = []
    url=f"http://site.api.espn.com/apis/site/v2/sports/football/nfl/scoreboard?limit=1000&dates={year}"
    response = requests.get(url)

    if response.status_code == 200:
        season_data = response.json()
        for game in season_data['events']:
            game_ids.append(game['id'])
    else:
        print(f"Failed to retrieve data. Status code: {response.status_code}")
    time.sleep(0.1)
    for game_id in tqdm(game_ids):
        game_url = f"https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/events/{game_id}/competitions/{game_id}/plays?limit=300"
        game_response = requests.get(game_url)
        if game_response.status_code == 200:
            play_by_play_data = game_response.json()
            for plays in play_by_play_data['items']:
                if plays['period']['number'] == 4:
                    fourth_quarter_plays.append(plays)
        else:
            print(f"Failed to retrieve play by play data. Status code: {game_response.status_code}")
        time.sleep(0.1)

    flat_data = json_normalize(fourth_quarter_plays, sep='_')  # Separates nested keys with underscores
    flat_data['sequenceNumber'] = flat_data['sequenceNumber'].astype(int)
    flat_data['sequenceNumber'] = flat_data['scoringPlay'].astype(int)
    flat_data['priority'] = flat_data['priority'].astype(int)
    flat_data = flat_data.drop(columns=["participants"])
    flat_data.to_sql("play_data", conn, if_exists="replace", index=False)
    conn.commit()

conn.close()


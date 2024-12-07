# playbyplay.py

import requests
import time
import sqlite3
from tqdm import tqdm
from pandas import json_normalize
import numpy as np
import pandas as pd

"""
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
"""

def get_game_drives(game_id):
    drives = []
    game_url = f"https://sports.core.api.espn.com/v2/sports/football/leagues/nfl/events/{game_id}/competitions/{game_id}/drives"
    game_response = requests.get(game_url)
    if game_response.status_code == 200:
        game_drives = game_response.json()
        for drive in game_drives['items']:
            if drive['start']['period']['number'] == 4:
                drives.append(json_normalize(drive['plays']['items'], sep='_'))
    else:
        print(f"Failed to retrieve play by play data. Status code: {game_response.status_code}")
    
    return drives


def get_drives_from_espn(years):
    fourth_quarter_drives = []
    for year in tqdm(years):
        game_ids = []
        url=f"http://site.api.espn.com/apis/site/v2/sports/football/nfl/scoreboard?limit=1000&dates={year}"
        response = requests.get(url)
        
        if response.status_code == 200:
            season_data = response.json()
            for game in season_data['events']:
                game_ids.append(game['id'])
        else:
            print(f"Failed to retrieve data. Status code: {response.status_code}")
        for game_id in game_ids:
            fourth_quarter_drives.extend(get_game_drives(game_id))
            time.sleep(0.1)
    
    return fourth_quarter_drives
    

    # flat_data = json_normalize(fourth_quarter_plays, sep='_')  # Separates nested keys with underscores
    # flat_data['sequenceNumber'] = flat_data['sequenceNumber'].astype(int)
    # flat_data['sequenceNumber'] = flat_data['scoringPlay'].astype(int)
    # flat_data['priority'] = flat_data['priority'].astype(int)
    # if "participants" in flat_data.columns:
    #     flat_data = flat_data.drop(columns=["participants"])
#     flat_data.to_sql("play_data", conn, if_exists="replace", index=False)
#     conn.commit()
def discretize_data(fourth_quarter_drives):
    num_states = 5
    states = [[] for i in range(num_states)]
    actions = []
    rewards = []
    terminal = []
    for flat_data in fourth_quarter_drives:
        num_plays = 0
        for i in range(len(flat_data)):
            row = flat_data.iloc[i]
            play = row["type_text"]
            if play == "Pass" or play == "Pass Interception" or play == "Sack" or play == "Pass Reception" or play == "Pass Incompletion" or play == "Passing Touchdown":
                actions.append(0)
            elif play == "Rush" or play == "Rushing Touchdown":
                actions.append(1)
            elif play == "Field Goal" or play == "Field Goal Missed" or play == "Field Goal Good":
                actions.append(2)
            else:
                continue        
            
            num_plays += 1

            score_difference = abs(row["homeScore"] - row["awayScore"])
            score_difference = 0 if score_difference <= 3 else 1 if score_difference <= 8 else 2 if score_difference <= 16 else 3
            
            time_left = row["clock_value"] // 60
            time_left = 0 if time_left <= 2 else 1 if time_left <= 5 else 2
            
            down = row["start_down"] - 1
            
            yards_to_first_down = int(row["start_distance"] // 5)
            yards_to_first_down = 0 if yards_to_first_down <= 5 else 2 if yards_to_first_down <= 10 else 3
            
            yards_to_end_zone = row["start_yardsToEndzone"]
            yards_to_end_zone = 0 if yards_to_end_zone <= 10 else 1 if yards_to_end_zone <= 30 else 2 if yards_to_end_zone <= 50 else 3
            
            states[0].append(score_difference)
            states[1].append(time_left)
            states[2].append(down)
            states[3].append(yards_to_first_down)
            states[4].append(yards_to_end_zone)
            terminal.append(0)            

            yards_gained = row["start_yardsToEndzone"] - row["end_yardsToEndzone"]
            score_value = row["scoreValue"]
            reward = yards_gained * 0.1 + score_value
            if play == "Pass Interception" or play == "Sack" or play == "Field Goal Missed":
                reward -= 2
            rewards.append(reward)
            
            if score_value != 0:
                break
        if num_plays > 0:
            terminal[-1] = 1
    states = np.array(states)
    ranges = []
    # breakpoint()
    for i in range(num_states):
        max_value = max(states[i])
        min_value = min(states[i])    
        ranges.append(max_value - min_value + 1)
    ranges = tuple(ranges)
    states = np.ravel_multi_index(states, ranges)

    next_states = [-1] * len(states)
    for i in range(len(states)):
        if not terminal[i]:
            next_states[i] = states[i + 1]

    plays = np.array([states, actions, rewards, next_states])
    plays = np.transpose(plays)
    plays = pd.DataFrame(plays, columns = ["state", "action", "reward", "next_state"])
    plays[["state", "action", "next_state"]] = plays[["state", "action", "next_state"]].astype(int)
    return plays

def get_all_plays():
    years = {2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023}
    drives = get_drives_from_espn(years)
    plays = discretize_data(drives)
    
    conn = sqlite3.connect("/Users/abeljohn/Developer/NFLPlayProject/play_by_play.db")
    plays.to_sql('disc_plays_no_2024', conn, if_exists='replace', index=False)
    conn.close()

# get_all_plays()
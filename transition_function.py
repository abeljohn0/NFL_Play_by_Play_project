# transition_function.py

import pandas as pd
from collections import defaultdict
import sqlite3

conn = sqlite3.connect("/Users/abeljohn/Developer/NFLPlayProject/play_by_play.db")

query = "SELECT * FROM disc_plays_no_2024"

plays = pd.read_sql_query(query, conn)

transition_counts = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))

for _, row in plays.iterrows():
    s, a, sp = row['state'], row['action'], row['next_state']
    transition_counts[s][a][sp] += 1

    transition_probabilities = []

    for state, actions in transition_counts.items():
        for action, next_states in actions.items():
            total_transitions = sum(next_states.values())
            for next_state, count in next_states.items():
                probability = count / total_transitions
                transition_probabilities.append((state, action, next_state, probability))

transition_df = pd.DataFrame(
    transition_probabilities, 
    columns=['state', 'action', 'next_state', 'probability']
)
# breakpoint()
transition_df.to_sql("transition_probs", conn, if_exists='replace', index=False)

conn.close()
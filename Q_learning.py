# Q_learning.py

import pandas as pd
import numpy as np
from tqdm import tqdm
import sqlite3
import pandas as pd

alpha = 0.1
gamma = 0.95
epochs = 5

conn = sqlite3.connect("/Users/abeljohn/Developer/NFLPlayProject/play_by_play.db")

query = "SELECT * FROM disc_plays_no_2024"

plays = pd.read_sql_query(query, conn)

states = plays['state'].unique()
actions = plays['action'].unique()
Q_table = pd.DataFrame(0, index=states, columns=actions, dtype=float)
for epoch in tqdm(range(epochs)):
    for index, row in plays.iterrows():
        s, a, r, sp = row['state'], row['action'], row['reward'], row['next_state']
        
        current_q = Q_table.loc[s, a]
        
        max_future_q = Q_table.loc[sp].max() if sp in Q_table.index else 0
        
        new_q = current_q + alpha * (r + gamma * max_future_q - current_q)
        Q_table.loc[s, a] = new_q

optimal_policy = Q_table.idxmax(axis=1)
print(optimal_policy)
print(len(optimal_policy))
# breakpoint()
optimal_policy_df = optimal_policy.reset_index()
optimal_policy_df.to_sql('optimal_policy', conn, if_exists='replace', index=False)
conn.close()
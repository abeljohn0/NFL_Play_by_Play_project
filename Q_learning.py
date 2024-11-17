import pandas as pd
import numpy as np
from tqdm import tqdm
import sqlite3
import pandas as pd

alpha = 0.1
gamma = 0.95
epochs = 1

conn = sqlite3.connect("/Users/abeljohn/Developer/NFLPlayProject/play_by_play.db")

query = "SELECT * FROM state_play_data"

df = pd.read_sql_query(query, conn)

conn.close()

states = df['s'].unique()
actions = df['a'].unique()
Q_table = pd.DataFrame(0, index=states, columns=actions, dtype=float)
for epoch in tqdm(range(epochs)):
    for index, row in df.iterrows():
        s, a, r, sp = row['s'], row['a'], row['r'], row['sp']
        
        current_q = Q_table.loc[s, a]
        
        max_future_q = Q_table.loc[sp].max() if sp in Q_table.index else 0
        
        new_q = current_q + alpha * (r + gamma * max_future_q - current_q)
        Q_table.loc[s, a] = new_q

optimal_policy = Q_table.idxmax(axis=1)
# with open("/Users/abeljohn/Downloads/medium.policy", "w") as f:
#         for si in range(1, 50001):
#             f.write(f"{optimal_policy[si]}\n")
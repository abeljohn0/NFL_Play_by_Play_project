# simulate_rewards.py
from playbyplay import get_game_drives, discretize_data
import numpy as np
import pandas as pd
import sqlite3

def simulate_rewards(start_states):
    conn = sqlite3.connect("/Users/abeljohn/Developer/NFLPlayProject/play_by_play.db")
    query_trans = "SELECT * FROM transition_probs"
    transition_probs = pd.read_sql_query(query_trans, conn)
    query_policy = "SELECT * FROM optimal_policy"
    policy = pd.read_sql_query(query_policy, conn)
    query_plays = "SELECT * FROM disc_plays_no_2024"
    all_plays = pd.read_sql_query(query_plays, conn)
    conn.close()

    discount = 0.95
    def q_value_calc(state, type="sim"):
        if state == -1:
            return 0
        
        if type == "sim":
            action = policy[policy["index"] == state]["0"].iloc[0]

        reward = all_plays[(all_plays['state'] == state) & (all_plays['action'] == action)]["reward"].mean()
        reward = 0 if pd.isna(reward) else reward
        print(reward)

        next_states = transition_probs[transition_probs["state"] == state]["next_state"]

        for ns in next_states:
            prob = transition_probs[
                (transition_probs["state"] == state) &
                (transition_probs["action"] == action) &
                (transition_probs["next_state"] == ns)
            ]
            if prob.empty:
                prob = 0
            else:
                prob = prob["probability"].iloc[0]
            reward += discount*(prob*q_value_calc(ns))
        
        return reward
    acc_reward = 0
    for state in start_states:
        acc_reward += q_value_calc(state)
        
    return acc_reward

drives = get_game_drives("401671814")
plays = discretize_data(drives)
print(sum(plays["reward"]))
starting_state = plays.iloc[0]
end_state_idxs = plays[:-1][plays['next_state'] == -1].index
start_states = plays.iloc[np.append(0,end_state_idxs.to_numpy()+1)]["state"]

reward = simulate_rewards(start_states)
print("sim reward", reward)
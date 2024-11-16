import sqlite3
import pandas as pd

conn = sqlite3.connect('your_database.db')

query = "SELECT * FROM play_data"

plays_df = pd.read_sql_query(query, conn)

conn.close()

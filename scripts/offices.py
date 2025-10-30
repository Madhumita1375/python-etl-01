import os
import pandas as pd
def extract_offices(conn):
    os.makedirs("data/offices", exist_ok=True)
    query = "SELECT * FROM OFFICES"
    df = pd.read_sql(query, conn)
    file_path = "data/offices/offices.csv"
    df.to_csv(file_path, index=False)

import os
import pandas as pd
def extract_orderdetails(conn):
    os.makedirs("data/orderdetails", exist_ok=True)
    query = "SELECT * FROM ORDERDETAILS"
    df = pd.read_sql(query, conn)
    file_path = "data/orderdetails/orderdetails.csv"
    df.to_csv(file_path, index=False)

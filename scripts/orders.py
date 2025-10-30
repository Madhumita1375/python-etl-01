import os
import pandas as pd
def extract_orders(conn):
    os.makedirs("data/orders", exist_ok=True)
    query = "SELECT * FROM ORDERS"
    df = pd.read_sql(query, conn)
    file_path = "data/orders/orders.csv"
    df.to_csv(file_path, index=False)


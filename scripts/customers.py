import os
import pandas as pd
def extract_customers(conn):
    os.makedirs("data/customers", exist_ok=True)
    query = "SELECT * FROM CUSTOMERS"
    df = pd.read_sql(query, conn)
    file_path = "data/customers/customers.csv"
    df.to_csv(file_path, index=False)

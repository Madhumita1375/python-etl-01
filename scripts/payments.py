import os
import pandas as pd
def extract_payments(conn):
    os.makedirs("data/payments", exist_ok=True)
    query = "SELECT * FROM PAYMENTS"
    df = pd.read_sql(query, conn)
    file_path = "data/payments/payments.csv"
    df.to_csv(file_path, index=False)

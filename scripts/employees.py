import os
import pandas as pd
def extract_employees(conn):
    os.makedirs("data/employees", exist_ok=True)
    query = "SELECT * FROM EMPLOYEES"
    df = pd.read_sql(query, conn)
    file_path = "data/employees/employees.csv"
    df.to_csv(file_path, index=False)

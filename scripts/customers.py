import os
import pandas as pd
from dotenv import load_dotenv
from db_connection import get_connection

load_dotenv()

output_dir = os.getenv("OUTPUT_FILE")

os.makedirs(output_dir, exist_ok=True)

conn = get_connection()

query = """
SELECT CUSTOMERNUMBER,CUSTOMERNAME,CONTACTLASTNAME,CONTACTFIRSTNAME,PHONE,ADDRESSLINE1,ADDRESSLINE2,CITY,STATE,POSTALCODE,COUNTRY,SALESREPEMPLOYEENUMBER ,CREDITLIMIT FROM CUSTOMERS"""
df = pd.read_sql(query, conn)

file_path = os.path.join(output_dir, "customers.csv")


df.to_csv(file_path, index=False)

conn.close()

print(f"orders data saved at: {file_path}")


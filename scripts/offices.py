import os
import pandas as pd
from dotenv import load_dotenv
from db_connection import get_connection
output_dir = os.getenv("OUTPUT_FILE")
os.makedirs(output_dir, exist_ok=True)
conn = get_connection()
query = """
SELECT OFFICECODE, CITY, PHONE, ADDRESSLINE1, ADDRESSLINE2, STATE, COUNTRY, POSTALCODE, TERRITORY
FROM OFFICES
"""
df = pd.read_sql(query, conn)
file_path = os.path.join(output_dir, "offices.csv")
df.to_csv(file_path, index=False)
conn.close()
print(f"Offices data saved successfully at: {file_path}")

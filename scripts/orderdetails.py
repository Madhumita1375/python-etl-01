import os
import pandas as pd
from dotenv import load_dotenv
from db_connection import get_connection

output_dir = os.getenv("OUTPUT_FILE")
os.makedirs(output_dir, exist_ok=True)
conn = get_connection()
query = """
SELECT ORDERNUMBER, PRODUCTCODE, QUANTITYORDERED, PRICEEACH, ORDERLINENUMBER
FROM ORDERDETAILS
"""
df = pd.read_sql(query, conn)
file_path = os.path.join(output_dir, "orderdetails.csv")
df.to_csv(file_path, index=False)
conn.close()
print(f"OrderDetails data saved successfully at: {file_path}")

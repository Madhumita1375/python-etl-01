import os
import sys
import pandas as pd
import boto3
from io import StringIO
from dotenv import load_dotenv
from db_connection import get_connection

load_dotenv()

if len(sys.argv) < 2:
    print("Table name not provided. Exiting.")
    sys.exit(1)

table = sys.argv[1].lower()

s3_bucket = os.getenv("S3_BUCKET")
batch_date = os.getenv("BATCH_DATE")
schema=os.getenv('DB_SCHEMA')
table_columns = [
"OFFICECODE", "CITY", "PHONE", "ADDRESSLINE1", "ADDRESSLINE2", "STATE", "COUNTRY", "POSTALCODE", "TERRITORY", "CREATE_TIMESTAMP","UPDATE_TIMESTAMP"
    ]
columns_str = ", ".join(table_columns)

conn = get_connection()

query = f"""
SELECT {columns_str}
FROM {schema}.{table}
WHERE UPDATE_TIMESTAMP >= TO_DATE('{batch_date}', 'YYYY-MM-DD')
"""

df = pd.read_sql(query, conn)

csv_buffer = StringIO()
df.to_csv(csv_buffer, index=False)

s3 = boto3.client("s3")
s3_path = f"{table.upper()}/{batch_date}/{table}.csv"

s3.put_object(
    Bucket=s3_bucket,
    Key=s3_path,
    Body=csv_buffer.getvalue()
)

print(f"{table}.csv uploaded successfully to s3://{s3_bucket}/{s3_path}")

conn.close()

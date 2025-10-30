import os
import pandas as pd
import boto3
from io import StringIO
from db_connection import get_connection

s3_bucket = os.getenv("S3_BUCKET")
s3_path = os.getenv("S3_BUCKET_PATH")

conn = get_connection()
query = """
SELECT EMPLOYEENUMBER, LASTNAME, FIRSTNAME, EXTENSION, EMAIL, OFFICECODE, REPORTSTO, JOBTITLE 
FROM EMPLOYEES
"""
df = pd.read_sql(query, conn)

csv_buffer = StringIO()
df.to_csv(csv_buffer, index=False)

s3 = boto3.client("s3")
s3.put_object(
    Bucket=s3_bucket,
    Key=f"{s3_path}employees.csv",
    Body=csv_buffer.getvalue()
)

print(f"customers.csv uploaded successfully to s3://{s3_bucket}/{s3_path}employees.csv")

conn.close()
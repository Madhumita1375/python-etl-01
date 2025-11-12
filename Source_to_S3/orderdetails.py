
import os
import boto3
import csv
from io import StringIO
from dotenv import load_dotenv
from db_connection import get_oracle_connection

def export_orderdetails_to_s3():
    load_dotenv()

    table_name = "orderdetails"
    s3_bucket = os.getenv("S3_BUCKET")
    ETL_BATCH_DATE = os.getenv("ETL_BATCH_DATE")


    print(f"\nProcessing table: {table_name}")
    print(f"Batch Date: {ETL_BATCH_DATE}")

    table_columns = [
        "ORDERNUMBER", "PRODUCTCODE", "QUANTITYORDERED", "PRICEEACH", "ORDERLINENUMBER", "CREATE_TIMESTAMP","UPDATE_TIMESTAMP"
    ]
    columns_str = ", ".join(table_columns)
    conn = get_oracle_connection()

    cur = conn.cursor()

    query = f"""
    SELECT {columns_str}
    FROM {table_name}@madhu_test_dblink
    WHERE UPDATE_TIMESTAMP >= TO_DATE('{ETL_BATCH_DATE}', 'YYYY-MM-DD')
    """
    cur.execute(query)

    columns = [desc[0] for desc in cur.description]
    rows = cur.fetchall()
    conn.close()

    csv_buffer = StringIO()
    writer = csv.writer(csv_buffer, quoting=csv.QUOTE_MINIMAL)
    writer.writerow(columns)
    for row in rows:
        writer.writerow(row)

    s3 = boto3.client("s3", region_name="eu-north-1")
    s3_path = f"{table_name.upper()}/{ETL_BATCH_DATE}/{table_name}.csv"
    s3.put_object(Bucket=s3_bucket, Key=s3_path, Body=csv_buffer.getvalue())

    print(f"{table_name}.csv uploaded successfully to s3://{s3_bucket}/{s3_path}")

if __name__ == "__main__":
    export_orderdetails_to_s3()

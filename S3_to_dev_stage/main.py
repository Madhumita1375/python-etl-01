# main.py
import os
import sys
from dotenv import load_dotenv
from redshift_con import redshift_get_connection

load_dotenv()
redshift_schema_etl_metadata=os.getenv("REDSHIFT_SCHEMA_ETL")
redshift_date_table=os.getenv("REDSHIFT_TABLE_ETL")
batch_date_col="etl_batch_date"
def s3_to_redshift(table):
    s3_bucket = os.getenv("S3_BUCKET")
    ETL_BATCH_DATE = os.getenv("ETL_BATCH_DATE")
    region = os.getenv("AWS_REGION", "eu-north-1")
    iam_role = os.getenv("REDSHIFT_IAM_ROLE")
    schema = os.getenv("REDSHIFT_SCHEMA", "public")

    s3_path = f"s3://{s3_bucket}/{table.upper()}/{ETL_BATCH_DATE}/{table}.csv"

    print(f"Loading table: {schema}.{table}")
    print(f"Batch Date   : {ETL_BATCH_DATE}")
    print(f"S3 File      : {s3_path}")

    conn = redshift_get_connection()
    cur = conn.cursor()
    cur.execute(f"SELECT {batch_date_col} FROM {redshift_schema_etl_metadata}.{redshift_date_table}")
    result = cur.fetchone()
    ETL_BATCH_DATE = result[0].strftime("%Y-%m-%d")
    print(ETL_BATCH_DATE)
    with open(".env", "r") as f:
        lines = f.readlines()

    with open(".env", "w") as f:
        for line in lines:
            if line.startswith("ETL_BATCH_DATE"):
                f.write(f"ETL_BATCH_DATE={ETL_BATCH_DATE}\n")
            else:
                f.write(line)

    truncate_sql = f"TRUNCATE {schema}.{table};"

    copy_sql = f"""
    COPY {schema}.{table}
    FROM '{s3_path}'
    IAM_ROLE '{iam_role}'
    REGION '{region}'
    FORMAT AS CSV
    DELIMITER ','
    IGNOREHEADER 1
    TRUNCATECOLUMNS
    TIMEFORMAT 'auto'
    DATEFORMAT 'auto';
    """

    try:
        cur.execute(truncate_sql)
        cur.execute(copy_sql)
        conn.commit()
        print(f"Loaded table '{table}'")
    except Exception as e:
        conn.rollback()
        print(f"Error loading table '{table}': {e}")

    cur.close()
    conn.close()

def run_all_tables():
    tables = os.getenv("TABLE_LIST")
    tables = [t.strip().lower() for t in tables.split(",")] if tables else []

    if not tables:
        print("No tables found in .env TABLE_LIST.")
        sys.exit(1)

    for table in tables:
        s3_to_redshift(table)

    print("\nAll tables completed.\n")

if __name__ == "__main__":
    run_all_tables()

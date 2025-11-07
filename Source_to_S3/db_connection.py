import os,psycopg2
import oracledb
from dotenv import load_dotenv
from datetime import datetime, timedelta
import sys

load_dotenv()

redshift_schema_etl_metadata=os.getenv("REDSHIFT_SCHEMA_ETL")
redshift_date_table=os.getenv("REDSHIFT_TABLE_ETL")
batch_date_col="etl_batch_date"

def get_batch_date_from_redshift(table):
    conn = psycopg2.connect(
        host=os.getenv("REDSHIFT_HOST"),
        port=os.getenv("REDSHIFT_PORT"),
        dbname=os.getenv("REDSHIFT_DB"),
        user=os.getenv("REDSHIFT_USER"),
        password=os.getenv("REDSHIFT_PASSWORD")
    )
    cur = conn.cursor()
    cur.execute(f"SELECT {batch_date_col} FROM {redshift_schema_etl_metadata}.{redshift_date_table}")
    result = cur.fetchone()
    conn.close()
    return result[0].strftime("%Y-%m-%d")

def update_env_batch_date(batch_date):
    with open(".env", "r") as f:
        lines = f.readlines()

    with open(".env", "w") as f:
        for line in lines:
            if line.startswith("BATCH_DATE"):
                f.write(f"BATCH_DATE={batch_date}\n")
            else:
                f.write(line)

def get_connection():
    db_user = os.getenv("DB_USER")
    db_password = os.getenv("DB_PASSWORD")
    db_dsn = os.getenv("DB_DSN")
    batch_date = os.getenv("BATCH_DATE")

    
    if batch_date == "2001-01-01":
        db_link_date = datetime.strptime("2005-06-09", "%Y-%m-%d")
    else:
        db_link_date = datetime.strptime(batch_date, "%Y-%m-%d")

    print(f"DB Link Date: {db_link_date.strftime('%Y-%m-%d')}")

    schema_name = f"CM_{db_link_date.strftime('%Y%m%d')}"
    schema_password = f"{schema_name}123"

    conn = oracledb.connect(user=db_user, password=db_password, dsn=db_dsn)
    cur = conn.cursor()

    drop_dblink_sql = """
    BEGIN
        EXECUTE IMMEDIATE 'DROP PUBLIC DATABASE LINK madhu_test_dblink';
    EXCEPTION WHEN OTHERS THEN
        IF SQLCODE != -2024 THEN
            RAISE;
        END IF;
    END;
    """

    create_dblink_sql = f"""
    BEGIN
        EXECUTE IMMEDIATE q'[CREATE PUBLIC DATABASE LINK madhu_test_dblink
        CONNECT TO {schema_name} IDENTIFIED BY "{schema_password}"
        USING '(DESCRIPTION=
            (ADDRESS=(PROTOCOL=TCP)
                     (HOST=classicmodels-2025.cvm8ii9txcwr.us-east-1.rds.amazonaws.com)
                     (PORT=1521))
            (CONNECT_DATA=(SERVICE_NAME=ORCL))
        )']';
    END;
    """

    try:
        cur.execute(drop_dblink_sql)
        cur.execute(create_dblink_sql)
        conn.commit()
        print(f"Database link created successfully for schema {schema_name}")
    except Exception as e:
        print(f"Error creating DB link: {e}")
        conn.close()
        sys.exit(1)

    return conn



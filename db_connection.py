import os
import oracledb
from dotenv import load_dotenv

load_dotenv()

def get_connection():
    dsn = oracledb.makedsn(
        os.getenv("DB_HOST"),
        os.getenv("DB_PORT"),
        service_name=os.getenv("DB_SERVICE")
    )
    conn = oracledb.connect(
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        dsn=dsn
    )
    schema = os.getenv("DB_SCHEMA")
    if schema:
        with conn.cursor() as cursor:
            cursor.execute(f"ALTER SESSION SET CURRENT_SCHEMA={schema}")
    return conn





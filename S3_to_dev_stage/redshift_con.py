
import psycopg2, os
from dotenv import load_dotenv
import redshift_connector

load_dotenv()
def redshift_get_connection():
    try:
        conn = psycopg2.connect(
            host=os.getenv("REDSHIFT_HOST"),
            port=os.getenv("REDSHIFT_PORT"),
            dbname=os.getenv("REDSHIFT_DB"),
            user=os.getenv("REDSHIFT_USER"),
            password=os.getenv("REDSHIFT_PASSWORD")
        )        
    except Exception as e:
        conn.close()
        print(f"Connection failed: {e}")
    return conn 
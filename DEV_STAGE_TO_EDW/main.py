import os,sys
import subprocess
from dotenv import load_dotenv
import psycopg2

load_dotenv()

# --- Redshift connection params ---
REDSHIFT_HOST = os.getenv("REDSHIFT_HOST")
REDSHIFT_PORT = os.getenv("REDSHIFT_PORT")
REDSHIFT_DB = os.getenv("REDSHIFT_DB")
REDSHIFT_USER = os.getenv("REDSHIFT_USER")
REDSHIFT_PASSWORD = os.getenv("REDSHIFT_PASSWORD")
REDSHIFT_SCHEMA_ETL = os.getenv("REDSHIFT_SCHEMA_ETL")
REDSHIFT_TABLE_ETL = os.getenv("REDSHIFT_TABLE_ETL")

def get_etl_batch_details():
    try:
        conn = psycopg2.connect(
            host=REDSHIFT_HOST,
            port=REDSHIFT_PORT,
            dbname=REDSHIFT_DB,
            user=REDSHIFT_USER,
            password=REDSHIFT_PASSWORD
        )
        cur = conn.cursor()
        query = f"""
            SELECT etl_batch_no, etl_batch_date
            FROM {REDSHIFT_SCHEMA_ETL}.{REDSHIFT_TABLE_ETL}
            ORDER BY etl_batch_no DESC LIMIT 1;
        """
        cur.execute(query)
        result = cur.fetchone()
        cur.close()
        conn.close()

        if not result:
            print("No batch info found in ETL metadata table.")
            sys.exit(1)

        etl_batch_no, etl_batch_date = result
        return int(etl_batch_no), etl_batch_date.strftime("%Y-%m-%d")

    except Exception as e:
        print(f"Error fetching batch info: {e}")
        sys.exit(1)

def update_env(etl_batch_no, etl_batch_date):
    env_file = ".env"
    lines = []
    with open(env_file, "r") as f:
        for line in f.readlines():
            if line.startswith("ETL_BATCH_NO"):
                line = f"ETL_BATCH_NO={etl_batch_no}\n"
            elif line.startswith("ETL_BATCH_DATE"):
                line = f"ETL_BATCH_DATE={etl_batch_date}\n"
            lines.append(line)
    with open(env_file, "w") as f:
        f.writelines(lines)
    print(f"Updated .env with ETL_BATCH_NO={etl_batch_no}, ETL_BATCH_DATE={etl_batch_date}\n")

def run_etl_for_table(table_name):
    etl_batch_no, etl_batch_date = get_etl_batch_details()
    update_env(etl_batch_no, etl_batch_date)
    script_path = f"DEV_STAGE_TO_EDW/{table_name.lower()}.py"
    if not os.path.exists(script_path):
        print(f"Script not found for table: {table_name}")
        return

    print(f"Running ETL for table: {table_name}")
    try:
        subprocess.run(["python", script_path], check=True)
        print(f"{table_name}.py executed successfully.\n")
    except subprocess.CalledProcessError as e:
        print(f" Error while executing {table_name}.py: {e}\n")

def run_etl_scripts():
    # Fetch batch context
    etl_batch_no, etl_batch_date = get_etl_batch_details()
    print(etl_batch_no, etl_batch_date)
    if not etl_batch_no or not etl_batch_date:
        print("!!!!!!!Missing ETL batch context — exiting.")
        return

    # Read TABLE_LIST_DW from .env
    tables = os.getenv("TABLE_LIST_DW")
    print(tables)
    tables = [t.strip().lower() for t in tables.split(",")] if tables else []

    if not tables:
        print("No TABLE_LIST_DW found in .env.")
        return

    print(f"\nStarting Stage → DW ETL Load")
    print(f"Batch No: {etl_batch_no} | Batch Date: {etl_batch_date}\n")

    for table in tables:
        run_etl_for_table(table)

    print("All Stage → DW ETL scripts executed successfully.\n")

if __name__ == "__main__":
    run_etl_scripts()

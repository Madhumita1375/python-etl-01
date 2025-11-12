
import os
import subprocess
from dotenv import load_dotenv
from pathlib import Path
from db_connection import create_or_reuse_db_link
from db_connection import get_batch_date_from_redshift, update_env_batch_date

load_dotenv()

tables = os.getenv("TABLE_LIST")
tables = [t.strip().lower() for t in tables.split(",")] if tables else []

ETL_BATCH_DATE = os.getenv("ETL_BATCH_DATE")

def main():
    if not tables:
        print("No tables found in .env (TABLE_LIST).")
        return
    ETL_BATCH_DATE = get_batch_date_from_redshift(tables)
    update_env_batch_date(ETL_BATCH_DATE)
    print(f"Initializing DB link for ETL batch date: {ETL_BATCH_DATE}")
    create_or_reuse_db_link()

    processes = []
    for table in tables:
        script_path = Path(f"Source_to_S3/{table.lower()}.py")
        if script_path.exists():
            p = subprocess.Popen(["python", str(script_path)])
            processes.append((table, p))
            print(f"Started {table}")
        else:
            print(f"Script not found for table '{table}'")

    for table, p in processes:
        p.wait()

    print("\nAll ETL jobs finished successfully.")

if __name__ == "__main__":
    main()



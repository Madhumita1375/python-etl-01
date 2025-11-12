import os
import sys
import subprocess
from pathlib import Path
import psycopg2
from dotenv import load_dotenv
from datetime import datetime
from Batch_log.batch_log import log_batch_start, log_batch_success, log_batch_failure

load_dotenv()

REDSHIFT_HOST = os.getenv("REDSHIFT_HOST")
REDSHIFT_PORT = os.getenv("REDSHIFT_PORT")
REDSHIFT_DB = os.getenv("REDSHIFT_DB")
REDSHIFT_USER = os.getenv("REDSHIFT_USER")
REDSHIFT_PASSWORD = os.getenv("REDSHIFT_PASSWORD")
REDSHIFT_SCHEMA_ETL = os.getenv("REDSHIFT_SCHEMA_ETL")
REDSHIFT_TABLE_ETL = os.getenv("REDSHIFT_TABLE_ETL")

def get_batch_info():
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


def run_pipeline(script_path):
    print(f"Running pipeline: {script_path}")
    subprocess.run(["python", str(script_path)], check=True)
    print(f"Completed {script_path}\n")


def main():

    etl_batch_no, etl_batch_date = get_batch_info()
    update_env(etl_batch_no, etl_batch_date)

    log_batch_start(etl_batch_no, etl_batch_date)

    pipelines = [
        "Source_to_S3/main.py",
        "S3_to_dev_stage/main.py",
        "DEV_STAGE_TO_EDW/main.py"
    ]

    try:
        for pipeline in pipelines:
            pipeline_path = Path(pipeline)
            if pipeline_path.exists():
                run_pipeline(pipeline_path)
            else:
                print(f"Skipping missing pipeline: {pipeline_path}")

        log_batch_success(etl_batch_no)
        print(" All ETL pipelines executed successfully.\n")

    except subprocess.CalledProcessError as e:
        print(f" Error while running pipeline: {e}")
        log_batch_failure(etl_batch_no)
        sys.exit(1)

    except Exception as e:
        print(f" Unexpected error: {e}")
        log_batch_failure(etl_batch_no)
        sys.exit(1)


if __name__ == "__main__":
    main()

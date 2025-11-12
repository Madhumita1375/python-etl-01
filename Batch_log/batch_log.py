import os
import psycopg2
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

def get_redshift_conn():
    return psycopg2.connect(
        host=os.getenv("REDSHIFT_HOST"),
        port=os.getenv("REDSHIFT_PORT"),
        dbname=os.getenv("REDSHIFT_DB"),
        user=os.getenv("REDSHIFT_USER"),
        password=os.getenv("REDSHIFT_PASSWORD")
    )

def log_batch_start(batch_no, ETL_BATCH_DATE):
    try:
        conn = get_redshift_conn()
        cur = conn.cursor()

        check_sql = f"""
            SELECT COUNT(*) 
            FROM j25madhumita_etl_metadata.batch_control_log
            WHERE etl_batch_no = {batch_no};
        """
        cur.execute(check_sql)
        exists = cur.fetchone()[0] > 0

        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        if exists:
            update_sql = f"""
                UPDATE j25madhumita_etl_metadata.batch_control_log
                SET etl_batch_status = 'R',  -- Running
                    etl_batch_start_time = '{current_time}',
                    etl_batch_end_time = NULL
                WHERE etl_batch_no = {batch_no};
            """
            cur.execute(update_sql)
            print(f"Batch {batch_no} restarted at {current_time}")
        else:
            insert_sql = f"""
                INSERT INTO j25madhumita_etl_metadata.batch_control_log
                (etl_batch_no, etl_batch_date, etl_batch_status, etl_batch_start_time)
                VALUES ({batch_no}, '{ETL_BATCH_DATE}', 'R', '{current_time}');
            """
            cur.execute(insert_sql)
            print(f" Batch {batch_no} started at {current_time}")

        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"Error logging batch start: {e}")

def log_batch_success(batch_no):
    try:
        conn = get_redshift_conn()
        cur = conn.cursor()

        end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        update_sql = f"""
            UPDATE j25madhumita_etl_metadata.batch_control_log
            SET etl_batch_status = 'C',  -- Completed
                etl_batch_end_time = '{end_time}'
            WHERE etl_batch_no = {batch_no};
        """
        cur.execute(update_sql)
        conn.commit()
        print(f"Batch {batch_no} completed at {end_time}")

        cur.close()
        conn.close()
    except Exception as e:
        print(f"Error logging batch success: {e}")

def log_batch_failure(batch_no):
    try:
        conn = get_redshift_conn()
        cur = conn.cursor()

        end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        update_sql = f"""
            UPDATE j25madhumita_etl_metadata.batch_control_log
            SET etl_batch_status = 'F',  -- Failed
                etl_batch_end_time = '{end_time}'
            WHERE etl_batch_no = {batch_no};
        """
        cur.execute(update_sql)
        conn.commit()
        print(f"Batch {batch_no} failed at {end_time}")

        cur.close()
        conn.close()
    except Exception as e:
        print(f"Error logging batch failure: {e}")
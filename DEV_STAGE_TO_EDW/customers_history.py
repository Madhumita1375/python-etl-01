import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

ETL_BATCH_NO = os.getenv("ETL_BATCH_NO")
ETL_BATCH_DATE = os.getenv("ETL_BATCH_DATE")
REDSHIFT_SCHEMA_DW=os.getenv("REDSHIFT_SCHEMA_DW")


try:
    conn = psycopg2.connect(
        host=os.getenv("REDSHIFT_HOST"),
        port=os.getenv("REDSHIFT_PORT"),
        dbname=os.getenv("REDSHIFT_DB"),
        user=os.getenv("REDSHIFT_USER"),
        password=os.getenv("REDSHIFT_PASSWORD")
    )
    cur = conn.cursor()
    print("Connected to Redshift successfully!\n")
    update_sql = f"""
    UPDATE {REDSHIFT_SCHEMA_DW}.customer_history AS hist
    SET 
        dw_active_record_ind = 0,
        effective_to_date = DATEADD(day, -1, '{ETL_BATCH_DATE}'),
        update_etl_batch_no = {ETL_BATCH_NO},
        update_etl_batch_date = '{ETL_BATCH_DATE}',
        dw_update_timestamp = GETDATE()
    FROM {REDSHIFT_SCHEMA_DW}.customers AS cust
    WHERE 
        hist.dw_active_record_ind = 1
        AND hist.dw_customer_id = cust.dw_customer_id
        AND hist.creditLimit <> cust.creditLimit;
    """
    cur.execute(update_sql)
    #print(f"Updated {cur.rowcount} old active records in customer_history.\n")

    insert_sql = f"""
    INSERT INTO {REDSHIFT_SCHEMA_DW}.customer_history (
        dw_customer_id,
        creditLimit,
        effective_from_date,
        dw_active_record_ind,
        dw_create_timestamp,
        dw_update_timestamp,
        create_etl_batch_no,
        create_etl_batch_date
    )
    SELECT 
        cust.dw_customer_id,
        cust.creditLimit,
        '{ETL_BATCH_DATE}' AS effective_from_date,
        1 AS dw_active_record_ind,
        GETDATE() AS dw_create_timestamp,
        GETDATE() AS dw_update_timestamp,
        {ETL_BATCH_NO} AS create_etl_batch_no,
        '{ETL_BATCH_DATE}' AS create_etl_batch_date
    FROM {REDSHIFT_SCHEMA_DW}.customers AS cust
    LEFT JOIN {REDSHIFT_SCHEMA_DW}.customer_history AS hist
        ON cust.dw_customer_id = hist.dw_customer_id
        AND hist.dw_active_record_ind = 1
    WHERE 
        hist.dw_customer_id IS NULL 
        OR cust.creditLimit <> hist.creditLimit;
    """
    cur.execute(insert_sql)
    #print(f"Inserted {cur.rowcount} new records into customer_history.\n")

    conn.commit()

except Exception as e:
    conn.rollback()
    print(f"Error during customer_history ETL: {e}")

finally:
    cur.close()
    conn.close()

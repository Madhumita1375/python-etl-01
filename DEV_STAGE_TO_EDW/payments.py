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

    insert_sql = f"""
        INSERT INTO {REDSHIFT_SCHEMA_DW}.payments (
            src_customerNumber,
            checkNumber,
            paymentDate,
            amount,
            src_create_timestamp,
            src_update_timestamp,
            etl_batch_no,
            etl_batch_date
        )
        SELECT
            s.customerNumber,
            s.checkNumber,
            s.paymentDate,
            s.amount,
            s.create_timestamp,
            s.update_timestamp,
            {ETL_BATCH_NO} AS etl_batch_no,
            '{ETL_BATCH_DATE}' AS etl_batch_date
        FROM j25madhumita_devstage.payments s
        LEFT JOIN {REDSHIFT_SCHEMA_DW}.payments p
            ON s.checkNumber = p.checkNumber
        WHERE p.checkNumber IS NULL;
    """
    cur.execute(insert_sql)
    #print(f"Inserted {cur.rowcount} new records into DW.\n")

    update_sql = f"""
        WITH updated AS (
            SELECT
                d.src_customerNumber,
                d.checkNumber,
                s.paymentDate,
                s.amount,
                s.update_timestamp AS src_update_timestamp
            FROM j25madhumita_devstage.payments s
            JOIN {REDSHIFT_SCHEMA_DW}.payments d
                ON s.customerNumber = d.src_customerNumber AND s.checkNumber = d.checkNumber
            WHERE s.update_timestamp > d.src_update_timestamp
        )
        UPDATE {REDSHIFT_SCHEMA_DW}.payments
        SET
            paymentDate = u.paymentDate,
            amount = u.amount,
            src_update_timestamp = u.src_update_timestamp,
            etl_batch_no = {ETL_BATCH_NO},
            etl_batch_date = '{ETL_BATCH_DATE}'
        FROM updated u
        WHERE {REDSHIFT_SCHEMA_DW}.payments.src_customerNumber = u.src_customerNumber
          AND {REDSHIFT_SCHEMA_DW}.payments.checkNumber = u.checkNumber;
    """
    cur.execute(update_sql)
    print(f"Updated {cur.rowcount} existing records in DW.\n")

    update_customer_ref = f"""
        UPDATE {REDSHIFT_SCHEMA_DW}.payments
        SET dw_customer_id = c.dw_customer_id
        FROM {REDSHIFT_SCHEMA_DW}.customers c
        WHERE {REDSHIFT_SCHEMA_DW}.payments.src_customerNumber = c.src_customerNumber;
    """
    cur.execute(update_customer_ref)
    #print(f"Updated {cur.rowcount} customer references in DW.\n")

    conn.commit()

except Exception as e:
    conn.rollback()
    print(f"Error during DW load: {e}")

finally:
    cur.close()
    conn.close()

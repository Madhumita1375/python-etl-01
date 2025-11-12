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
        INSERT INTO {REDSHIFT_SCHEMA_DW}.orders (
            src_orderNumber,
            orderDate,
            requiredDate,
            shippedDate,
            status,
            comments,
            src_customerNumber,
            src_create_timestamp,
            src_update_timestamp,
            cancelledDate,
            etl_batch_no,
            etl_batch_date
        )
        SELECT
            s.orderNumber,
            s.orderDate,
            s.requiredDate,
            s.shippedDate,
            s.status,
            s.comments,
            s.customerNumber,
            s.create_timestamp,
            s.update_timestamp,
            s.cancelledDate,
            {ETL_BATCH_NO} AS etl_batch_no,
            '{ETL_BATCH_DATE}' AS etl_batch_date
        FROM j25madhumita_devstage.orders s
        LEFT JOIN {REDSHIFT_SCHEMA_DW}.orders o
            ON s.orderNumber = o.src_orderNumber
        WHERE o.src_orderNumber IS NULL;
    """
    cur.execute(insert_sql)
    #print(f"Inserted {cur.rowcount} new records into DW.\n")

    update_sql = f"""
        WITH updated AS (
            SELECT
                d.src_orderNumber,
                s.orderDate,
                s.requiredDate,
                s.shippedDate,
                s.status,
                s.comments,
                s.update_timestamp AS src_update_timestamp,
                s.cancelledDate
            FROM j25madhumita_devstage.orders s
            JOIN {REDSHIFT_SCHEMA_DW}.orders d
                ON s.orderNumber = d.src_orderNumber
            WHERE s.update_timestamp > d.src_update_timestamp
        )
        UPDATE {REDSHIFT_SCHEMA_DW}.orders
        SET
            orderDate = u.orderDate,
            requiredDate = u.requiredDate,
            shippedDate = u.shippedDate,
            status = u.status,
            comments = u.comments,
            src_update_timestamp = u.src_update_timestamp,
            cancelledDate = u.cancelledDate,
            etl_batch_no = {ETL_BATCH_NO},
            etl_batch_date = '{ETL_BATCH_DATE}'
        FROM updated u
        WHERE {REDSHIFT_SCHEMA_DW}.orders.src_orderNumber = u.src_orderNumber;
    """
    cur.execute(update_sql)
    #print(f"Updated {cur.rowcount} existing records in DW.\n")

    update_customer_ref = f"""
        UPDATE {REDSHIFT_SCHEMA_DW}.orders
        SET dw_customer_id = c.dw_customer_id
        FROM {REDSHIFT_SCHEMA_DW}.customers c
        WHERE {REDSHIFT_SCHEMA_DW}.orders.src_customerNumber = c.src_customerNumber;
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

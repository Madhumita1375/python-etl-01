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
        INSERT INTO {REDSHIFT_SCHEMA_DW}.orderdetails (
            src_orderNumber,
            src_productCode,
            quantityOrdered,
            priceEach,
            orderLineNumber,
            src_create_timestamp,
            src_update_timestamp,
            etl_batch_no,
            etl_batch_date
        )
        SELECT
            s.orderNumber,
            s.productCode,
            s.quantityOrdered,
            s.priceEach,
            s.orderLineNumber,
            s.create_timestamp,
            s.update_timestamp,
            {ETL_BATCH_NO} AS etl_batch_no,
            '{ETL_BATCH_DATE}' AS etl_batch_date
        FROM j25madhumita_devstage.orderdetails s
        LEFT JOIN {REDSHIFT_SCHEMA_DW}.orderdetails o
            ON s.orderNumber = o.src_orderNumber AND s.productCode = o.src_productCode
        WHERE o.src_orderNumber IS NULL AND o.src_productCode IS NULL;
    """
    cur.execute(insert_sql)
    #print(f"Inserted {cur.rowcount} new records into DW.\n")

    update_sql = f"""
        WITH updated AS (
            SELECT
                d.src_orderNumber,
                d.src_productCode,
                s.quantityOrdered,
                s.priceEach,
                s.orderLineNumber,
                s.update_timestamp AS src_update_timestamp
            FROM j25madhumita_devstage.orderdetails s
            JOIN {REDSHIFT_SCHEMA_DW}.orderdetails d
                ON s.orderNumber = d.src_orderNumber AND s.productCode = d.src_productCode
            WHERE s.update_timestamp > d.src_update_timestamp
        )
        UPDATE {REDSHIFT_SCHEMA_DW}.orderdetails
        SET
            quantityOrdered = u.quantityOrdered,
            priceEach = u.priceEach,
            orderLineNumber = u.orderLineNumber,
            src_update_timestamp = u.src_update_timestamp,
            etl_batch_no = {ETL_BATCH_NO},
            etl_batch_date = '{ETL_BATCH_DATE}'
        FROM updated u
        WHERE {REDSHIFT_SCHEMA_DW}.orderdetails.src_orderNumber = u.src_orderNumber
          AND {REDSHIFT_SCHEMA_DW}.orderdetails.src_productCode = u.src_productCode;
    """
    cur.execute(update_sql)
    #print(f"Updated {cur.rowcount} existing records in DW.\n")

    update_order_ref = f"""
        UPDATE {REDSHIFT_SCHEMA_DW}.orderdetails
        SET dw_order_id = o.dw_order_id
        FROM {REDSHIFT_SCHEMA_DW}.orders o
        WHERE {REDSHIFT_SCHEMA_DW}.orderdetails.src_orderNumber = o.src_orderNumber;
    """
    cur.execute(update_order_ref)
    #print(f"Updated {cur.rowcount} order references in DW.\n")

    update_product_ref = f"""
        UPDATE {REDSHIFT_SCHEMA_DW}.orderdetails
        SET dw_product_id = p.dw_product_id
        FROM {REDSHIFT_SCHEMA_DW}.products p
        WHERE {REDSHIFT_SCHEMA_DW}.orderdetails.src_productCode = p.src_productCode;
    """
    cur.execute(update_product_ref)
    #print(f"Updated {cur.rowcount} product references in DW.\n")

    conn.commit()

except Exception as e:
    conn.rollback()
    print(f"Error during DW load: {e}")

finally:
    cur.close()
    conn.close()

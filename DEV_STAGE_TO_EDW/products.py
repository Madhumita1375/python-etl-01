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
        INSERT INTO {REDSHIFT_SCHEMA_DW}.products (
            src_productCode,
            productName,
            productLine,
            productScale,
            productVendor,
            productDescription,
            quantityInStock,
            buyPrice,
            MSRP,
            src_create_timestamp,
            src_update_timestamp,
            etl_batch_no,
            etl_batch_date
        )
        SELECT
            s.productCode,
            s.productName,
            s.productLine,
            s.productScale,
            s.productVendor,
            s.productDescription,
            s.quantityInStock,
            s.buyPrice,
            s.MSRP,
            s.create_timestamp,
            s.update_timestamp,
            {ETL_BATCH_NO} AS etl_batch_no,
            '{ETL_BATCH_DATE}' AS etl_batch_date
        FROM j25madhumita_devstage.products s
        LEFT JOIN {REDSHIFT_SCHEMA_DW}.products p
            ON s.productCode = p.src_productCode
        WHERE p.src_productCode IS NULL;
    """
    cur.execute(insert_sql)
    #print(f"Inserted {cur.rowcount} new records into DW.\n")

    update_sql = f"""
        WITH updated AS (
            SELECT
                d.src_productCode,
                s.productName,
                s.productLine,
                s.productScale,
                s.productVendor,
                s.productDescription,
                s.quantityInStock,
                s.buyPrice,
                s.MSRP,
                s.update_timestamp AS src_update_timestamp
            FROM j25madhumita_devstage.products s
            JOIN {REDSHIFT_SCHEMA_DW}.products d
                ON s.productCode = d.src_productCode
            WHERE s.update_timestamp > d.src_update_timestamp
        )
        UPDATE {REDSHIFT_SCHEMA_DW}.products
        SET
            productName = u.productName,
            productLine = u.productLine,
            productScale = u.productScale,
            productVendor = u.productVendor,
            productDescription = u.productDescription,
            quantityInStock = u.quantityInStock,
            buyPrice = u.buyPrice,
            MSRP = u.MSRP,
            src_update_timestamp = u.src_update_timestamp,
            etl_batch_no = {ETL_BATCH_NO},
            etl_batch_date = '{ETL_BATCH_DATE}'
        FROM updated u
        WHERE {REDSHIFT_SCHEMA_DW}.products.src_productCode = u.src_productCode;
    """
    cur.execute(update_sql)
    #print(f"Updated {cur.rowcount} existing records in DW.\n")

    update_query  = f"""
        UPDATE {REDSHIFT_SCHEMA_DW}.products
        SET dw_product_line_id = pl.dw_product_line_id
        FROM {REDSHIFT_SCHEMA_DW}.productlines pl
        WHERE {REDSHIFT_SCHEMA_DW}.products.productLine = pl.productLine;
    """
    cur.execute(update_query )
    #print(f"Updated {cur.rowcount} product line references in DW.\n")

    conn.commit()

except Exception as e:
    conn.rollback()
    print(f"Error during DW load: {e}")

finally:
    cur.close()
    conn.close()

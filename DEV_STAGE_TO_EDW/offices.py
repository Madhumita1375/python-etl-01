import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

ETL_BATCH_NO = os.getenv("ETL_BATCH_NO")
ETL_BATCH_DATE = os.getenv("ETL_BATCH_DATE")

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
    insert_sql = f"""
        INSERT INTO j25madhumita_devdw.offices (
            officeCode,
            city,
            phone,
            addressLine1,
            addressLine2,
            state,
            country,
            postalCode,
            territory,
            src_create_timestamp,
            src_update_timestamp,
            etl_batch_no,
            etl_batch_date
        )
        SELECT
            s.officeCode,
            s.city,
            s.phone,
            s.addressLine1,
            s.addressLine2,
            s.state,
            s.country,
            s.postalCode,
            s.territory,
            s.create_timestamp AS src_create_timestamp,
            s.update_timestamp AS src_update_timestamp,
            {ETL_BATCH_NO} AS etl_batch_no,
            '{ETL_BATCH_DATE}' AS etl_batch_date
        FROM j25madhumita_devstage.offices s
        LEFT JOIN j25madhumita_devdw.offices o
            ON s.officeCode = o.officeCode
        WHERE o.officeCode IS NULL;
    """
    cur.execute(insert_sql)
    print(f"Inserted {cur.rowcount} new records into DW.\n")

    update_sql = f"""
        WITH updated AS (
            SELECT
                d.officeCode AS dw_officeCode,
                s.city,
                s.phone,
                s.addressLine1,
                s.addressLine2,
                s.state,
                s.country,
                s.postalCode,
                s.territory,
                s.update_timestamp AS src_update_timestamp
            FROM j25madhumita_devstage.offices s
            JOIN j25madhumita_devdw.offices d
                ON s.officeCode = d.officeCode
            WHERE s.update_timestamp > d.src_update_timestamp
        )
        UPDATE j25madhumita_devdw.offices
        SET
            city = u.city,
            phone = u.phone,
            addressLine1 = u.addressLine1,
            addressLine2 = u.addressLine2,
            state = u.state,
            country = u.country,
            postalCode = u.postalCode,
            territory = u.territory,
            src_update_timestamp = u.src_update_timestamp,
            etl_batch_no = {ETL_BATCH_NO},
            etl_batch_date = '{ETL_BATCH_DATE}'
        FROM updated u
        WHERE j25madhumita_devdw.offices.officeCode = u.dw_officeCode;
    """
    cur.execute(update_sql)
    print(f"Updated {cur.rowcount} existing records in DW.\n")

    conn.commit()

except Exception as e:
    conn.rollback()
    print(f"Error during DW load: {e}")

finally:
    cur.close()
    conn.close()
    print("Connection closed.")

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
    #print("Connected to Redshift successfully!\n")

    insert_sql = f"""
    INSERT INTO {REDSHIFT_SCHEMA_DW}.customers (
        src_customerNumber,
        customerName,
        contactLastName,
        contactFirstName,
        phone,
        addressLine1,
        addressLine2,
        city,
        state,
        postalCode,
        country,
        salesRepEmployeeNumber,
        creditLimit,
        src_create_timestamp,
        src_update_timestamp,
        etl_batch_no,
        etl_batch_date
    )
    SELECT
        s.customerNumber AS src_customerNumber,
        s.customerName,
        s.contactLastName,
        s.contactFirstName,
        s.phone,
        s.addressLine1,
        s.addressLine2,
        s.city,
        s.state,
        s.postalCode,
        s.country,
        s.salesRepEmployeeNumber,
        s.creditLimit,
        s.create_timestamp AS src_create_timestamp,
        s.update_timestamp AS src_update_timestamp,
        {ETL_BATCH_NO} AS etl_batch_no,
        '{ETL_BATCH_DATE}' AS etl_batch_date
    FROM j25madhumita_devstage.customers AS s
    LEFT JOIN {REDSHIFT_SCHEMA_DW}.customers AS c
        ON s.customerNumber = c.src_customerNumber
    WHERE c.src_customerNumber IS NULL;
    """
    cur.execute(insert_sql)
    #print(f"Inserted {cur.rowcount} new customer records into DW.\n")

    update_sql = f"""
    UPDATE {REDSHIFT_SCHEMA_DW}.customers AS d
    SET
        customerName = s.customerName,
        contactLastName = s.contactLastName,
        contactFirstName = s.contactFirstName,
        phone = s.phone,
        addressLine1 = s.addressLine1,
        addressLine2 = s.addressLine2,
        city = s.city,
        state = s.state,
        postalCode = s.postalCode,
        country = s.country,
        salesRepEmployeeNumber = s.salesRepEmployeeNumber,
        creditLimit = s.creditLimit,
        src_update_timestamp = s.update_timestamp,
        etl_batch_no = {ETL_BATCH_NO},
        etl_batch_date = '{ETL_BATCH_DATE}'
    FROM j25madhumita_devstage.customers AS s
    WHERE d.src_customerNumber = s.customerNumber
      AND s.update_timestamp > d.src_update_timestamp;
    """
    cur.execute(update_sql)
    #print(f"Updated {cur.rowcount} existing customer records in DW.\n")

    update_fk_sql = f"""
    UPDATE {REDSHIFT_SCHEMA_DW}.customers AS c
    SET dw_employee_id = e.dw_employee_id
    FROM {REDSHIFT_SCHEMA_DW}.employees AS e
    WHERE c.salesRepEmployeeNumber = e.employeeNumber;
    """
    cur.execute(update_fk_sql)
    #print(f" Updated {cur.rowcount} employee references in DW.\n")

    conn.commit()

except Exception as e:
    conn.rollback()
    print(f"Error during CUSTOMER ETL: {e}")

finally:
    cur.close()
    conn.close()

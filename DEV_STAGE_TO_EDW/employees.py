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
    print("Connected to Redshift successfully\n")

    insert_sql = f"""
        INSERT INTO j25madhumita_devdw.employees (
            employeeNumber,
            lastName,
            firstName,
            extension,
            email,
            officeCode,
            reportsTo,
            jobTitle,
            src_create_timestamp,
            src_update_timestamp,
            etl_batch_no,
            etl_batch_date
        )
        SELECT
            s.employeeNumber,
            s.lastName,
            s.firstName,
            s.extension,
            s.email,
            s.officeCode,
            s.reportsTo,
            s.jobTitle,
            s.create_timestamp AS src_create_timestamp,
            s.update_timestamp AS src_update_timestamp,
            {ETL_BATCH_NO} AS etl_batch_no,
            '{ETL_BATCH_DATE}' AS etl_batch_date
        FROM j25madhumita_devstage.employees s
        LEFT JOIN j25madhumita_devdw.employees e
            ON s.employeeNumber = e.employeeNumber
        WHERE e.employeeNumber IS NULL;
    """
    cur.execute(insert_sql)
    print(f"Inserted {cur.rowcount} new records into DW.\n")

    update_sql = f"""
        WITH updated AS (
            SELECT
                d.employeeNumber AS dw_employeeNumber,
                s.lastName,
                s.firstName,
                s.extension,
                s.email,
                s.officeCode,
                s.reportsTo,
                s.jobTitle,
                s.update_timestamp AS src_update_timestamp
            FROM j25madhumita_devstage.employees s
            JOIN j25madhumita_devdw.employees d
                ON s.employeeNumber = d.employeeNumber
            WHERE s.update_timestamp > d.src_update_timestamp
        )
        UPDATE j25madhumita_devdw.employees
        SET
            lastName = u.lastName,
            firstName = u.firstName,
            extension = u.extension,
            email = u.email,
            officeCode = u.officeCode,
            reportsTo = u.reportsTo,
            jobTitle = u.jobTitle,
            src_update_timestamp = u.src_update_timestamp,
            etl_batch_no = {ETL_BATCH_NO},
            etl_batch_date = '{ETL_BATCH_DATE}'
        FROM updated u
        WHERE j25madhumita_devdw.employees.employeeNumber = u.dw_employeeNumber;
    """
    cur.execute(update_sql)
    print(f"Updated {cur.rowcount} existing records in DW.\n")

    update_office_ref = """
        UPDATE j25madhumita_devdw.employees
        SET dw_office_id = o.dw_office_id
        FROM j25madhumita_devdw.offices o
        WHERE j25madhumita_devdw.employees.officeCode = o.officeCode;
    """
    cur.execute(update_office_ref)
    print(f"Updated {cur.rowcount} office references in DW.\n")

    update_reporting_ref = """
        UPDATE j25madhumita_devdw.employees
        SET dw_reporting_employee_id = r.dw_employee_id
        FROM (SELECT employeeNumber, dw_employee_id FROM j25madhumita_devdw.employees ) r
        WHERE j25madhumita_devdw.employees.reportsTo = r.employeeNumber;
    """
    cur.execute(update_reporting_ref)
    print(f"Updated {cur.rowcount} reporting relationships in DW.\n")

    conn.commit()

except Exception as e:
    conn.rollback()
    print(f"Error during DW load: {e}")

finally:
    cur.close()
    conn.close()
    print("Connection closed.")

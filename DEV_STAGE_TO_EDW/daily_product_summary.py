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

    insert_query = f"""
    INSERT INTO j25madhumita_devdw.daily_product_summary (
        summary_date,
        dw_product_id,
        customer_apd,
        product_cost_amount,
        product_mrp_amount,
        cancelled_product_qty,
        cancelled_cost_amount,
        cancelled_mrp_amount,
        cancelled_order_apd,
        dw_create_timestamp,
        dw_update_timestamp,
        etl_batch_no,
        etl_batch_date
    )
    WITH CTE AS (
        -- Normal Orders
        SELECT
            o.orderDate AS summary_date,
            od.dw_product_id,
            1 AS customer_apd,
            SUM(od.quantityOrdered * od.priceEach) AS product_cost_amount,
            SUM(od.quantityOrdered * p.MSRP) AS product_mrp_amount,
            0 AS cancelled_product_qty,
            0 AS cancelled_cost_amount,
            0 AS cancelled_mrp_amount,
            0 AS cancelled_order_apd
        FROM j25madhumita_devdw.orderdetails od
        JOIN j25madhumita_devdw.orders o ON od.src_orderNumber = o.src_orderNumber
        JOIN j25madhumita_devdw.products p ON od.src_productCode = p.src_productCode
        WHERE CAST(o.orderDate AS DATE)>= '{ETL_BATCH_DATE}'
        GROUP BY o.orderDate, od.dw_product_id

        UNION ALL

        -- Cancelled Orders
        SELECT
            o.cancelledDate AS summary_date,
            od.dw_product_id,
            1 AS customer_apd,
            0 AS product_cost_amount,
            0 AS product_mrp_amount,
            SUM(od.quantityOrdered) AS cancelled_product_qty,
            SUM(od.quantityOrdered * od.priceEach) AS cancelled_cost_amount,
            SUM(od.quantityOrdered * p.MSRP) AS cancelled_mrp_amount,
            1 AS cancelled_order_apd
        FROM j25madhumita_devdw.orderdetails od
        JOIN j25madhumita_devdw.orders o ON od.src_orderNumber = o.src_orderNumber
        JOIN j25madhumita_devdw.products p ON od.src_productCode = p.src_productCode
        WHERE CAST(o.cancelledDate AS DATE) >= '{ETL_BATCH_DATE}'
          AND UPPER(TRIM(o.status)) = 'CANCELLED'
        GROUP BY o.cancelledDate, od.dw_product_id
    )
    SELECT
        summary_date,
        dw_product_id,
        MAX(customer_apd) AS customer_apd,
        SUM(product_cost_amount) AS product_cost_amount,
        SUM(product_mrp_amount) AS product_mrp_amount,
        SUM(cancelled_product_qty) AS cancelled_product_qty,
        SUM(cancelled_cost_amount) AS cancelled_cost_amount,
        SUM(cancelled_mrp_amount) AS cancelled_mrp_amount,
        MAX(cancelled_order_apd) AS cancelled_order_apd,
        GETDATE() AS dw_create_timestamp,
        GETDATE() AS dw_update_timestamp,
        {ETL_BATCH_NO} AS etl_batch_no,
        '{ETL_BATCH_DATE}' AS etl_batch_date
    FROM CTE
    GROUP BY summary_date, dw_product_id;
    """

    cur.execute(insert_query)
    print(f"Inserted {cur.rowcount} rows into daily_product_summary.\n")
    conn.commit()

except Exception as e:
    conn.rollback()
    print(f"Error during DW load: {e}")

finally:
    cur.close()
    conn.close()
    print("Connection closed.")

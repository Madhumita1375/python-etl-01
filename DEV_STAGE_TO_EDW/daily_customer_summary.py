import os
import psycopg2
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Import batch context helper from main.py if values are missing
ETL_BATCH_NO = os.getenv("ETL_BATCH_NO")
ETL_BATCH_DATE = os.getenv("ETL_BATCH_DATE")

if not ETL_BATCH_NO or not ETL_BATCH_DATE or ETL_BATCH_NO == "None" or ETL_BATCH_DATE == "None":
    from main import get_etl_batch_details
    ETL_BATCH_NO, ETL_BATCH_DATE = get_etl_batch_details()

print(f"\nRunning daily_customer_summary ETL | Batch No: {ETL_BATCH_NO} | Date: {ETL_BATCH_DATE}\n")

try:
    # Connect to Redshift
    conn = psycopg2.connect(
        host=os.getenv("REDSHIFT_HOST"),
        port=os.getenv("REDSHIFT_PORT"),
        dbname=os.getenv("REDSHIFT_DB"),
        user=os.getenv("REDSHIFT_USER"),
        password=os.getenv("REDSHIFT_PASSWORD")
    )
    cur = conn.cursor()
    print("Connected to Redshift successfully!\n")

    query = f"""
    INSERT INTO j25madhumita_devdw.daily_customer_summary (
        summary_date,
        dw_customer_id,
        order_count,
        order_apd,
        order_cost_amount,
        cancelled_order_count,
        cancelled_order_amount,
        cancelled_order_apd,
        shipped_order_count,
        shipped_order_amount,
        shipped_order_apd,
        payment_apd,
        payment_amount,
        products_ordered_qty,
        products_items_qty,
        order_mrp_amount,
        new_customer_apd,
        new_customer_paid_apd,
        dw_create_timestamp,
        dw_update_timestamp,
        etl_batch_no,
        etl_batch_date
    )
    WITH first_payment_cte AS (
        SELECT dw_customer_id,
               MIN(paymentDate) AS first_payment_date
        FROM j25madhumita_devdw.PAYMENTS
        GROUP BY dw_customer_id
    )
    SELECT summary_date,
           dw_customer_id,
           MAX(order_count) AS order_count,
           MAX(order_apd) AS order_apd,
           MAX(order_cost_amount) AS order_cost_amount,
           MAX(cancelled_order_count) AS cancelled_order_count,
           SUM(cancelled_order_amount) AS cancelled_order_amount,
           MAX(cancelled_order_apd) AS cancelled_order_apd,
           MAX(shipped_order_count) AS shipped_order_count,
           SUM(shipped_order_amount) AS shipped_order_amount,
           MAX(shipped_order_apd) AS shipped_order_apd,
           MAX(payment_apd) AS payment_apd,
           SUM(payment_amount) AS payment_amount,
           MAX(products_ordered_qty) AS products_ordered_qty,
           MAX(products_items_qty) AS products_items_qty,
           SUM(order_mrp_amount) AS order_mrp_amount,
           CASE
             WHEN summary_date = DATE(MAX(final.dw_create_timestamp)) THEN 1
             ELSE 0
           END AS new_customer_apd,
           MAX(new_customer_paid_apd) AS new_customer_paid_apd,
           GETDATE() AS dw_create_timestamp,
           GETDATE() AS dw_update_timestamp,
           MAX(etl_batch_no) AS etl_batch_no,
           MAX(etl_batch_date) AS etl_batch_date
    FROM (

        -- CUSTOMER CREATION
        SELECT DATE(c.src_create_timestamp) AS summary_date,
               c.dw_customer_id,
               0 AS order_count,
               0 AS order_apd,
               0 AS order_cost_amount,
               0 AS cancelled_order_count,
               0 AS cancelled_order_amount,
               0 AS cancelled_order_apd,
               0 AS shipped_order_count,
               0 AS shipped_order_amount,
               0 AS shipped_order_apd,
               0 AS payment_apd,
               0 AS payment_amount,
               0 AS products_ordered_qty,
               0 AS products_items_qty,
               0 AS order_mrp_amount,
               0 AS new_customer_apd,
               0 AS new_customer_paid_apd,
               c.src_create_timestamp AS dw_create_timestamp,
               c.src_update_timestamp AS dw_update_timestamp,
               c.etl_batch_no,
               c.etl_batch_date
        FROM j25madhumita_devdw.CUSTOMERS c
        WHERE CAST(c.src_create_timestamp AS DATE) >= '{ETL_BATCH_DATE}'
        GROUP BY c.dw_customer_id, c.src_create_timestamp, c.src_update_timestamp, c.etl_batch_no, c.etl_batch_date

        UNION ALL

        -- PRODUCT ORDER SUMMARY
        SELECT o.orderDate AS summary_date,
               o.dw_customer_id,
               0 AS order_count,
               0 AS order_apd,
               0 AS order_cost_amount,
               0 AS cancelled_order_count,
               0 AS cancelled_order_amount,
               0 AS cancelled_order_apd,
               0 AS shipped_order_count,
               0 AS shipped_order_amount,
               0 AS shipped_order_apd,
               0 AS payment_apd,
               0 AS payment_amount,
               COUNT(DISTINCT od.dw_product_id) AS products_ordered_qty,
               SUM(od.quantityOrdered) AS products_items_qty,
               SUM(od.quantityOrdered * p.MSRP) AS order_mrp_amount,
               0 AS new_customer_apd,
               0 AS new_customer_paid_apd,
               o.src_create_timestamp AS dw_create_timestamp,
               o.src_update_timestamp AS dw_update_timestamp,
               od.etl_batch_no,
               od.etl_batch_date
        FROM j25madhumita_devdw.ORDERDETAILS od
        JOIN j25madhumita_devdw.ORDERS o ON o.dw_order_id = od.dw_order_id
        JOIN j25madhumita_devdw.PRODUCTS p ON od.dw_product_id = p.dw_product_id
        WHERE CAST(o.orderDate AS DATE) >= '{ETL_BATCH_DATE}'
        GROUP BY o.orderDate, o.dw_customer_id, o.src_create_timestamp, o.src_update_timestamp, od.etl_batch_no, od.etl_batch_date

        UNION ALL

        -- CANCELLED ORDERS
        SELECT o.cancelledDate AS summary_date,
               o.dw_customer_id,
               0 AS order_count,
               0 AS order_apd,
               0 AS order_cost_amount,
               COUNT(DISTINCT od.dw_order_id) AS cancelled_order_count,
               SUM(od.priceEach * od.quantityOrdered) AS cancelled_order_amount,
               1 AS cancelled_order_apd,
               0 AS shipped_order_count,
               0 AS shipped_order_amount,
               0 AS shipped_order_apd,
               0 AS payment_apd,
               0 AS payment_amount,
               0 AS products_ordered_qty,
               0 AS products_items_qty,
               0 AS order_mrp_amount,
               0 AS new_customer_apd,
               0 AS new_customer_paid_apd,
               o.src_create_timestamp AS dw_create_timestamp,
               o.src_update_timestamp AS dw_update_timestamp,
               od.etl_batch_no,
               od.etl_batch_date
        FROM j25madhumita_devdw.ORDERDETAILS od
        JOIN j25madhumita_devdw.ORDERS o ON o.dw_order_id = od.dw_order_id
        WHERE CAST(o.cancelledDate AS DATE) >= '{ETL_BATCH_DATE}' AND UPPER(TRIM(o.status)) = 'CANCELLED'
        GROUP BY o.cancelledDate, o.dw_customer_id, o.src_create_timestamp, o.src_update_timestamp, od.etl_batch_no, od.etl_batch_date

        UNION ALL

        -- SHIPPED ORDERS
        SELECT o.shippedDate AS summary_date,
               o.dw_customer_id,
               0 AS order_count,
               0 AS order_apd,
               0 AS order_cost_amount,
               0 AS cancelled_order_count,
               0 AS cancelled_order_amount,
               0 AS cancelled_order_apd,
               COUNT(DISTINCT od.dw_order_id) AS shipped_order_count,
               SUM(od.priceEach * od.quantityOrdered) AS shipped_order_amount,
               1 AS shipped_order_apd,
               0 AS payment_apd,
               0 AS payment_amount,
               0 AS products_ordered_qty,
               0 AS products_items_qty,
               0 AS order_mrp_amount,
               0 AS new_customer_apd,
               0 AS new_customer_paid_apd,
               o.src_create_timestamp AS dw_create_timestamp,
               o.src_update_timestamp AS dw_update_timestamp,
               od.etl_batch_no,
               od.etl_batch_date
        FROM j25madhumita_devdw.ORDERDETAILS od
        JOIN j25madhumita_devdw.ORDERS o ON o.dw_order_id = od.dw_order_id
        WHERE CAST(o.shippedDate AS DATE) >= '{ETL_BATCH_DATE}' AND UPPER(TRIM(o.status)) = 'SHIPPED'
        GROUP BY o.shippedDate, o.dw_customer_id, o.src_create_timestamp, o.src_update_timestamp, od.etl_batch_no, od.etl_batch_date

        UNION ALL

        -- PAYMENTS
        SELECT p.paymentDate AS summary_date,
               p.dw_customer_id,
               0 AS order_count,
               0 AS order_apd,
               0 AS order_cost_amount,
               0 AS cancelled_order_count,
               0 AS cancelled_order_amount,
               0 AS cancelled_order_apd,
               0 AS shipped_order_count,
               0 AS shipped_order_amount,
               0 AS shipped_order_apd,
               1 AS payment_apd,
               SUM(p.amount) AS payment_amount,
               0 AS products_ordered_qty,
               0 AS products_items_qty,
               0 AS order_mrp_amount,
               0 AS new_customer_apd,
               CASE WHEN fpay.first_payment_date = p.paymentDate THEN 1 ELSE 0 END AS new_customer_paid_apd,
               p.src_create_timestamp AS dw_create_timestamp,
               p.src_update_timestamp AS dw_update_timestamp,
               p.etl_batch_no,
               p.etl_batch_date
        FROM j25madhumita_devdw.PAYMENTS p
        JOIN first_payment_cte fpay ON p.dw_customer_id = fpay.dw_customer_id
        WHERE CAST(p.paymentDate AS DATE) >= '{ETL_BATCH_DATE}'
        GROUP BY p.paymentDate, p.dw_customer_id, fpay.first_payment_date, p.src_create_timestamp, p.src_update_timestamp, p.etl_batch_no, p.etl_batch_date

        UNION ALL

        -- ACTIVE (NON-CANCELLED/SHIPPED) ORDERS
        SELECT o.orderDate AS summary_date,
               o.dw_customer_id,
               COUNT(DISTINCT od.dw_order_id) AS order_count,
               1 AS order_apd,
               SUM(od.priceEach * od.quantityOrdered) AS order_cost_amount,
               0 AS cancelled_order_count,
               0 AS cancelled_order_amount,
               0 AS cancelled_order_apd,
               0 AS shipped_order_count,
               0 AS shipped_order_amount,
               0 AS shipped_order_apd,
               0 AS payment_apd,
               0 AS payment_amount,
               0 AS products_ordered_qty,
               0 AS products_items_qty,
               0 AS order_mrp_amount,
               0 AS new_customer_apd,
               0 AS new_customer_paid_apd,
               o.src_create_timestamp AS dw_create_timestamp,
               o.src_update_timestamp AS dw_update_timestamp,
               od.etl_batch_no,
               od.etl_batch_date
        FROM j25madhumita_devdw.ORDERDETAILS od
        JOIN j25madhumita_devdw.ORDERS o ON o.dw_order_id = od.dw_order_id
        WHERE CAST(o.orderDate AS DATE) >= '{ETL_BATCH_DATE}' AND UPPER(TRIM(o.status)) NOT IN ('CANCELLED', 'SHIPPED')
        GROUP BY o.orderDate, o.dw_customer_id, o.src_create_timestamp, o.src_update_timestamp, od.etl_batch_no, od.etl_batch_date
    ) final
    GROUP BY summary_date, dw_customer_id;
    """

    cur.execute(query)
    print(f" Inserted {cur.rowcount} rows into daily_customer_summary.\n")
    conn.commit()

except Exception as e:
    conn.rollback()
    print(f"Error during DW summary load: {e}")

finally:
    cur.close()
    conn.close()
    print("Connection closed.\n")

import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

ETL_BATCH_NO = os.getenv("ETL_BATCH_NO")
ETL_BATCH_DATE = os.getenv("ETL_BATCH_DATE")
REDSHIFT_SCHEMA_DW=os.getenv("REDSHIFT_SCHEMA_DW")



#print(f"\nRunning monthly_customer_summary ETL | Batch No: {ETL_BATCH_NO} | Date: {ETL_BATCH_DATE}\n")

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

    update_sql = f"""
    UPDATE {REDSHIFT_SCHEMA_DW}.monthly_customer_summary AS mc
    SET 
        order_count = mc.order_count + dc.order_count,
        order_apd = CASE WHEN mc.order_apd = 1 THEN 1 ELSE dc.order_apd END,
        order_apm = CASE WHEN mc.order_apm = 1 THEN 1 ELSE dc.order_apd END,

        order_cost_amount = mc.order_cost_amount + dc.order_cost_amount,

        cancelled_order_count = mc.cancelled_order_count + dc.cancelled_order_count,
        cancelled_order_amount = mc.cancelled_order_amount + dc.cancelled_order_amount,
        cancelled_order_apd = CASE WHEN mc.cancelled_order_apd = 1 THEN 1 ELSE dc.cancelled_order_apd END,
        cancelled_order_apm = CASE WHEN mc.cancelled_order_apm = 1 THEN 1 ELSE dc.cancelled_order_apd END,

        shipped_order_count = mc.shipped_order_count + dc.shipped_order_count,
        shipped_order_amount = mc.shipped_order_amount + dc.shipped_order_amount,
        shipped_order_apd = CASE WHEN mc.shipped_order_apd = 1 THEN 1 ELSE dc.shipped_order_apd END,
        shipped_order_apm = CASE WHEN mc.shipped_order_apm = 1 THEN 1 ELSE dc.shipped_order_apd END,

        payment_apd = CASE WHEN mc.payment_apd = 1 THEN 1 ELSE dc.payment_apd END,
        payment_apm = CASE WHEN mc.payment_apm = 1 THEN 1 ELSE dc.payment_apd END,
        payment_amount = mc.payment_amount + dc.payment_amount,

        products_ordered_qty = mc.products_ordered_qty + dc.products_ordered_qty,
        products_items_qty = mc.products_items_qty + dc.products_items_qty,
        order_mrp_amount = mc.order_mrp_amount + dc.order_mrp_amount,

        new_customer_apd = CASE WHEN mc.new_customer_apd = 1 THEN 1 ELSE dc.new_customer_apd END,
        new_customer_apm = CASE WHEN mc.new_customer_apm = 1 THEN 1 ELSE dc.new_customer_apd END,

        new_customer_paid_apd = CASE WHEN mc.new_customer_paid_apd = 1 THEN 1 ELSE dc.new_customer_paid_apd END,
        new_customer_paid_apm = CASE WHEN mc.new_customer_paid_apm = 1 THEN 1 ELSE dc.new_customer_paid_apd END,

        dw_update_timestamp = GETDATE(),
        etl_batch_no = dc.etl_batch_no,
        etl_batch_date = dc.etl_batch_date
    FROM {REDSHIFT_SCHEMA_DW}.daily_customer_summary AS dc
    WHERE 
        mc.start_of_the_month_date = DATE_TRUNC('month', dc.summary_date)
        AND mc.dw_customer_id = dc.dw_customer_id
        AND CAST(dc.summary_date AS DATE) >= '{ETL_BATCH_DATE}';
    """
    cur.execute(update_sql)
    #print(f"Updated {cur.rowcount} monthly_customer_summary records.\n")

    insert_sql = f"""
    INSERT INTO {REDSHIFT_SCHEMA_DW}.monthly_customer_summary (
        start_of_the_month_date,
        dw_customer_id,
        order_count,
        order_apd,
        order_apm,
        order_cost_amount,
        cancelled_order_count,
        cancelled_order_amount,
        cancelled_order_apd,
        cancelled_order_apm,
        shipped_order_count,
        shipped_order_amount,
        shipped_order_apd,
        shipped_order_apm,
        payment_apd,
        payment_apm,
        payment_amount,
        products_ordered_qty,
        products_items_qty,
        order_mrp_amount,
        new_customer_apd,
        new_customer_apm,
        new_customer_paid_apd,
        new_customer_paid_apm,
        dw_create_timestamp,
        dw_update_timestamp,
        etl_batch_no,
        etl_batch_date
    )
    SELECT 
        DATE_TRUNC('month', dc.summary_date) AS start_of_the_month_date,
        dc.dw_customer_id,
        SUM(dc.order_count) AS order_count,
        MAX(dc.order_apd) AS order_apd,
        MAX(dc.order_apd) AS order_apm,
        SUM(dc.order_cost_amount) AS order_cost_amount,
        SUM(dc.cancelled_order_count) AS cancelled_order_count,
        SUM(dc.cancelled_order_amount) AS cancelled_order_amount,
        MAX(dc.cancelled_order_apd) AS cancelled_order_apd,
        MAX(dc.cancelled_order_apd) AS cancelled_order_apm,
        SUM(dc.shipped_order_count) AS shipped_order_count,
        SUM(dc.shipped_order_amount) AS shipped_order_amount,
        MAX(dc.shipped_order_apd) AS shipped_order_apd,
        MAX(dc.shipped_order_apd) AS shipped_order_apm,
        MAX(dc.payment_apd) AS payment_apd,
        MAX(dc.payment_apd) AS payment_apm,
        SUM(dc.payment_amount) AS payment_amount,
        SUM(dc.products_ordered_qty) AS products_ordered_qty,
        SUM(dc.products_items_qty) AS products_items_qty,
        SUM(dc.order_mrp_amount) AS order_mrp_amount,
        MAX(dc.new_customer_apd) AS new_customer_apd,
        MAX(dc.new_customer_apd) AS new_customer_apm,
        MAX(dc.new_customer_paid_apd) AS new_customer_paid_apd,
        MAX(dc.new_customer_paid_apd) AS new_customer_paid_apm,
        GETDATE() AS dw_create_timestamp,
        GETDATE() AS dw_update_timestamp,
        MAX(dc.etl_batch_no) AS etl_batch_no,
        MAX(dc.etl_batch_date) AS etl_batch_date
    FROM {REDSHIFT_SCHEMA_DW}.daily_customer_summary AS dc
    LEFT JOIN {REDSHIFT_SCHEMA_DW}.monthly_customer_summary AS dcs 
        ON DATE_TRUNC('month', dc.summary_date) = dcs.start_of_the_month_date
        AND dc.dw_customer_id = dcs.dw_customer_id
    WHERE dcs.start_of_the_month_date IS NULL
    GROUP BY DATE_TRUNC('month', dc.summary_date), dc.dw_customer_id;
    """
    cur.execute(insert_sql)
    #print(f"Inserted {cur.rowcount} new monthly_customer_summary records.\n")

    conn.commit()

except Exception as e:
    conn.rollback()
    print(f"Error during monthly_customer_summary ETL: {e}")

finally:
    cur.close()
    conn.close()
    #print("Connection closed.\n")

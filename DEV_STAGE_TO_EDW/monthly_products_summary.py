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

    update_sql = f"""
    UPDATE {REDSHIFT_SCHEMA_DW}.monthly_product_summary AS mp
    SET 
        customer_apd = CASE WHEN mp.customer_apd = 1 THEN 1 ELSE dp.customer_apd END,
        customer_apm = CASE WHEN mp.customer_apm = 1 THEN 1 ELSE dp.customer_apd END,
        product_cost_amount = mp.product_cost_amount + dp.product_cost_amount,
        product_mrp_amount = mp.product_mrp_amount + dp.product_mrp_amount,
        cancelled_product_qty = mp.cancelled_product_qty + dp.cancelled_product_qty,
        cancelled_cost_amount = mp.cancelled_cost_amount + dp.cancelled_cost_amount,
        cancelled_mrp_amount = mp.cancelled_mrp_amount + dp.cancelled_mrp_amount,
        cancelled_order_apd = CASE WHEN mp.cancelled_order_apd = 1 THEN 1 ELSE dp.cancelled_order_apd END,
        cancelled_order_apm = CASE WHEN mp.cancelled_order_apm = 1 THEN 1 ELSE dp.cancelled_order_apd END,
        dw_update_timestamp = GETDATE(),
        etl_batch_no = dp.etl_batch_no,
        etl_batch_date = dp.etl_batch_date
    FROM {REDSHIFT_SCHEMA_DW}.daily_product_summary AS dp
    WHERE mp.start_of_the_month_date = DATE_TRUNC('month', dp.summary_date)
      AND mp.dw_product_id = dp.dw_product_id
      AND CAST(dp.summary_date AS DATE) >= '{ETL_BATCH_DATE}';
    """
    cur.execute(update_sql)
    #print(f"Updated {cur.rowcount} existing monthly product records.\n")

    insert_sql = f"""
    INSERT INTO {REDSHIFT_SCHEMA_DW}.monthly_product_summary
    SELECT 
        DATE_TRUNC('month', dp.summary_date) AS start_of_the_month_date,
        dp.dw_product_id,
        MAX(dp.customer_apd) AS customer_apd,
        MAX(dp.customer_apd) AS customer_apm,
        SUM(dp.product_cost_amount) AS product_cost_amount,
        SUM(dp.product_mrp_amount) AS product_mrp_amount,
        SUM(dp.cancelled_product_qty) AS cancelled_product_qty,
        SUM(dp.cancelled_cost_amount) AS cancelled_cost_amount,
        SUM(dp.cancelled_mrp_amount) AS cancelled_mrp_amount,
        MAX(dp.cancelled_order_apd) AS cancelled_order_apd,
        MAX(dp.cancelled_order_apd) AS cancelled_order_apm,
        GETDATE() AS dw_create_timestamp,
        GETDATE() AS dw_update_timestamp,
        MAX(dp.etl_batch_no) AS etl_batch_no,
        MAX(dp.etl_batch_date) AS etl_batch_date
    FROM {REDSHIFT_SCHEMA_DW}.daily_product_summary dp
    LEFT JOIN {REDSHIFT_SCHEMA_DW}.monthly_product_summary mp
      ON DATE_TRUNC('month', dp.summary_date) = mp.start_of_the_month_date
      AND dp.dw_product_id = mp.dw_product_id
    WHERE mp.start_of_the_month_date IS NULL
    GROUP BY DATE_TRUNC('month', dp.summary_date), dp.dw_product_id;
    """
    cur.execute(insert_sql)
    #print(f"Inserted {cur.rowcount} new monthly product records.\n")

    conn.commit()

except Exception as e:
    conn.rollback()
    print(f"Error during monthly product summary load: {e}")

finally:
    cur.close()
    conn.close()

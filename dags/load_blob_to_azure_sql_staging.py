from datetime import datetime
from io import StringIO
import csv
import os
from pathlib import Path

import pymssql
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from azure.storage.blob import BlobServiceClient


# This DAG loads the raw orders.csv file from Azure Blob Storage
# into the Azure SQL staging table.
@dag(
    dag_id="load_blob_to_azure_sql_staging",
    start_date=datetime(2026, 7, 1),
    schedule=None,
    catchup=False,
    tags=["food-delivery", "azure-sql", "staging"],
)
def load_blob_to_azure_sql_staging():
    @task
    def validate_environment():
        required_env_vars = [
            "AZURE_STORAGE_CONNECTION_STRING",
            "AZURE_STORAGE_CONTAINER_NAME",
            "AZURE_SQL_SERVER",
            "AZURE_SQL_DATABASE",
            "AZURE_SQL_USERNAME",
            "AZURE_SQL_PASSWORD",
        ]

        missing_vars = [
            env_var for env_var in required_env_vars if not os.getenv(env_var)
        ]

        if missing_vars:
            raise ValueError(f"Missing environment variables: {missing_vars}")

        return {"status": "environment variables found"}

    @task
    def apply_staging_schema_fix():
        # This SQL fix changes items_in_order from INT to NVARCHAR(MAX).
        # The source column contains item descriptions, not item counts.
        sql_file = Path("/opt/airflow/sql/02_fix_staging_schema.sql")

        if not sql_file.exists():
            raise FileNotFoundError(f"SQL file not found: {sql_file}")

        sql_text = sql_file.read_text(encoding="utf-8")

        conn = pymssql.connect(
            server=os.getenv("AZURE_SQL_SERVER"),
            user=os.getenv("AZURE_SQL_USERNAME"),
            password=os.getenv("AZURE_SQL_PASSWORD"),
            database=os.getenv("AZURE_SQL_DATABASE"),
            login_timeout=30,
            timeout=60,
        )

        try:
            with conn.cursor() as cursor:
                cursor.execute(sql_text)
                conn.commit()
        finally:
            conn.close()

        return {"status": "staging schema fix applied"}

    def parse_float(value):
        if value is None or value.strip() == "":
            return None
        return float(value.replace(",", "").strip())

    def parse_distance_km(value):
        # Source examples: "3km", "2km", "<1km"
        # For "<1km", we store 0.5 as an estimated midpoint for staging.
        if value is None or value.strip() == "":
            return None

        cleaned_value = value.strip().lower().replace("km", "")

        if cleaned_value.startswith("<"):
            return 0.5

        return float(cleaned_value)

    def parse_datetime(value):
        if value is None or value.strip() == "":
            return None

        return datetime.strptime(value.strip(), "%I:%M %p, %B %d %Y")

    @task
    def download_blob_and_parse_rows():
        context = get_current_context()
        load_date = context["ds"]

        connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
        container_name = os.getenv("AZURE_STORAGE_CONTAINER_NAME")

        blob_name = f"raw/food_delivery/orders/load_date={load_date}/orders.csv"

        blob_service_client = BlobServiceClient.from_connection_string(
            connection_string
        )
        blob_client = blob_service_client.get_blob_client(
            container=container_name,
            blob=blob_name,
        )

        # Download CSV content from Azure Blob into memory.
        blob_content = blob_client.download_blob().readall().decode("utf-8-sig")

        reader = csv.DictReader(StringIO(blob_content))
        rows = []

        for source_row in reader:
            rows.append(
                {
                    "restaurant_id": source_row["Restaurant ID"],
                    "restaurant_name": source_row["Restaurant name"],
                    "subzone": source_row["Subzone"],
                    "city": source_row["City"],
                    "order_id": source_row["Order ID"],
                    "order_placed_at": parse_datetime(source_row["Order Placed At"]),
                    "order_status": source_row["Order Status"],
                    "delivery": source_row["Delivery"],
                    "distance": parse_distance_km(source_row["Distance"]),
                    "items_in_order": source_row["Items in order"],
                    "instructions": source_row["Instructions"] or None,
                    "discount_construct": source_row["Discount construct"] or None,
                    "bill_subtotal": parse_float(source_row["Bill subtotal"]),
                    "packaging_charges": parse_float(source_row["Packaging charges"]),
                    "restaurant_discount_promo": parse_float(
                        source_row["Restaurant discount (Promo)"]
                    ),
                    "restaurant_discount_flat_offs_freebies_others": parse_float(
                        source_row["Restaurant discount (Flat offs, Freebies & others)"]
                    ),
                    "gold_discount": parse_float(source_row["Gold discount"]),
                    "brand_pack_discount": parse_float(
                        source_row["Brand pack discount"]
                    ),
                    "total": parse_float(source_row["Total"]),
                    "rating": parse_float(source_row["Rating"]),
                    "review": source_row["Review"] or None,
                    "cancellation_rejection_reason": source_row[
                        "Cancellation / Rejection reason"
                    ]
                    or None,
                    "restaurant_compensation_cancellation": parse_float(
                        source_row["Restaurant compensation (Cancellation)"]
                    ),
                    "restaurant_penalty_rejection": parse_float(
                        source_row["Restaurant penalty (Rejection)"]
                    ),
                    "kpt_duration_minutes": parse_float(
                        source_row["KPT duration (minutes)"]
                    ),
                    "rider_wait_time_minutes": parse_float(
                        source_row["Rider wait time (minutes)"]
                    ),
                    "order_ready_marked": source_row["Order Ready Marked"] or None,
                    "customer_complaint_tag": source_row["Customer complaint tag"]
                    or None,
                    "customer_id": source_row["Customer ID"],
                    "source_file_name": blob_name,
                    "load_date": load_date,
                }
            )

        return {
            "rows": rows,
            "row_count": len(rows),
            "blob_name": blob_name,
            "load_date": load_date,
        }

    @task
    def load_rows_to_staging(parsed_result):
        rows = parsed_result["rows"]

        insert_sql = """
        INSERT INTO staging.stg_food_delivery_orders (
            restaurant_id,
            restaurant_name,
            subzone,
            city,
            order_id,
            order_placed_at,
            order_status,
            delivery,
            distance,
            items_in_order,
            instructions,
            discount_construct,
            bill_subtotal,
            packaging_charges,
            restaurant_discount_promo,
            restaurant_discount_flat_offs_freebies_others,
            gold_discount,
            brand_pack_discount,
            total,
            rating,
            review,
            cancellation_rejection_reason,
            restaurant_compensation_cancellation,
            restaurant_penalty_rejection,
            kpt_duration_minutes,
            rider_wait_time_minutes,
            order_ready_marked,
            customer_complaint_tag,
            customer_id,
            source_file_name,
            load_date
        )
        VALUES (
            %(restaurant_id)s,
            %(restaurant_name)s,
            %(subzone)s,
            %(city)s,
            %(order_id)s,
            %(order_placed_at)s,
            %(order_status)s,
            %(delivery)s,
            %(distance)s,
            %(items_in_order)s,
            %(instructions)s,
            %(discount_construct)s,
            %(bill_subtotal)s,
            %(packaging_charges)s,
            %(restaurant_discount_promo)s,
            %(restaurant_discount_flat_offs_freebies_others)s,
            %(gold_discount)s,
            %(brand_pack_discount)s,
            %(total)s,
            %(rating)s,
            %(review)s,
            %(cancellation_rejection_reason)s,
            %(restaurant_compensation_cancellation)s,
            %(restaurant_penalty_rejection)s,
            %(kpt_duration_minutes)s,
            %(rider_wait_time_minutes)s,
            %(order_ready_marked)s,
            %(customer_complaint_tag)s,
            %(customer_id)s,
            %(source_file_name)s,
            %(load_date)s
        );
        """

        delete_sql = """
        DELETE FROM staging.stg_food_delivery_orders
        WHERE load_date = %s;
        """

        conn = pymssql.connect(
            server=os.getenv("AZURE_SQL_SERVER"),
            user=os.getenv("AZURE_SQL_USERNAME"),
            password=os.getenv("AZURE_SQL_PASSWORD"),
            database=os.getenv("AZURE_SQL_DATABASE"),
            login_timeout=30,
            timeout=120,
        )

        try:
            with conn.cursor() as cursor:
                # Delete existing rows for the same load_date.
                # This makes the load rerunnable during development.
                cursor.execute(delete_sql, (parsed_result["load_date"],))
                cursor.executemany(insert_sql, rows)
                conn.commit()
        finally:
            conn.close()

        return {
            "row_count": len(rows),
            "load_date": parsed_result["load_date"],
            "blob_name": parsed_result["blob_name"],
            "status": "loaded into staging",
        }

    @task
    def print_result(load_result):
        print("Azure SQL staging load completed.")
        print(f"Blob name: {load_result['blob_name']}")
        print(f"Load date: {load_result['load_date']}")
        print(f"Rows loaded: {load_result['row_count']}")
        print(f"Status: {load_result['status']}")

    env_check_result = validate_environment()
    schema_fix_result = apply_staging_schema_fix()
    parsed_result = download_blob_and_parse_rows()

    # env_check_result and schema_fix_result are included here to force task order.
    load_result = load_rows_to_staging(parsed_result)
    print_result(load_result)

    env_check_result >> schema_fix_result >> parsed_result >> load_result


load_blob_to_azure_sql_staging()
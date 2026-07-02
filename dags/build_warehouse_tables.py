from datetime import datetime
from pathlib import Path
import os

import pymssql
from airflow.decorators import dag, task


# This DAG creates and loads warehouse tables from the staging layer.
# Warehouse tables are cleaner and more analytics-friendly than staging tables.
@dag(
    dag_id="build_warehouse_tables",
    start_date=datetime(2026, 7, 1),
    schedule=None,
    catchup=False,
    tags=["food-delivery", "azure-sql", "warehouse"],
)
def build_warehouse_tables():
    @task
    def validate_environment():
        required_env_vars = [
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

        return {"status": "Azure SQL environment variables found"}

    @task
    def run_sql_file(sql_file_name):
        # SQL files are mounted into the Airflow container from the local sql/ folder.
        sql_file = Path(f"/opt/airflow/sql/{sql_file_name}")

        if not sql_file.exists():
            raise FileNotFoundError(f"SQL file not found: {sql_file}")

        sql_text = sql_file.read_text(encoding="utf-8")

        # SQL Server tools understand GO, but pymssql does not.
        # We split the file into separate batches before executing.
        sql_batches = [
            batch.strip()
            for batch in sql_text.split("GO")
            if batch.strip()
        ]

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
                for batch in sql_batches:
                    cursor.execute(batch)
                conn.commit()
        finally:
            conn.close()

        return {
            "sql_file": str(sql_file),
            "batch_count": len(sql_batches),
            "status": "SQL file executed",
        }
    @task
    def print_result(create_result, load_result):
        print("Warehouse build completed.")
        print(f"Create SQL: {create_result['sql_file']}")
        print(f"Load SQL: {load_result['sql_file']}")

    env_check_result = validate_environment()
    create_result = run_sql_file("04_create_warehouse_tables.sql")
    load_result = run_sql_file("05_load_warehouse_tables.sql")
    mart_result = run_sql_file("06_create_mart_views.sql")
    print_result(create_result, load_result)

    env_check_result >> create_result >> load_result >> mart_result

build_warehouse_tables()
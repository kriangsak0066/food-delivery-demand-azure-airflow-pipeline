from datetime import datetime
from pathlib import Path
import os

import pymssql
from airflow.decorators import dag, task


# This DAG creates the Azure SQL staging schema and staging table.
# It reads SQL from the sql/ folder so SQL logic stays separate from Python orchestration code.
@dag(
    dag_id="create_azure_sql_staging_table",
    start_date=datetime(2026, 7, 1),
    schedule=None,
    catchup=False,
    tags=["food-delivery", "azure-sql", "staging"],
)
def create_azure_sql_staging_table():
    @task
    def validate_environment():
        # These values come from the .env file through docker-compose env_file.
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

        # Do not return or print password.
        return {
            "server": os.getenv("AZURE_SQL_SERVER"),
            "database": os.getenv("AZURE_SQL_DATABASE"),
            "username": os.getenv("AZURE_SQL_USERNAME"),
            "status": "Azure SQL environment variables found",
        }

    @task
    def validate_sql_file():
        # This path is inside the Airflow container.
        # It maps from local project path:
        # sql/01_create_staging_tables.sql
        sql_file = Path("/opt/airflow/sql/01_create_staging_tables.sql")

        if not sql_file.exists():
            raise FileNotFoundError(f"SQL file not found: {sql_file}")

        sql_text = sql_file.read_text(encoding="utf-8")

        if "stg_food_delivery_orders" not in sql_text:
            raise ValueError("Expected staging table name was not found in SQL file.")

        return {
            "sql_file": str(sql_file),
            "status": "SQL file found",
        }

    @task
    def run_create_staging_sql(env_check_result, sql_check_result):
        sql_file = Path(sql_check_result["sql_file"])
        sql_text = sql_file.read_text(encoding="utf-8")

        # Connect to Azure SQL Database using pymssql.
        # The server should look like:
        # your-server-name.database.windows.net
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

        return {
            "server": env_check_result["server"],
            "database": env_check_result["database"],
            "sql_file": str(sql_file),
            "status": "staging schema and table created",
        }

    @task
    def print_result(result):
        # Print only safe metadata. Never print passwords or connection strings.
        print("Azure SQL staging setup completed.")
        print(f"Server: {result['server']}")
        print(f"Database: {result['database']}")
        print(f"SQL file: {result['sql_file']}")
        print(f"Status: {result['status']}")

    env_check_result = validate_environment()
    sql_check_result = validate_sql_file()
    result = run_create_staging_sql(env_check_result, sql_check_result)
    print_result(result)


create_azure_sql_staging_table()
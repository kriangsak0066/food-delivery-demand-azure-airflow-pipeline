from datetime import datetime
from pathlib import Path
import os

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
from azure.storage.blob import BlobServiceClient


# This DAG uploads the local raw food delivery CSV file to Azure Blob Storage.
# It replaces the local blob_simulator step with a real cloud raw landing zone.
@dag(
    dag_id="upload_raw_to_azure_blob",
    start_date=datetime(2026, 7, 1),
    schedule=None,
    catchup=False,
    tags=["food-delivery", "azure-blob", "raw"],
)
def upload_raw_to_azure_blob():
    @task
    def validate_environment():
        # These values come from the .env file through docker-compose env_file.
        connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
        container_name = os.getenv("AZURE_STORAGE_CONTAINER_NAME")

        # Do not print the connection string because it is a secret.
        if not connection_string:
            raise ValueError("AZURE_STORAGE_CONNECTION_STRING is missing.")

        if not container_name:
            raise ValueError("AZURE_STORAGE_CONTAINER_NAME is missing.")

        return {
            "container_name": container_name,
            "status": "environment variables found",
        }

    @task
    def validate_source_file():
        # This path is inside the Airflow container.
        # It maps from local project path:
        # data/raw/food_delivery_orders/orders.csv
        source_file = Path("/opt/airflow/data/raw/food_delivery_orders/orders.csv")

        if not source_file.exists():
            raise FileNotFoundError(f"Source file not found: {source_file}")

        file_size_mb = source_file.stat().st_size / (1024 * 1024)

        return {
            "source_file": str(source_file),
            "file_size_mb": round(file_size_mb, 2),
        }

    @task
    def upload_file_to_blob(env_check_result, source_check_result):
        context = get_current_context()
        load_date = context["ds"]

        connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
        container_name = env_check_result["container_name"]
        source_file = Path(source_check_result["source_file"])

        # This is the blob path in Azure.
        # Azure Blob uses virtual folders, so this full path becomes the blob name.
        blob_name = f"raw/food_delivery/orders/load_date={load_date}/orders.csv"

        blob_service_client = BlobServiceClient.from_connection_string(
            connection_string
        )

        blob_client = blob_service_client.get_blob_client(
            container=container_name,
            blob=blob_name,
        )

        # overwrite=True makes the DAG rerunnable for the same load_date.
        # This is useful during development because we can test the DAG multiple times.
        with source_file.open("rb") as file:
            blob_client.upload_blob(file, overwrite=True)

        blob_properties = blob_client.get_blob_properties()

        return {
            "container_name": container_name,
            "blob_name": blob_name,
            "blob_size_mb": round(blob_properties.size / (1024 * 1024), 2),
            "load_date": load_date,
            "status": "uploaded to Azure Blob Storage",
        }

    @task
    def print_upload_result(upload_result):
        # This task prints only safe information.
        # Never print secrets such as connection strings or account keys.
        print("Azure Blob upload completed.")
        print(f"Container: {upload_result['container_name']}")
        print(f"Blob name: {upload_result['blob_name']}")
        print(f"Blob size MB: {upload_result['blob_size_mb']}")
        print(f"Load date: {upload_result['load_date']}")
        print(f"Status: {upload_result['status']}")

    env_check_result = validate_environment()
    source_check_result = validate_source_file()
    upload_result = upload_file_to_blob(env_check_result, source_check_result)
    print_upload_result(upload_result)


upload_raw_to_azure_blob()
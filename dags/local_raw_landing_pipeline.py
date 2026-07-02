from datetime import datetime
from pathlib import Path
import csv
import shutil

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context


# This DAG simulates the raw landing step before we connect to Azure Blob Storage.
# Instead of uploading to Azure, it copies the raw CSV into a local folder
# that follows the same path pattern we plan to use in Azure Blob.
@dag(
    dag_id="local_raw_landing_pipeline",
    start_date=datetime(2026, 7, 1),
    schedule=None,
    catchup=False,
    tags=["food-delivery", "raw", "learning"],
)
def local_raw_landing_pipeline():
    @task
    def validate_source_file():
        # This is the original raw dataset mounted into the Airflow container.
        # Local project path:
        # data/raw/food_delivery_orders/orders.csv
        #
        # Container path:
        # /opt/airflow/data/raw/food_delivery_orders/orders.csv
        source_file = Path("/opt/airflow/data/raw/food_delivery_orders/orders.csv")

        # Fail early if the source file is missing.
        # In real pipelines, it is better to stop here than continue with bad input.
        if not source_file.exists():
            raise FileNotFoundError(f"Source file not found: {source_file}")

        # Read only the header and a few sample rows.
        # We are not transforming the data here; this step only validates that
        # the file can be opened and has readable CSV content.
        with source_file.open("r", encoding="utf-8-sig", newline="") as file:
            reader = csv.reader(file)
            header = next(reader)
            sample_rows = [next(reader, None) for _ in range(5)]

        # Remove empty sample rows if the file has fewer than 5 data rows.
        sample_rows = [row for row in sample_rows if row is not None]

        # Returning a dictionary allows the next Airflow task to use this result.
        # Airflow stores this task output in XCom behind the scenes.
        return {
            "source_file": str(source_file),
            "column_count": len(header),
            "sample_row_count": len(sample_rows),
        }

    @task
    def copy_to_local_raw_landing(source_check_result):
        # Get the logical run date from Airflow.
        # context["ds"] is formatted as YYYY-MM-DD.
        context = get_current_context()
        load_date = context["ds"]

        source_file = Path(source_check_result["source_file"])

        # This folder simulates the Azure Blob raw zone.
        # Later, the same structure will become:
        # raw/food_delivery/orders/load_date=YYYY-MM-DD/orders.csv
        target_dir = Path(
            f"/opt/airflow/data/processed/blob_simulator/raw/food_delivery/orders/load_date={load_date}"
        )

        # Create the partition folder if it does not already exist.
        target_dir.mkdir(parents=True, exist_ok=True)

        # Copy the original file into the raw landing path.
        # We keep the filename stable as orders.csv for predictable downstream loading.
        target_file = target_dir / "orders.csv"
        shutil.copy2(source_file, target_file)

        return {
            "load_date": load_date,
            "target_file": str(target_file),
            "status": "copied to local raw landing",
        }

    @task
    def print_result(copy_result):
        # This task is mainly for learning/debugging.
        # It prints the final landing result into the Airflow task log.
        print("Local raw landing completed.")
        print(f"Load date: {copy_result['load_date']}")
        print(f"Target file: {copy_result['target_file']}")
        print(f"Status: {copy_result['status']}")

    # Define task dependencies.
    # Airflow will run them in this order:
    # 1. validate_source_file
    # 2. copy_to_local_raw_landing
    # 3. print_result
    source_check_result = validate_source_file()
    copy_result = copy_to_local_raw_landing(source_check_result)
    print_result(copy_result)


# Register the DAG so Airflow can discover it.
local_raw_landing_pipeline()
from datetime import datetime
from pathlib import Path

from airflow.decorators import dag, task


@dag(
    dag_id="hello_food_delivery_pipeline",
    start_date=datetime(2026, 7, 1),
    schedule=None,
    catchup=False,
    tags=["food-delivery", "learning"],
)
def hello_food_delivery_pipeline():
    @task
    def check_raw_file():
        raw_file_path = Path("/opt/airflow/data/raw/food_delivery_orders/orders.csv")

        if not raw_file_path.exists():
            raise FileNotFoundError(f"Raw file not found: {raw_file_path}")

        file_size_mb = raw_file_path.stat().st_size / (1024 * 1024)

        return {
            "file_path": str(raw_file_path),
            "file_size_mb": round(file_size_mb, 2),
            "status": "raw file found",
        }

    @task
    def print_pipeline_message(file_check_result):
        print("Food delivery pipeline is ready.")
        print(f"Raw file path: {file_check_result['file_path']}")
        print(f"File size MB: {file_check_result['file_size_mb']}")
        print(f"Status: {file_check_result['status']}")

    file_check_result = check_raw_file()
    print_pipeline_message(file_check_result)


hello_food_delivery_pipeline()
from airflow.sdk import asset
from pendulum import now
import os

FILE_PATH = "/opt/airflow/logs/data/data_extract.txt"


@asset(
    schedule="@daily",
    uri=FILE_PATH,

    name="fetch_data",
)
def fetch_data():
    # Ensure the directory exists
    os.makedirs(os.path.dirname(FILE_PATH), exist_ok=True)

    # Simulate fetching data by writing to a file
    with open(FILE_PATH, "w") as f:
        f.write(f"Data fetched on {now('America/Halifax')}\n")

    print(f"Data written to {FILE_PATH}")
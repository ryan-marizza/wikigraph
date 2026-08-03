from datetime import datetime
from airflow.sdk import dag, task


@dag(
    dag_id="hello_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["example"],
)
def hello_pipeline():
    @task
    def extract() -> dict:
        return {"rows": 42}

    @task
    def transform(data: dict) -> int:
        return data["rows"] * 2

    @task
    def load(value: int) -> None:
        print(f"Loaded {value} rows")

    load(transform(extract()))


hello_pipeline()
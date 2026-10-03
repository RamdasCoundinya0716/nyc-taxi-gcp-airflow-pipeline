import os
from datetime import datetime, timedelta

import requests
from airflow.sdk import Variable, dag, task
from airflow.providers.google.cloud.hooks.gcs import GCSHook

BUCKET = Variable.get("GCS_BUCKET", default="your-bucket")
MONTHS = Variable.get("TAXI_MONTHS", default="2024-01,2024-02,2024-03").split(",")
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
RAW_PREFIX = "raw/yellow_taxi"


@dag(
    dag_id="p1_land_nyc_taxi_to_gcs",
    start_date=datetime(2026, 10, 1),
    schedule=None,
    catchup=False,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2)},
    tags=["nyc-taxi", "part1"],
)
def land_nyc_taxi():
    @task
    def download_to_gcs(month: str) -> str:
        file_name = f"yellow_tripdata_{month}.parquet"
        object_name = f"{RAW_PREFIX}/{month}/{file_name}"
        hook = GCSHook()

        if hook.exists(bucket_name=BUCKET, object_name=object_name):
            print(f"gs://{BUCKET}/{object_name} already exists, skipping")
            return object_name

        local_path = f"/tmp/{file_name}"
        with requests.get(f"{BASE_URL}/{file_name}", stream=True, timeout=120) as r:
            r.raise_for_status()
            with open(local_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=8 * 1024 * 1024):
                    f.write(chunk)

        hook.upload(bucket_name=BUCKET, object_name=object_name, filename=local_path)
        os.remove(local_path)
        print(f"uploaded gs://{BUCKET}/{object_name}")
        return object_name

    @task
    def verify(objects: list[str]) -> None:
        hook = GCSHook()
        for obj in objects:
            size = hook.get_size(bucket_name=BUCKET, object_name=obj)
            if not size:
                raise ValueError(f"{obj} is missing or empty in gs://{BUCKET}")
            print(f"OK {obj}: {size / 1e6:.1f} MB")

    verify(download_to_gcs.expand(month=MONTHS))


land_nyc_taxi()
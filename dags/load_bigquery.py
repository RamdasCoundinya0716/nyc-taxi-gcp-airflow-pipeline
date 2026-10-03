"""Part 3: load cleaned Parquet from GCS into BigQuery.

Creates two tables with identical data:
  nyc_taxi.yellow_trips       partitioned by pickup_date, clustered by pu_location_id
  nyc_taxi.yellow_trips_flat  plain copy (no partitioning / clustering) for the Part 4 benchmark
"""
from datetime import datetime, timedelta

from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryInsertJobOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.sdk import DAG, Variable

PROJECT = Variable.get("GCP_PROJECT_ID", default="nyc-taxi-pipeline-510514")
REGION = Variable.get("GCP_REGION", default="asia-south1")
BUCKET = Variable.get("GCS_BUCKET", default="your-bucket")
DATASET = "nyc_taxi"

with DAG(
    dag_id="p3_load_bigquery",
    start_date=datetime(2026, 10, 1),
    schedule=None,
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=2)},
    tags=["nyc-taxi", "part3"],
) as dag:
    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_dataset",
        project_id=PROJECT,
        dataset_id=DATASET,
        location=REGION,  # must match the bucket region
        exists_ok=True,
    )

    load_partitioned = GCSToBigQueryOperator(
        task_id="load_partitioned_clustered",
        bucket=BUCKET,
        source_objects=["clean/yellow_taxi/*.parquet"],
        destination_project_dataset_table=f"{PROJECT}.{DATASET}.yellow_trips",
        source_format="PARQUET",
        write_disposition="WRITE_TRUNCATE",
        time_partitioning={"type": "DAY", "field": "pickup_date"},
        cluster_fields=["pu_location_id"],
        location=REGION,
    )

    create_flat_copy = BigQueryInsertJobOperator(
        task_id="create_unpartitioned_copy",
        location=REGION,
        configuration={
            "query": {
                "query": f"CREATE OR REPLACE TABLE `{PROJECT}.{DATASET}.yellow_trips_flat` "
                         f"AS SELECT * FROM `{PROJECT}.{DATASET}.yellow_trips`",
                "useLegacySql": False,
            }
        },
    )

    create_dataset >> load_partitioned >> create_flat_copy
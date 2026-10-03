"""Part 2: clean the raw taxi files with PySpark on an ephemeral Dataproc cluster."""
from datetime import datetime, timedelta

from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator,
)
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.sdk import DAG, Variable

PROJECT = Variable.get("GCP_PROJECT_ID", default="your-gcp-project-id")
REGION = Variable.get("GCP_REGION", default="asia-south1")
BUCKET = Variable.get("GCS_BUCKET", default="your-bucket")
MONTHS = Variable.get("TAXI_MONTHS", default="2024-01,2024-02,2024-03")
CLUSTER = "nyc-taxi-cluster"

CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n2-highmem-4",
        "disk_config": {"boot_disk_type": "pd-balanced", "boot_disk_size_gb": 100},
    },
    "worker_config": {"num_instances": 0},
    "software_config": {
        "image_version": "2.2-debian12",
        "properties": {"dataproc:dataproc.allow.zero.workers": "true"},
    },
}

PYSPARK_JOB = {
    "reference": {"project_id": PROJECT},
    "placement": {"cluster_name": CLUSTER},
    "pyspark_job": {
        "main_python_file_uri": f"gs://{BUCKET}/jobs/clean_taxi.py",
        "args": [BUCKET, MONTHS],
        # Single node: driver, YARN daemons and executor share 32 GB, so size them explicitly.
        "properties": {
            "spark.driver.memory": "3g",
            "spark.executor.instances": "1",
            "spark.executor.memory": "12g",
            "spark.executor.cores": "3",
            "spark.dynamicAllocation.enabled": "false",
            "spark.sql.shuffle.partitions": "48",
        },
    },
}

with DAG(
    dag_id="p2_clean_taxi_dataproc",
    start_date=datetime(2026, 10, 1),
    schedule=None,
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=2)},
    tags=["nyc-taxi", "part2"],
) as dag:
    upload_job = LocalFilesystemToGCSOperator(
        task_id="upload_job_script",
        src="/opt/airflow/dags/jobs/clean_taxi.py",
        dst="jobs/clean_taxi.py",
        bucket=BUCKET,
    )

    create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster",
        project_id=PROJECT,
        region=REGION,
        cluster_name=CLUSTER,
        cluster_config=CLUSTER_CONFIG,
    )

    run_job = DataprocSubmitJobOperator(
        task_id="run_pyspark_clean",
        project_id=PROJECT,
        region=REGION,
        job=PYSPARK_JOB,
    )

    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster",
        project_id=PROJECT,
        region=REGION,
        cluster_name=CLUSTER,
        trigger_rule="all_done",  # delete even if the job fails
    )

    upload_job >> create_cluster >> run_job >> delete_cluster
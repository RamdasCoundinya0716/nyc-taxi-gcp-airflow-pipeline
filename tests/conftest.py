import os

# Variables are read when the DAG files are parsed; env vars keep parsing offline.
os.environ.setdefault("AIRFLOW_VAR_GCS_BUCKET", "test-bucket")
os.environ.setdefault("AIRFLOW_VAR_TAXI_MONTHS", "2024-01,2024-02")
os.environ.setdefault("AIRFLOW_VAR_GCP_PROJECT_ID", "test-project")
os.environ.setdefault("AIRFLOW_VAR_GCP_REGION", "asia-south1")
os.environ.setdefault("AIRFLOW__CORE__LOAD_EXAMPLES", "False")

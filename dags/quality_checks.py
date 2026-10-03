"""Part 4: SQL data quality checks, then a partitioning/clustering benchmark.

Checks use BigQuery ASSERT, so the task fails if any rule is violated.
The benchmark runs identical queries on the flat and the partitioned+clustered
table with the query cache off and logs bytes processed for each.
"""
from datetime import datetime, timedelta

from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.sdk import DAG, Variable, task

PROJECT = Variable.get("GCP_PROJECT_ID", default="nyc-taxi-pipeline-510514")
REGION = Variable.get("GCP_REGION", default="asia-south1")
DATASET = "nyc_taxi"
T = f"`{PROJECT}.{DATASET}.yellow_trips`"
FLAT = f"`{PROJECT}.{DATASET}.yellow_trips_flat`"

QUALITY_SQL = f"""
ASSERT (SELECT COUNT(*) FROM {T}
        WHERE pickup_ts IS NULL OR dropoff_ts IS NULL OR pu_location_id IS NULL
           OR do_location_id IS NULL OR total_amount IS NULL) = 0
  AS 'null values found in key columns';

ASSERT (SELECT COUNT(*) FROM {T}
        WHERE dropoff_ts <= pickup_ts OR fare_amount < 3 OR trip_distance <= 0 OR total_amount <= 0) = 0
  AS 'invalid trip rows found';

ASSERT (SELECT COUNT(*) FROM (
          SELECT 1 FROM {T}
          GROUP BY vendor_id, pickup_ts, dropoff_ts, pu_location_id, do_location_id, total_amount
          HAVING COUNT(*) > 1)) = 0
  AS 'duplicate trips found';

ASSERT (SELECT COUNT(*) FROM {T}
        WHERE pickup_date < DATE '2024-01-01' OR pickup_date > DATE '2024-12-31') = 0
  AS 'pickup_date outside 2024';

ASSERT (SELECT COUNT(*) FROM {T}) = (SELECT COUNT(*) FROM {FLAT})
  AS 'partitioned and flat tables have different row counts';
"""

BENCHMARKS = {
    "one day + one zone": "pickup_date = DATE '2024-06-15' AND pu_location_id = 132",
    "one month": "pickup_date BETWEEN DATE '2024-06-01' AND DATE '2024-06-30'",
}

with DAG(
    dag_id="p4_quality_and_benchmark",
    start_date=datetime(2026, 10, 1),
    schedule=None,
    catchup=False,
    default_args={"retries": 0, "retry_delay": timedelta(minutes=1)},
    tags=["nyc-taxi", "part4"],
) as dag:
    quality_checks = BigQueryInsertJobOperator(
        task_id="quality_checks",
        location=REGION,
        configuration={"query": {"query": QUALITY_SQL, "useLegacySql": False}},
    )

    @task
    def benchmark() -> list[dict]:
        from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
        from google.cloud import bigquery

        client = BigQueryHook().get_client(project_id=PROJECT, location=REGION)
        cfg = bigquery.QueryJobConfig(use_query_cache=False)
        results = []
        for name, where in BENCHMARKS.items():
            stats = {}
            for label, table in (("flat", FLAT), ("partitioned", T)):
                sql = f"SELECT COUNT(*) AS trips, ROUND(AVG(total_amount), 2) AS avg_total FROM {table} WHERE {where}"
                job = client.query(sql, job_config=cfg)
                job.result()
                stats[label] = {
                    "bytes": job.total_bytes_processed,
                    "ms": int((job.ended - job.started).total_seconds() * 1000),
                    "slot_ms": job.slot_millis,
                }
            cut = 1 - stats["partitioned"]["bytes"] / stats["flat"]["bytes"]
            row = {"query": name, "bytes_reduction_pct": round(cut * 100, 1), **{
                f"{k}_{m}": v for k, d in stats.items() for m, v in d.items()}}
            print("BENCHMARK", row)
            results.append(row)
        return results

    quality_checks >> benchmark()
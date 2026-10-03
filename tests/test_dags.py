from pathlib import Path

import pytest

DAG_DIR = Path(__file__).resolve().parents[1] / "dags"
EXPECTED = {
    "p1_land_nyc_taxi_to_gcs",
    "p2_clean_taxi_dataproc",
    "p3_load_bigquery",
    "p4_quality_and_benchmark",
}


@pytest.fixture(scope="session")
def dagbag():
    from airflow.dag_processing.dagbag import DagBag

    return DagBag(dag_folder=str(DAG_DIR))


def test_no_import_errors(dagbag):
    assert dagbag.import_errors == {}


def test_all_dags_present(dagbag):
    assert EXPECTED <= set(dagbag.dags)


def test_cluster_is_deleted_even_if_job_fails(dagbag):
    task = dagbag.dags["p2_clean_taxi_dataproc"].get_task("delete_cluster")
    assert getattr(task.trigger_rule, "value", task.trigger_rule) == "all_done"


def test_p2_task_order(dagbag):
    dag = dagbag.dags["p2_clean_taxi_dataproc"]
    assert dag.get_task("upload_job_script").downstream_task_ids == {"create_cluster"}
    assert dag.get_task("create_cluster").downstream_task_ids == {"run_pyspark_clean"}
    assert dag.get_task("run_pyspark_clean").downstream_task_ids == {"delete_cluster"}


def test_quality_checks_gate_the_benchmark(dagbag):
    dag = dagbag.dags["p4_quality_and_benchmark"]
    assert dag.get_task("benchmark").upstream_task_ids == {"quality_checks"}


def test_orchestrator_chains_all_stages(dagbag):
    dag = dagbag.dags["p0_nyc_taxi_end_to_end"]
    assert len(dag.tasks) == 4

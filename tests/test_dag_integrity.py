"""Basic DAG integrity tests. Run with: pytest tests/"""
import os

from airflow.models import DagBag


def get_dagbag():
    dags_folder = os.path.join(os.path.dirname(__file__), "..", "dags")
    return DagBag(dag_folder=dags_folder, include_examples=False)


def test_no_import_errors():
    dagbag = get_dagbag()
    assert not dagbag.import_errors, f"DAG import errors: {dagbag.import_errors}"


def test_dag_loaded():
    dagbag = get_dagbag()
    dag = dagbag.get_dag("weather_etl_pipeline")
    assert dag is not None


def test_tasks_have_retries_configured():
    dagbag = get_dagbag()
    dag = dagbag.get_dag("weather_etl_pipeline")
    for task in dag.tasks:
        assert task.retries >= 1, f"{task.task_id} should have retries configured"

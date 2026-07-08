"""DAG-integrity tests: every DAG parses, the expected DAGs exist, and production DAGs
carry the hardening guards (retries + failure alerting, and the Spark/docling pools that
cap concurrency). Smoke-test DAGs are exempt from the retry/alerting checks."""
import os

import pytest

pytestmark = pytest.mark.airflow

DAGS_FOLDER = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "dags"))

EXPECTED_DAGS = {
    "dag_bronze_postgres", "dag_bronze_mysql", "dag_bronze_api", "dag_bronze_pdf",
    "dag_silver_daily", "dag_gold_daily", "dag_silver_monthly", "dag_gold_monthly",
}


@pytest.fixture(scope="module")
def dagbag():
    from airflow.models import DagBag
    return DagBag(dag_folder=DAGS_FOLDER, include_examples=False)


def _production_dags(dagbag):
    return [d for dag_id, d in dagbag.dags.items() if "smoke" not in dag_id]


def _field(task, name):
    """Read an operator field, transparently for mapped (`.expand`) operators whose
    default_args/partial kwargs live in `partial_kwargs`."""
    from airflow.models.mappedoperator import MappedOperator

    if isinstance(task, MappedOperator):
        if name in task.partial_kwargs:
            return task.partial_kwargs[name]
        return (task.dag.default_args or {}).get(name)
    return getattr(task, name, None)


def test_no_import_errors(dagbag):
    assert dagbag.import_errors == {}, f"DAG import errors: {dagbag.import_errors}"


def test_expected_dags_present(dagbag):
    missing = EXPECTED_DAGS - set(dagbag.dag_ids)
    assert not missing, f"missing DAGs: {sorted(missing)}"


def test_production_tasks_have_retries_and_alerting(dagbag):
    offenders = []
    for dag in _production_dags(dagbag):
        for task in dag.tasks:
            retries = _field(task, "retries") or 0
            callback = _field(task, "on_failure_callback")
            if retries < 1 or not callback:
                offenders.append(f"{dag.dag_id}.{task.task_id} (retries={retries}, cb={bool(callback)})")
    assert not offenders, f"tasks missing retries/alerting: {offenders}"


def test_spark_tasks_use_spark_pool(dagbag):
    bad = []
    for dag in _production_dags(dagbag):
        for task in dag.tasks:
            if task.task_id == "bronze_extract" and _field(task, "pool") != "spark":
                bad.append(f"{dag.dag_id}.{task.task_id} pool={_field(task, 'pool')}")
    assert bad == [], f"bronze_extract tasks not in the 'spark' pool: {bad}"


def test_pdf_parse_uses_docling_pool(dagbag):
    dag = dagbag.dags["dag_bronze_pdf"]
    parse = dag.get_task("parse_pdf")
    assert _field(parse, "pool") == "docling"

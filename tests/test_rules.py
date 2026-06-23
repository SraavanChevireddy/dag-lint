from daglint.rules import lint_source


def codes(source: str) -> set[str]:
    return {f.code for f in lint_source(source)}


def test_flags_missing_catchup():
    src = """
from airflow import DAG
import pendulum
dag = DAG("d", start_date=pendulum.datetime(2024,1,1), schedule="@daily")
"""
    assert "DL001" in codes(src)


def test_no_catchup_warning_when_explicit():
    src = """
from airflow import DAG
import pendulum
dag = DAG("d", start_date=pendulum.datetime(2024,1,1), catchup=False,
          default_args={"retries": 2})
"""
    assert "DL001" not in codes(src)


def test_flags_missing_retries():
    src = """
from airflow import DAG
dag = DAG("d", catchup=False)
"""
    assert "DL002" in codes(src)


def test_retries_in_default_args_clears_dl002():
    src = """
from airflow import DAG
dag = DAG("d", catchup=False, default_args={"retries": 3})
"""
    assert "DL002" not in codes(src)


def test_flags_top_level_io():
    src = """
import requests
from airflow import DAG
data = requests.get("http://example.com")   # runs on every parse!
dag = DAG("d", catchup=False, default_args={"retries": 1})
"""
    assert "DL003" in codes(src)


def test_io_inside_function_is_ok():
    src = """
import requests
from airflow import DAG
def fetch():
    return requests.get("http://example.com")
dag = DAG("d", catchup=False, default_args={"retries": 1})
"""
    assert "DL003" not in codes(src)


def test_flags_poke_sensor():
    src = """
from airflow.sensors.python import PythonSensor
s = PythonSensor(task_id="w", python_callable=lambda: True, timeout=60)
"""
    assert "DL004" in codes(src)


def test_reschedule_sensor_is_ok():
    src = """
from airflow.sensors.python import PythonSensor
s = PythonSensor(task_id="w", python_callable=lambda: True,
                 mode="reschedule", timeout=60)
"""
    assert "DL004" not in codes(src)


def test_flags_sensor_without_timeout():
    src = """
from airflow.sensors.python import PythonSensor
s = PythonSensor(task_id="w", python_callable=lambda: True, mode="reschedule")
"""
    assert "DL005" in codes(src)


def test_clean_dag_has_no_findings():
    src = """
from airflow import DAG
import pendulum
dag = DAG("clean", start_date=pendulum.datetime(2024,1,1),
          schedule="@daily", catchup=False, default_args={"retries": 2})
"""
    assert codes(src) == set()

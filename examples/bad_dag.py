"""An intentionally bad DAG to demonstrate what dag-lint catches.

Run:  dag-lint examples/bad_dag.py
"""
import requests
from airflow import DAG
from airflow.sensors.python import PythonSensor

# DL003: this HTTP call runs on EVERY scheduler parse, not at runtime.
CONFIG = requests.get("http://config-service/settings").json()

# DL001 (no catchup) + DL002 (no retries).
dag = DAG("reporting", schedule="@daily")

# DL004 (poke mode holds a worker) + DL005 (no timeout, can hang forever).
wait = PythonSensor(task_id="wait_for_data", python_callable=lambda: True)

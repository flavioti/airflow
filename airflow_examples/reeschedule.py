from __future__ import annotations
import pendulum

from airflow.decorators import dag, task
from airflow.sensors.base import PokeReturnValue


@dag(
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["flavio"],
)
def my_test_a():

    @task.sensor(poke_interval=60, timeout=3600, mode="reschedule")
    def wait_for_upstream() -> PokeReturnValue:
        return PokeReturnValue(is_done=True, xcom_value="xcom_value")

    @task.branch_virtualenv(
        requirements=["pandas"],
        venv_cache_path="inteligencia_adaptativa/venv_cache",
    )
    def task_1() -> None:
        print("task_1")
        return "OK"

    wait_for_upstream() >> task_1()

tutorial_etl_dag = my_test_a()

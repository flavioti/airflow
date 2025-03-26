# 11

from __future__ import annotations

import random

import pendulum

from airflow.decorators import dag, task


@dag(
    schedule=None,
    start_date=pendulum.datetime(2025, 3, 20, tz="America/Sao_Paulo"),
    catchup=False,
    tags=["flavio"],
    dag_display_name="my_test_a",
    concurrency=1,
    max_active_runs=1,
    max_active_tasks=1,
    max_consecutive_failed_dag_runs=1,
    default_args={"retries": 0},
    is_paused_upon_creation=False,
)
def my_test_a():

    @task.virtualenv(
        requirements=[],
        show_return_value_in_logs=True,
        venv_cache_path="mycache/venv_cache",
        provide_context=True,
        multiple_outputs=True,
    )
    def run():
        execution_id = "34234129234012341"
        return {"execution": execution_id}

    @task.sensor(poke_interval=10, mode="reschedule")
    def wait(execution_id):
        print(execution_id)
        if random.random() > 0.5:
            return True
        return False

    @task.virtualenv(
        requirements=[],
        show_return_value_in_logs=True,
        venv_cache_path="mycache/venv_cache",
        provide_context=True,
        multiple_outputs=True,
    )
    def collect_metrics():
        execution_id = "34234129234012341"
        return {"execution": execution_id}
    
    spe = run()
    wait(spe["execution_id"]) >> collect_metrics()


my_dag = my_test_a()

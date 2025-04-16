from datetime import datetime
from airflow.decorators import dag, task
import random

# Define the task function at the module level
@task.virtualenv(
    task_id="print_kwargs_task",  # Unique task ID
    # without dill, error happens: _pickle.PicklingError: Can't pickle <function print_kwargs at 0x7f22fb84eb00>: it's not the same object as unusual_prefix_e3c97fcb24b45cff701129385991f547177d8b06_temp.print_kwargs
    use_dill=True,  # Use dill for serialization if needed
    multiple_outputs=True,  # Enable multiple outputs
    # show_return_value_in_logs=True,  # Show return value in logs
    requirements=["celery"],  # List of requirements for the virtual environment
    system_site_packages=True,  # Use system site packages
    inherit_env=True,  # Inherit environment variables
    # venv_cache_path="mycache/venv_cache",  # Path for caching the virtual environment
    # provide_context=True,  # Provide context to the task
    # op_kwargs={"example_param": 123},  # Example parameter
    # op_args=["example_arg"],  # Example argument
)
def print_kwargs(*args, **kwargs):
    print("params: ", kwargs.get("params", {"example_param": 123}))
    print("conf: ", kwargs["conf"])
    print("dag: ", kwargs["dag"])
    print("dag_run: ", kwargs["dag_run"])
    print("data_interval_end:", kwargs["data_interval_end"])
    print("data_interval_start:", kwargs["data_interval_start"])
    print("ds:", kwargs["ds"])
    print("execution_date:", kwargs["execution_date"])
    print("expanded_ti_count:", kwargs["expanded_ti_count"])
    print("inlets:", kwargs["inlets"])
    print("logical_date:", kwargs["logical_date"])
    print("macros:", kwargs["macros"])
    print("map_index_template:", kwargs["map_index_template"])
    print("next_ds:", kwargs["next_ds"])
    print("next_execution_date:", kwargs["next_execution_date"])
    print("outlets:", kwargs["outlets"])
    print("prev_data_interval_end_success:", kwargs["prev_data_interval_end_success"])
    print("prev_ds:", kwargs["prev_ds"])
    print("prev_execution_date:", kwargs["prev_execution_date"])
    print("prev_execution_date_success:", kwargs["prev_execution_date_success"])
    print("prev_start_date_success:", kwargs["prev_start_date_success"])
    print("prev_end_date_success:", kwargs["prev_end_date_success"])
    print("run_id:", kwargs["run_id"])
    print("task:", kwargs["task"])
    print("task_instance_key_str:", kwargs["task_instance_key_str"])
    print("test_mode:", kwargs["test_mode"])
    print("tomorrow_ds:", kwargs["tomorrow_ds"])
    print("triggering_dataset_events:", kwargs["triggering_dataset_events"])
    print("ts:", kwargs["ts"])
    print("yesterday_ds:", kwargs["yesterday_ds"])
    print("templates_dict:", kwargs["templates_dict"])
    print("args: ", args)
    print("kwargs: ", kwargs)

    return {"foo": "bar"}

@task.sensor(
    poke_interval=10,
    mode="reschedule",
    timeout=30,  # Timeout for the sensor (seconds)
    soft_fail=True,  # Allow the task to fail softly
    retry_delay=10,  # Delay between retries
    retries=3,  # Number of retries
)
def wait(execution_id):
    print(execution_id)
    if random.random() > 0.5:
        return True
    else:
        print("Waiting for condition to be met...")
        if random.random() > 0.5:
            raise Exception("Random exception")
    return False

# Define the DAG
@dag(
    dag_id="example_print_kwargs_dag",  # Unique DAG ID
    default_args={"owner": "airflow"},  # Default arguments for the DAG
    catchup=False,  # Disable catchup, meaning it won't backfill
    tags=["example"],  # Tags for the DAG
    # max_active_runs=6,  # Limit the number of active runs (DAG RUN, not task instance)
    # concurrency=6,  # Limit the number of concurrent tasks
    # max_active_tasks=6,  # Limit the number of active tasks
    # max_consecutive_failed_dag_runs=1,  # Limit consecutive failed runs
    is_paused_upon_creation=False,  # Start the DAG in an unpaused state
    default_view="graph",  # Default view for the UI  ['grid', 'graph', 'duration', 'gantt', 'landing_times']
    dag_display_name="Example Print Kwargs DAG",  # Display name for the DAG
    # params={"example_param": 123},  # Parameters for the DAG
    doc_md="""
    ### Example Print Kwargs DAG
    This DAG demonstrates how to pass and print keyword arguments in a task.
    """,
    start_date=datetime(2023, 1, 1),  # Set a start date
    schedule_interval="* * * * *",  # Set to None for manual triggering
)
def example_print_kwargs_dag():
    # Call the task function
    print_kwargs_task = print_kwargs()
    value = print_kwargs_task["foo"]
    wait(value)

# Instantiate the DAG
example_print_kwargs_dag = example_print_kwargs_dag()

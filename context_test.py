from airflow import DAG
from airflow.decorators import task
from datetime import datetime

@task.virtualenv(
    # use_dill=True,  # Required for passing complex data structures
    # system_site_packages=False,  # Isolate the virtual environment
    # requirements=[],  # Example requirement
    task_id="print_kwargs_task",  # Unique task ID
    # provide_context=True,  # Allow passing context variables
)
def print_kwargs(**kwargs):
    print(kwargs)
    
with DAG(
    dag_id="print_kwargs_dag",
    start_date=datetime(2023, 1, 1),
) as dag:
    print_kwargs_task = print_kwargs()

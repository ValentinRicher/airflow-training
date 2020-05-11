import datetime
import pathlib

from airflow import DAG
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from scripts.move_file import move_file
from utils.config import config

# Following are defaults which can be overridden later on
default_args = {
    "owner": "my_organization",
    "depends_on_past": False,
    "start_date": datetime.datetime(2010, 1, 1),
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=1),
}

dag = DAG(
    "trigger",
    catchup=False,
    default_args=default_args,
    schedule_interval="* * * * *",
    description="This DAG checks when a file is dumped in the 'dumped' folder and if so triggers the main DAG."
)


def conditionally_trigger(context, dag_run_obj):
    """
    Moves the most recent file in the "dumped" folder to the "toprocess" folder,
    if it exists.
    If so returns the dag_run_obj which is the condition to trigger the next DAG.
    For more information see the documentation
    # https://airflow.apache.org/docs/stable/_api/airflow/operators/dagrun_operator/index.html#module-airflow.operators.dagrun_operator

    Parameters
    ----------
    context : dict
        Execution context.
        For more information on the key/values see https://bcb.github.io/airflow/execute-context
    dag_run_obj : airflow.models.dagrun
        Describes an instance of a DAG.
        For more information on the attribute of this object see https://airflow.apache.org/docs/stable/_api/airflow/models/dagrun/index.html#module-airflow.models.dagrun

    Returns
    -------
    dag_run_obj :

    """

    dumped_folder = pathlib.Path(config["dumped_folder"])
    toprocess_folder = pathlib.Path(config["toprocess_folder"])
    files = list(pathlib.Path(dumped_folder).rglob("*.csv"))
    if files:
        dumped_filepath, toprocess_filepath = move_file(
            dumped_folder, toprocess_folder)
        dag_run_obj.payload = {"toprocess_filepath": str(toprocess_filepath)}
        return dag_run_obj


# https://airflow.apache.org/docs/stable/_api/airflow/operators/dagrun_operator/index.html#module-airflow.operators.dagrun_operator
trigger = TriggerDagRunOperator(
    task_id="trigger_dagrun",
    trigger_dag_id="main",
    python_callable=conditionally_trigger,
    dag=dag,
)

trigger

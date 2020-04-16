import datetime
import pathlib
from airflow import DAG
from scripts.move_file import move_file
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from utils.config import config

# Following are defaults which can be overridden later on
default_args = {
    "owner": "my_organization",
    "depends_on_past": False,
    "start_date": datetime.datetime(2010, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=1),
}

dag = DAG(
    "trigger",
    catchup=False,
    default_args=default_args,
    schedule_interval="* * * * *",
)


def conditionally_trigger(context, dag_run_obj):
    """
    """

    c_p = context["params"]["condition_param"]
    print("Controller DAG : conditionally_trigger = {}".format(c_p))

    dumped_folder = pathlib.Path(config["dumped_folder"])
    toprocess_folder = pathlib.Path(config["toprocess_folder"])
    # files = list(pathlib.Path(origin_folder).rglob("*.csv"))
    # files = [str(f) for f in files]
    # if context["params"]["condition_param"]:
    #     dag_run_obj.payload = {"message": context["params"]["message"]}
    #     print(dag_run_obj.payload)
    #     return dag_run_obj
    # if files:
    #     argmin_date = np.argmin(
    #         [
    #             datetime.datetime.strptime(
    #                 str(f.split("/")[-1].split("_")[1]), "%Y%m%dT%H%M"
    #             )
    #             for f in files
    #         ]
    #     )
    #     dump_filepath = pathlib.Path(files[argmin_date])
    #     toprocess_filepath = pathlib.Path(
    #         str(dump_filepath).replace("trips_bucket", "toprocess_bucket")
    #     )
    #     print("dump_filepath: {}".format(dump_filepath))
    #     print("toprocess_filepath: {}".format(toprocess_filepath))
    #     shutil.move(dump_filepath, toprocess_filepath)
    print(dumped_folder)
    print(toprocess_folder)
    dumped_filepath, toprocess_filepath = move_file(
        dumped_folder, toprocess_folder)
    print(dumped_filepath)
    print(toprocess_filepath)
    if toprocess_filepath:
        dag_run_obj.payload = {"toprocess_filepath": str(toprocess_filepath)}
        return dag_run_obj


# https://airflow.apache.org/docs/stable/_api/airflow/operators/dagrun_operator/index.html#module-airflow.operators.dagrun_operator
trigger = TriggerDagRunOperator(
    task_id="trigger_dagrun",
    trigger_dag_id="main",
    python_callable=conditionally_trigger,
    params={"condition_param": True, "message": "Hello World"},
    dag=dag,
)

trigger

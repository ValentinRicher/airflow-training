import random
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from scripts.move_file import move_file
from utils.config import config

default_args = {
    "owner": "nyc",
    "depends_on_past": False,
    "start_date": datetime(2010, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

dag = DAG(
    "feeder",
    catchup=False,
    default_args=default_args,
    schedule_interval="*/5 * * * *",
    description="This feeder DAG simulates the dump of files every five minutes by NYC Open Data."
)

dump_trips = PythonOperator(
    task_id="dump_trips",
    python_callable=move_file,
    op_kwargs={
        'from_folder': config["origin_folder"],
        'to_folder': config["dumped_folder"]},
    dag=dag)

dump_trips

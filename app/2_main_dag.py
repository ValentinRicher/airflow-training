from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import datetime
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook
import pandas as pd

from airflow.models import Connection
from airflow import settings
from utils.config import config
from pathlib import Path


def create_conn(username, password, host=None):
    new_conn = Connection(conn_id=f'postgres_connection',
                                  login=username,
                                  host=host if host else None)
    new_conn.set_password(password)

    session = settings.Session()
    session.add(new_conn)
    session.commit()


create_conn("postgres", "example", "db")

# Following are defaults which can be overridden later on
default_args = {
    "owner": "my_organization",
    "depends_on_past": False,
    "start_date": datetime.datetime.now(),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=1),
}

dag = DAG(
    "main",
    template_searchpath=[config["sql_files"]],
    catchup=False,
    default_args=default_args,
    schedule_interval="*/5 * * * *",
)


# t1, t2, t3 and t4 are examples of tasks created using operators
def run_this_func(ds, **kwargs):
    print(
        "Remotely received value of {} for key=message".format(
            kwargs["dag_run"].conf["toprocess_filepath"]
        )
    )


def clean_data(**kwargs):
    filepath = kwargs["dag_run"].conf["toprocess_filepath"]
    df = pd.read_csv(filepath)
    df.dropna(axis=1, how="all", inplace=True)
    df = df[
        (df["pickup_longitude"] != 0)
        & (df["pickup_latitude"] != 0)
        & (df["dropoff_longitude"] != 0)
        & (df["dropoff_latitude"] != 0)
    ]
    df = df[df["fare_amount"] >= 0]
    cleaned_filepath = Path.joinpath(
        Path(config["cleaned_folder"]), Path(filepath).name)
    df.to_csv(cleaned_filepath, index=False)
    return cleaned_filepath


def load_data_to_db(data):

    insert_sql = "INSERT INTO trips (VendorID, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, trip_distance, pickup_longitude, pickup_latitude, RatecodeID, store_and_fwd_flag, dropoff_longitude, dropoff_latitude, payment_type, fare_amount, extra, mta_tax, tip_amount, tolls_amount, improvement_surcharge, total_amount) VALUES( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

    # create postgres_hook
    pg_hook = PostgresHook(
        postgres_conn_id="postgres_connection", schema="postgres")
    # connect to the PostgreSQL database
    connection = pg_hook.get_conn()
    # create a new cursor
    cur = connection.cursor()
    # execute the INSERT statement
    cur.executemany(insert_sql, data)
    # commit the changes to the database
    connection.commit()
    # close communication with the database
    cur.close()


def push_to_postgres(**kwargs):
    filepath = kwargs["task_instance"].xcom_pull(task_ids="data_cleaner")
    print(filepath)
    df = pd.read_csv(filepath)
    data_list = [list(row) for row in df.itertuples(index=False)]
    load_data_to_db(data_list)


run_this = PythonOperator(
    task_id="run_this", provide_context=True, python_callable=run_this_func, dag=dag
)

table_creator = PostgresOperator(
    task_id="table_creator",
    postgres_conn_id="postgres_connection",
    sql="create_trips_table.sql",
    dag=dag
)

data_cleaner = PythonOperator(
    task_id="data_cleaner",
    python_callable=clean_data,
    provide_context=True,
    dag=dag
)

postgres_pusher = PythonOperator(
    task_id="postgres_pusher",
    python_callable=push_to_postgres,
    provide_context=True,
    dag=dag
)


run_this >> [table_creator, data_cleaner] >> postgres_pusher

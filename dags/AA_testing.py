from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

def hello():
    print("Halo dari Airflow DAG sederhana!")

default_args = {
    "owner": "sensei",
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
}

with DAG(
    dag_id="AAAA_hello_dag",
    default_args=default_args,
    schedule_interval="*/1 * * * *",  # tiap 1 menit
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['contoh', 'sederhana']
) as dag:

    hello_task = PythonOperator(
        task_id="say_hello",
        python_callable=hello
    )

    hello_task

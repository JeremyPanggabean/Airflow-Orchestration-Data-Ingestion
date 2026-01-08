from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import datetime
from dateutil.relativedelta import relativedelta


default_args = {"owner": "airflow", "start_date": datetime(2024, 1, 1)}

with DAG(
    dag_id="1_ingest_csv_bash",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    ingest_orders = BashOperator(
        task_id="ingest_orders",
        bash_command=(
            "PGPASSWORD='postgres' psql -h host.docker.internal -U postgres -d postgres "
            "-c \"\\copy public.orders FROM '/csv/orders.csv' CSV HEADER\""
        ),
    )

    ingest_users = BashOperator(
        task_id="ingest_users",
        bash_command=(
            "PGPASSWORD='postgres' psql -h host.docker.internal -U postgres -d postgres "
            "-c \"\\copy public.users FROM '/csv/users.csv' CSV HEADER\""
        ),
    )

    ingest_products = BashOperator(
        task_id="ingest_products",
        bash_command=(
            "PGPASSWORD='postgres' psql -h host.docker.internal -U postgres -d postgres "
            "-c \"\\copy public.products FROM '/csv/products.csv' CSV HEADER\""
        ),
    )

    [ingest_orders, ingest_users, ingest_products]

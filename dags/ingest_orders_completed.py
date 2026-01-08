from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import datetime
from dateutil.relativedelta import relativedelta


def today_date(execution_date: datetime):
    return execution_date.strftime("%Y-%m-%d")

def prev_month_same_day(execution_date: datetime):
    prev_month_same_day = execution_date - relativedelta(months=1)
    return prev_month_same_day.strftime("%Y-%m-%d")

default_args = {"owner": "airflow", "start_date": datetime(2024, 1, 1)}

with DAG(
    dag_id="2_ingest_orders_completed",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    tags=["orders", "completed"],
) as dag:

    exec_date = datetime.now()
    today = today_date(exec_date)
    prev_month = prev_month_same_day(exec_date)

    ingest_orders_today = PostgresOperator(
        task_id="ingest_orders_today",
        postgres_conn_id="latihan",
        params={"today": today},
        sql="""
            INSERT INTO public.orders_completed (
                order_id, user_id, product_id, order_amount, status, date
            )
            SELECT order_id, user_id, product_id, order_amount, status, date
            FROM public.orders o
            WHERE o.date = '{{ params.today }}';
        """,
    )

    ingest_orders_prev_month_same_day = PostgresOperator(
        task_id="ingest_orders_prev_month_same_day",
        postgres_conn_id="latihan",
        params={"prev_month": prev_month},
        sql="""
            INSERT INTO public.orders_completed (
                order_id, user_id, product_id, order_amount, status, date
            )
            SELECT order_id, user_id, product_id, order_amount, status, date
            FROM public.orders o
            WHERE o.date = '{{ params.prev_month }}';
        """,
    )

    ingest_orders_today >> ingest_orders_prev_month_same_day

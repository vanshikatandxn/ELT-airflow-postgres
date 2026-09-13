"""
Weather ETL pipeline DAG.
Starting minimal: one task that just prints, to confirm Airflow
picks up and runs DAG files correctly before adding real logic.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def say_hello(**context):
    print(f"Hello from weather_etl_pipeline! Logical date: {context['ds']}")


with DAG(
    dag_id="weather_etl_pipeline",
    description="Extract weather data from Open-Meteo, load into Postgres raw/staging/marts",
    default_args=default_args,
    schedule_interval="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["weather", "etl", "postgres"],
) as dag:

    hello = PythonOperator(
        task_id="say_hello",
        python_callable=say_hello,
    )

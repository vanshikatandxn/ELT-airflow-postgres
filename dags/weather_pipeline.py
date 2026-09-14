"""
Weather ETL pipeline DAG.
Extracts current weather from Open-Meteo and loads raw rows into Postgres.
"""
from datetime import datetime, timedelta

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

CITIES = [
    {"name": "Delhi", "latitude": 28.6139, "longitude": 77.2090},
    {"name": "New York", "latitude": 40.7128, "longitude": -74.0060},
    {"name": "London", "latitude": 51.5072, "longitude": -0.1276},
    {"name": "Tokyo", "latitude": 35.6762, "longitude": 139.6503},
]

POSTGRES_CONN_ID = "weather_dw"

default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def extract_and_load(**context):
    ds = context["ds"]
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    rows = []

    for city in CITIES:
        url = (
            "https://api.open-meteo.com/v1/forecast"
            f"?latitude={city['latitude']}&longitude={city['longitude']}"
            "&current_weather=true"
        )
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        payload = response.json()["current_weather"]
        rows.append((
            city["name"], city["latitude"], city["longitude"],
            payload["time"], payload["temperature"],
            payload["windspeed"], payload["weathercode"], ds,
        ))

    hook.insert_rows(
        table="raw.weather_raw",
        rows=rows,
        target_fields=[
            "city", "latitude", "longitude", "observed_at",
            "temperature_c", "windspeed_kmh", "weathercode", "dag_run_date",
        ],
    )


with DAG(
    dag_id="weather_etl_pipeline",
    description="Extract weather data from Open-Meteo, load into Postgres raw/staging/marts",
    default_args=default_args,
    schedule_interval="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    template_searchpath=["/opt/airflow/sql"],
    tags=["weather", "etl", "postgres"],
) as dag:

    create_schemas = PostgresOperator(
        task_id="create_schemas",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="01_create_schemas.sql",
    )

    create_raw_table = PostgresOperator(
        task_id="create_raw_table",
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="02_create_raw_table.sql",
    )

    extract_load = PythonOperator(
        task_id="extract_and_load",
        python_callable=extract_and_load,
    )

    create_schemas >> create_raw_table >> extract_load

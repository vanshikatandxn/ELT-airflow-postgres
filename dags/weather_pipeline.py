"""
Weather ETL pipeline DAG.
Step 3: extract task calls the live Open-Meteo API and prints the
result. No database writes yet - that's the next step.
"""
from datetime import datetime, timedelta

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator

CITIES = [
    {"name": "Delhi", "latitude": 28.6139, "longitude": 77.2090},
    {"name": "New York", "latitude": 40.7128, "longitude": -74.0060},
    {"name": "London", "latitude": 51.5072, "longitude": -0.1276},
    {"name": "Tokyo", "latitude": 35.6762, "longitude": 139.6503},
]

default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def extract_weather(**context):
    for city in CITIES:
        url = (
            "https://api.open-meteo.com/v1/forecast"
            f"?latitude={city['latitude']}&longitude={city['longitude']}"
            "&current_weather=true"
        )
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        payload = response.json()["current_weather"]
        print(f"{city['name']}: {payload}")


with DAG(
    dag_id="weather_etl_pipeline",
    description="Extract weather data from Open-Meteo, load into Postgres raw/staging/marts",
    default_args=default_args,
    schedule_interval="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["weather", "etl", "postgres"],
) as dag:

    extract = PythonOperator(
        task_id="extract_weather",
        python_callable=extract_weather,
    )

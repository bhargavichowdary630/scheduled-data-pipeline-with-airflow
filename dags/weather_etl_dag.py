"""
weather_etl_dag.py
------------------
An Airflow DAG that runs a daily ETL pipeline:

    extract_weather  →  transform_weather  →  load_weather

- Extract : Fetch daily temperature data from the Open-Meteo public API
            (no API key required) for New York City.
- Transform: Parse the JSON response and compute a daily summary.
- Load     : Insert the summary row into a local SQLite database.
"""

import sqlite3
import os
import json
import logging
from datetime import datetime, timedelta

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

LATITUDE = 40.7128       # New York City
LONGITUDE = -74.0060

AIRFLOW_HOME = os.environ.get(
    "AIRFLOW_HOME",
    os.path.join(os.path.dirname(__file__), "..", "airflow_home"),
)
DB_PATH = os.path.join(AIRFLOW_HOME, "weather.db")

API_URL = "https://api.open-meteo.com/v1/forecast"

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Default DAG arguments
# ---------------------------------------------------------------------------

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# ---------------------------------------------------------------------------
# Task functions
# ---------------------------------------------------------------------------

def extract_weather(**context):
    """
    Call the Open-Meteo API and return the raw JSON payload.
    The logical execution date is used so the DAG is idempotent.
    """
    execution_date = context["ds"]          # e.g. "2024-05-01"
    log.info("Extracting weather data for date: %s", execution_date)

    params = {
        "latitude": LATITUDE,
        "longitude": LONGITUDE,
        "daily": ["temperature_2m_max", "temperature_2m_min", "temperature_2m_mean"],
        "timezone": "America/New_York",
        "start_date": execution_date,
        "end_date": execution_date,
    }

    response = requests.get(API_URL, params=params, timeout=30)
    response.raise_for_status()
    payload = response.json()

    log.info("API response received. Keys: %s", list(payload.keys()))

    # Push raw payload to XCom so the next task can pull it
    return json.dumps(payload)


def transform_weather(**context):
    """
    Pull the raw JSON from XCom, parse it, and return a clean dict.
    """
    ti = context["ti"]
    raw_json = ti.xcom_pull(task_ids="extract_weather")
    payload = json.loads(raw_json)

    daily = payload.get("daily", {})
    dates = daily.get("time", [])

    if not dates:
        raise ValueError("No daily data returned by the API.")

    # We requested a single day, so index 0 is our target date
    record = {
        "date": dates[0],
        "latitude": payload["latitude"],
        "longitude": payload["longitude"],
        "temp_max_c": daily.get("temperature_2m_max", [None])[0],
        "temp_min_c": daily.get("temperature_2m_min", [None])[0],
        "temp_mean_c": daily.get("temperature_2m_mean", [None])[0],
        "loaded_at": datetime.utcnow().isoformat(),
    }

    log.info("Transformed record: %s", record)
    return json.dumps(record)


def load_weather(**context):
    """
    Pull the transformed record from XCom and insert it into SQLite.
    """
    ti = context["ti"]
    clean_json = ti.xcom_pull(task_ids="transform_weather")
    record = json.loads(clean_json)

    log.info("Loading record into SQLite at %s", DB_PATH)

    conn = sqlite3.connect(DB_PATH)
    try:
        conn.execute("""
            INSERT INTO weather_data
                (date, latitude, longitude, temp_max_c, temp_min_c, temp_mean_c, loaded_at)
            VALUES
                (:date, :latitude, :longitude, :temp_max_c, :temp_min_c, :temp_mean_c, :loaded_at)
        """, record)
        conn.commit()
        log.info("Record inserted successfully for date: %s", record["date"])
    finally:
        conn.close()

# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

with DAG(
    dag_id="weather_etl_pipeline",
    description="Daily ETL: extract NYC weather → transform → load to SQLite",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,              # Don't backfill historical runs on first deploy
    default_args=default_args,
    tags=["etl", "weather", "beginner"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract_weather",
        python_callable=extract_weather,
    )

    transform_task = PythonOperator(
        task_id="transform_weather",
        python_callable=transform_weather,
    )

    load_task = PythonOperator(
        task_id="load_weather",
        python_callable=load_weather,
    )

    # Define linear task dependencies
    extract_task >> transform_task >> load_task
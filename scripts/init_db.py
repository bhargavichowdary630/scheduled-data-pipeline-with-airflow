"""
init_db.py
----------
Run this once before starting the Airflow pipeline to create the
SQLite database and the weather_data table.

Usage:
    python scripts/init_db.py
"""

import sqlite3
import os

# Place the database inside airflow_home so Airflow can reach it easily.
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", os.path.join(os.getcwd(), "airflow_home"))
DB_PATH = os.path.join(AIRFLOW_HOME, "weather.db")


def init_db():
    os.makedirs(AIRFLOW_HOME, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS weather_data (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            date        TEXT    NOT NULL,
            latitude    REAL    NOT NULL,
            longitude   REAL    NOT NULL,
            temp_max_c  REAL,
            temp_min_c  REAL,
            temp_mean_c REAL,
            loaded_at   TEXT    NOT NULL
        )
    """)

    conn.commit()
    conn.close()
    print(f"Database initialized at: {DB_PATH}")
    print("Table 'weather_data' is ready.")


if __name__ == "__main__":
    init_db()
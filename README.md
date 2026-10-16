# Scheduled Data Pipeline with Airflow

A working Apache Airflow DAG that runs a daily ETL (Extract, Transform, Load) pipeline. It fetches weather data from the Open-Meteo public API (no API key required), transforms the raw JSON into a clean format, and loads it into a local SQLite database.

## What This Demonstrates

- Apache Airflow DAG creation and scheduling
- Task dependencies using `>>` operators
- Python callables as Airflow tasks (PythonOperator)
- XCom for passing data between tasks
- ETL best practices: extract → transform → load
- SQLite as a simple data store

## Project Structure

```
airflow-etl-pipeline/
├── README.md
├── requirements.txt
├── dags/
│   └── weather_etl_dag.py     # The main Airflow DAG
└── scripts/
    └── init_db.py             # Standalone script to initialize the SQLite DB
```

## Prerequisites

- Python 3.8+
- pip

## Setup & Installation

### 1. Clone the repo

```bash
git clone https://github.com/YOUR_USERNAME/airflow-etl-pipeline.git
cd airflow-etl-pipeline
```

### 2. Create and activate a virtual environment

```bash
python -m venv venv
source venv/bin/activate        # On Windows: venv\Scripts\activate
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### 4. Initialize the Airflow database

```bash
export AIRFLOW_HOME=$(pwd)/airflow_home   # On Windows: set AIRFLOW_HOME=%CD%\airflow_home
airflow db init
```

### 5. Initialize the SQLite weather database

```bash
python scripts/init_db.py
```

### 6. Copy the DAG into Airflow's dags folder

```bash
mkdir -p airflow_home/dags
cp dags/weather_etl_dag.py airflow_home/dags/
```

### 7. Create an Airflow admin user

```bash
airflow users create \
  --username admin \
  --password admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com
```

### 8. Start Airflow (two terminal windows)

**Terminal 1 — Web server:**
```bash
export AIRFLOW_HOME=$(pwd)/airflow_home
airflow webserver --port 8080
```

**Terminal 2 — Scheduler:**
```bash
export AIRFLOW_HOME=$(pwd)/airflow_home
airflow scheduler
```

### 9. Open the UI

Navigate to [http://localhost:8080](http://localhost:8080) and log in with `admin / admin`.

Enable the `weather_etl_pipeline` DAG and trigger it manually, or wait for the daily schedule.

## How the Pipeline Works

```
extract_weather  →  transform_weather  →  load_weather
```

| Task | What it does |
|---|---|
| `extract_weather` | Calls Open-Meteo API for current-day temperature data in New York |
| `transform_weather` | Parses and cleans the JSON, adds a timestamp |
| `load_weather` | Inserts the clean record into a local SQLite table |

## Viewing the Results

After the DAG runs, inspect the database:

```bash
python - <<'EOF'
import sqlite3, os
db_path = os.path.join(os.getcwd(), "airflow_home", "weather.db")
conn = sqlite3.connect(db_path)
for row in conn.execute("SELECT * FROM weather_data ORDER BY loaded_at DESC LIMIT 10"):
    print(row)
conn.close()
EOF
```

## Customization Ideas

- Swap SQLite for PostgreSQL or a cloud data warehouse
- Add email alerts on task failure using Airflow's `email_on_failure`
- Extend the transformation step with pandas for richer analytics
- Add a fourth task that exports results to a CSV or S3 bucket

## License

MIT
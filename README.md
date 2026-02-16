# Crypto-Warehouse ELT Pipeline

An ELT (Extract, Load, Transform) pipeline to ingest high-frequency cryptocurrency trade data into a PostgreSQL data warehouse.

## Overview

This project dumps raw JSON data from the CoinGecko API into a staging table in PostgreSQL. It then uses SQL Stored Procedures to parse, clean, and transform this data into a Star Schema (Fact and Dimension tables) for analysis.

## Prerequisites

- **Python 3.8+**
- **PostgreSQL 12+**
- **Pip packages**: `requests`, `psycopg2-binary`, `python-dotenv`, `fastapi`, `uvicorn`

## Setup

1.  **Clone the repository**.
2.  **Install dependencies**:
    ```bash
    pip install -r requirements.txt
    ```
3.  **Configure Database**:
    -   Copy `.env.example` to `.env`.
    -   Update `.env` with your PostgreSQL credentials.
4.  **Initialize Database**:
    Run the setup script to create tables, procedures, and views:
    ```bash
    python src/setup_db.py
    ```
    *This will also pre-populate the `Dim_Date` table from 2020 to 2030.*

## Usage

### Run the Pipeline
To fetch data and process it immediately:
```bash
python src/extract_load.py
```

### Run the Metrics API
Start the REST API to expose pipeline, data quality, and performance metrics:
```bash
uvicorn api:app --app-dir src --reload
```

Endpoints:
- `GET /metrics/pipeline`
- `GET /metrics/data-quality`
- `GET /metrics/performance`

### Scheduling
To simulate a streaming environment, schedule the script to run every 10-30 minutes.

**Windows Task Scheduler**:
1.  Create a Basic Task.
2.  Trigger: "Daily", repeat every 10 minutes.
3.  Action: "Start a program".
    -   Program/script: `python` (or path to python executable)
    -   Arguments: `c:\path\to\project\src\extract_load.py`

**Cron (Linux/Mac)**:
```bash
*/10 * * * * /usr/bin/python3 /path/to/project/src/extract_load.py
```

## Architecture

1.  **Extract**: Python fetches JSON from CoinGecko.
2.  **Load**: Raw JSON is inserted into `Staging_API_Response`.
3.  **Transform**: `sp_ParseRawData` stored procedure:
    -   Parses JSON.
    -   Checks for NULL prices (logs error to `Data_Quality_Logs`).
    -   Updates `Dim_Currency`.
    -   Inserts into `Fact_Market_Metrics`.
4.  **Analyze**: Views provided:
    -   `vw_MovingAverages`
    -   `vw_Volatility`
    -   `vw_DailyVolumeRank`
5.  **Observe**: Pipeline run status is tracked in `Pipeline_Run_Logs` and surfaced via the API.

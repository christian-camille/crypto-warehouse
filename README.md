# Crypto-Warehouse ELT Pipeline

An ELT (Extract, Load, Transform) pipeline to ingest high-frequency cryptocurrency trade data into a PostgreSQL data warehouse.

## Overview

This project dumps raw JSON data from the CoinGecko API into a staging table in PostgreSQL. It then uses SQL Stored Procedures to parse, clean, and transform this data into a Star Schema (Fact and Dimension tables) for analysis.

## Prerequisites

- **Docker** and **Docker Compose** (for Docker setup), or:
- **Python 3.8+**, **PostgreSQL 12+**, and **Pip packages**: `requests`, `psycopg2-binary`, `python-dotenv`, `fastapi`, `uvicorn`

## Setup

### Option A: Docker (Recommended)

1.  **Clone the repository**.
2.  **Start all services**:
    ```bash
    docker compose up --build
    ```
    This starts PostgreSQL and the API server. Database schema setup runs automatically on first launch.

    - API available at `http://localhost:8000`
    - PostgreSQL available at `localhost:5432`

3.  **Run pipeline commands** inside the running container:
    ```bash
    docker compose exec app python src/extract_load.py
    docker compose exec app python src/backfill_history.py --days 90 --top-coins 20
    docker compose exec app python src/analysis_report.py --output-dir outputs --formats csv json
    ```

4.  **Stop services**:
    ```bash
    docker compose down
    ```
    Add `-v` to also remove the database volume.

Source code is volume-mounted (`src/`, `sql/`, `frontend/`, `outputs/`), so changes reflect without rebuilding.

### Option B: Local

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
- `GET /analytics/moving-averages`
- `GET /analytics/volatility`
- `GET /analytics/daily-volume-rank`
- `GET /analytics/market-cap-trends`
- `GET /analytics/price-correlation`
- `GET /analytics/anomaly-detection`
- `GET /analytics/market-health`

Example endpoint calls:
```bash
curl "http://127.0.0.1:8000/analytics/moving-averages?limit=100"
curl "http://127.0.0.1:8000/analytics/price-correlation?limit=400&min_overlap=24"
curl "http://127.0.0.1:8000/analytics/anomaly-detection?limit=200&anomaly_only=true"
```

### Generate Insights + Exports
Create a markdown insights report and export analytical view outputs as CSV/JSON:
```bash
python src/analysis_report.py --output-dir outputs --formats csv json --limit-per-view 5000
```

### Backfill Historical Data (3 Months)
Backfill recent history from CoinGecko into staging, then parse into warehouse tables:
```bash
python src/backfill_history.py --days 90 --top-coins 20
```

Notes:
- Uses CoinGecko `market_chart/range` and converts payloads to the existing staging JSON shape.
- Inserts one staged snapshot per timestamp and then calls `sp_ParseRawData`.
- Use `--top-coins` to control runtime/API volume and `--pause-seconds` to reduce rate-limit risk.

Example output artifacts:
- `outputs/reports/insights_YYYYMMDD_HHMMSS.md`
- `outputs/exports/vw_moving_averages.csv`
- `outputs/exports/vw_moving_averages.json`
- `outputs/exports/vw_volatility.csv`
- `outputs/exports/vw_volatility.json`
- `outputs/exports/vw_daily_volume_rank.csv`
- `outputs/exports/vw_daily_volume_rank.json`
- `outputs/exports/vw_market_cap_trends.csv`
- `outputs/exports/vw_market_cap_trends.json`
- `outputs/exports/vw_price_correlation.csv`
- `outputs/exports/vw_price_correlation.json`
- `outputs/exports/vw_anomaly_detection.csv`
- `outputs/exports/vw_anomaly_detection.json`
- `outputs/exports/vw_market_health.csv`
- `outputs/exports/vw_market_health.json`

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
    -   `vw_MarketCapTrends`
    -   `vw_PriceCorrelation`
    -   `vw_AnomalyDetection`
    -   `vw_MarketHealth`
5.  **Observe**: Pipeline run status is tracked in `Pipeline_Run_Logs` and surfaced via the API.

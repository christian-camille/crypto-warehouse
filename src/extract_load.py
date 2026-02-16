import requests
import psycopg2
import json
import os
import datetime
import time
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_NAME = os.getenv("DB_NAME", "crypto_warehouse")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASS = os.getenv("DB_PASS", "password")
DB_PORT = os.getenv("DB_PORT", "5432")

# CoinGecko API URL
API_URL = "https://api.coingecko.com/api/v3/coins/markets"
PARAMS = {
    "vs_currency": "usd",
    "order": "market_cap_desc",
    "per_page": 100,
    "page": 1,
    "sparkline": "false"
}

def get_crypto_data():
    """Fetches cryptocurrency data from CoinGecko API."""
    try:
        print(f"Fetching data from {API_URL}...")
        response = requests.get(API_URL, params=PARAMS)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
        return None

def load_raw_data(data):
    """Inserts raw JSON data into the staging table."""
    conn = None
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASS,
            port=DB_PORT
        )
        cur = conn.cursor()
        
        # Insert raw JSON
        sql = "INSERT INTO Staging_API_Response (RawJSON) VALUES (%s)"
        cur.execute(sql, (json.dumps(data),))
        
        conn.commit()
        cur.close()
        print("Successfully inserted raw data into Staging_API_Response.")
        return True
        
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error loading data to Postgres: {error}")
        return False
    finally:
        if conn is not None:
            conn.close()

def run_pipeline():
    print(f"Starting pipeline execution at {datetime.datetime.now()}")
    run_id = None
    try:
        run_id = log_pipeline_start()

        # 1. Extract
        data = get_crypto_data()

        if not data:
            log_pipeline_end(run_id, "FAILED", "No data fetched")
            print("No data fetched, skipping load.")
            return

        # 2. Load (Raw)
        if not load_raw_data(data):
            raise RuntimeError("Failed to load raw data")

        # 3. Transform (Trigger Stored Procedure)
        # Note: We could trigger this here or rely on a DB trigger/schedule.
        # For this script, let's trigger it immediately to complete the flow.
        if not trigger_transformation():
            raise RuntimeError("Failed to trigger transformation")

        log_pipeline_end(run_id, "SUCCESS", None)
    except Exception as error:
        print(f"Pipeline failed: {error}")
        if run_id is not None:
            log_pipeline_end(run_id, "FAILED", str(error))

def trigger_transformation():
    """Calls the stored procedure to parse and transform data."""
    conn = None
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASS,
            port=DB_PORT
        )
        cur = conn.cursor()
        
        print("Triggering transformation (sp_ParseRawData)...")
        cur.execute("CALL sp_ParseRawData();")
        
        conn.commit()
        cur.close()
        print("Transformation complete.")
        return True
        
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error executing transformation: {error}")
        return False
    finally:
        if conn is not None:
            conn.close()

def log_pipeline_start():
    """Creates a pipeline run log entry and returns the RunID."""
    conn = None
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASS,
            port=DB_PORT
        )
        cur = conn.cursor()
        cur.execute("INSERT INTO Pipeline_Run_Logs (Status) VALUES ('RUNNING') RETURNING RunID;")
        run_id = cur.fetchone()[0]
        conn.commit()
        cur.close()
        return run_id
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error creating pipeline run log: {error}")
        return None
    finally:
        if conn is not None:
            conn.close()

def log_pipeline_end(run_id, status, error_message):
    """Updates a pipeline run log entry with final status and timing."""
    if run_id is None:
        return
    conn = None
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASS,
            port=DB_PORT
        )
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE Pipeline_Run_Logs
            SET EndedAt = CURRENT_TIMESTAMP,
                Status = %s,
                ErrorMessage = %s
            WHERE RunID = %s
            """,
            (status, error_message, run_id)
        )
        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error updating pipeline run log: {error}")
    finally:
        if conn is not None:
            conn.close()

if __name__ == "__main__":
    run_pipeline()

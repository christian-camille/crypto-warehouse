import requests
import psycopg2
import json
import os
import datetime
import time
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Database connection parameters - SHOULD BE ENV VARS IN PROD/REAL USE
# For now, we will use placeholders or env vars if available
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
        
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error loading data to Postgres: {error}")
    finally:
        if conn is not None:
            conn.close()

def run_pipeline():
    print(f"Starting pipeline execution at {datetime.datetime.now()}")
    
    # 1. Extract
    data = get_crypto_data()
    
    if data:
        # 2. Load (Raw)
        load_raw_data(data)
        
        # 3. Transform (Trigger Stored Procedure)
        # Note: We could trigger this here or rely on a DB trigger/schedule.
        # For this script, let's trigger it immediately to complete the flow.
        trigger_transformation()
    else:
        print("No data fetched, skipping load.")

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
        
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error executing transformation: {error}")
    finally:
        if conn is not None:
            conn.close()

if __name__ == "__main__":
    run_pipeline()

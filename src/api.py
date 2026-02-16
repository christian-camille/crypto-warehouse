from fastapi import FastAPI, HTTPException
import os
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

load_dotenv()

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_NAME = os.getenv("DB_NAME", "crypto_warehouse")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASS = os.getenv("DB_PASS", "password")
DB_PORT = os.getenv("DB_PORT", "5432")

app = FastAPI(title="Crypto Warehouse Metrics API")


def get_connection():
    return psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
        port=DB_PORT,
        cursor_factory=RealDictCursor
    )


@app.get("/metrics/pipeline")
def get_pipeline_metrics():
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT
                COUNT(*) AS total_runs,
                COUNT(*) FILTER (WHERE Status = 'FAILED') AS failed_runs,
                COUNT(*) FILTER (WHERE Status = 'SUCCESS') AS success_runs,
                ROUND(
                    COUNT(*) FILTER (WHERE Status = 'SUCCESS')::numeric
                    / NULLIF(COUNT(*), 0) * 100,
                    2
                ) AS success_rate_pct,
                MAX(EndedAt) AS last_run_at,
                ROUND(
                    AVG(EXTRACT(EPOCH FROM (EndedAt - StartedAt)))
                    FILTER (WHERE EndedAt IS NOT NULL),
                    2
                ) AS avg_run_seconds,
                ROUND(
                    MAX(EXTRACT(EPOCH FROM (EndedAt - StartedAt)))
                    FILTER (WHERE EndedAt IS NOT NULL),
                    2
                ) AS last_run_seconds
            FROM Pipeline_Run_Logs;
            """
        )
        result = cur.fetchone()
        cur.close()
        conn.close()
        return result
    except Exception as error:
        raise HTTPException(status_code=500, detail=str(error))


@app.get("/metrics/data-quality")
def get_data_quality_metrics():
    try:
        conn = get_connection()
        cur = conn.cursor()

        cur.execute(
            """
            SELECT
                SUM(CASE WHEN PriceUSD IS NULL THEN 1 ELSE 0 END) AS missing_price,
                SUM(CASE WHEN MarketCapUSD IS NULL THEN 1 ELSE 0 END) AS missing_marketcap,
                SUM(CASE WHEN Volume24hUSD IS NULL THEN 1 ELSE 0 END) AS missing_volume
            FROM Fact_Market_Metrics;
            """
        )
        missing = cur.fetchone()

        cur.execute(
            """
            SELECT COUNT(*) AS duplicate_rows
            FROM (
                SELECT CurrencyID, Timestamp, COUNT(*) AS c
                FROM Fact_Market_Metrics
                GROUP BY CurrencyID, Timestamp
                HAVING COUNT(*) > 1
            ) dupes;
            """
        )
        duplicates = cur.fetchone()

        cur.execute(
            """
            WITH deltas AS (
                SELECT
                    CurrencyID,
                    Timestamp,
                    CASE
                        WHEN LAG(PriceUSD) OVER (PARTITION BY CurrencyID ORDER BY Timestamp) IS NULL
                            THEN NULL
                        ELSE (PriceUSD - LAG(PriceUSD) OVER (PARTITION BY CurrencyID ORDER BY Timestamp))
                            / NULLIF(LAG(PriceUSD) OVER (PARTITION BY CurrencyID ORDER BY Timestamp), 0)
                            * 100
                    END AS pct_change
                FROM Fact_Market_Metrics
            )
            SELECT COUNT(*) AS anomaly_count
            FROM deltas
            WHERE pct_change IS NOT NULL AND ABS(pct_change) >= 50;
            """
        )
        anomalies = cur.fetchone()

        cur.execute(
            """
            SELECT ErrorLevel, COUNT(*) AS count
            FROM Data_Quality_Logs
            GROUP BY ErrorLevel
            ORDER BY ErrorLevel;
            """
        )
        dq_logs = cur.fetchall()

        cur.close()
        conn.close()

        return {
            "missing_values": missing,
            "duplicates": duplicates,
            "anomalies": anomalies,
            "data_quality_logs": dq_logs
        }
    except Exception as error:
        raise HTTPException(status_code=500, detail=str(error))


@app.get("/metrics/performance")
def get_performance_metrics():
    try:
        conn = get_connection()
        cur = conn.cursor()

        cur.execute(
            """
            SELECT
                MAX(Timestamp) AS latest_fact_timestamp,
                ROUND(EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - MAX(Timestamp))), 2) AS data_freshness_seconds
            FROM Fact_Market_Metrics;
            """
        )
        freshness = cur.fetchone()

        cur.execute(
            """
            SELECT
                ROUND(
                    AVG(EXTRACT(EPOCH FROM (EndedAt - StartedAt)))
                    FILTER (WHERE EndedAt IS NOT NULL),
                    2
                ) AS avg_processing_seconds,
                ROUND(
                    MAX(EXTRACT(EPOCH FROM (EndedAt - StartedAt)))
                    FILTER (WHERE EndedAt IS NOT NULL),
                    2
                ) AS last_processing_seconds
            FROM Pipeline_Run_Logs;
            """
        )
        processing = cur.fetchone()

        cur.execute(
            """
            SELECT
                COUNT(*) AS total_fact_rows,
                COUNT(*) FILTER (WHERE Timestamp >= CURRENT_TIMESTAMP - INTERVAL '24 hours') AS last_24h_rows,
                COUNT(DISTINCT CurrencyID) AS distinct_currencies
            FROM Fact_Market_Metrics;
            """
        )
        row_counts = cur.fetchone()

        cur.close()
        conn.close()

        return {
            "data_freshness": freshness,
            "processing_time": processing,
            "row_counts": row_counts
        }
    except Exception as error:
        raise HTTPException(status_code=500, detail=str(error))

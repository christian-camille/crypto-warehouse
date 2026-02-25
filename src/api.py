from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
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

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def get_connection():
    return psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
        port=DB_PORT,
        cursor_factory=RealDictCursor
    )


def fetch_all_rows(sql, params=None):
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(sql, params or ())
        rows = cur.fetchall()
        cur.close()
        return rows
    finally:
        conn.close()


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
                MAX(StartedAt) AS last_started_at,
                (
                    SELECT Status
                    FROM Pipeline_Run_Logs
                    ORDER BY StartedAt DESC
                    LIMIT 1
                ) AS last_run_status,
                (
                    SELECT COUNT(*)
                    FROM Pipeline_Run_Logs
                    WHERE StartedAt::date = CURRENT_DATE
                ) AS runs_today,
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

        cur.execute(
            """
            WITH recent AS (
                SELECT StartedAt
                FROM Pipeline_Run_Logs
                ORDER BY StartedAt DESC
                LIMIT 20
            ),
            ordered AS (
                SELECT StartedAt, LAG(StartedAt) OVER (ORDER BY StartedAt) AS prev
                FROM recent
            )
            SELECT ROUND(AVG(EXTRACT(EPOCH FROM (StartedAt - prev))) / 60, 2) AS avg_interval_minutes
            FROM ordered
            WHERE prev IS NOT NULL;
            """
        )
        avg_interval = cur.fetchone()

        cur.execute(
            """
            SELECT ROUND(EXTRACT(EPOCH FROM (EndedAt - StartedAt)) / 60, 2) AS minutes
            FROM Pipeline_Run_Logs
            WHERE EndedAt IS NOT NULL
            ORDER BY EndedAt DESC
            LIMIT 12;
            """
        )
        durations = [row["minutes"] for row in cur.fetchall()]
        durations.reverse()

        if result is not None:
            result["avg_interval_minutes"] = avg_interval.get("avg_interval_minutes")
            result["duration_trend_minutes"] = durations
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
                COUNT(*) AS total_rows,
                COUNT(*) FILTER (
                    WHERE PriceUSD IS NULL OR MarketCapUSD IS NULL OR Volume24hUSD IS NULL
                ) AS missing_rows,
                SUM(CASE WHEN PriceUSD IS NULL THEN 1 ELSE 0 END) AS missing_price,
                SUM(CASE WHEN MarketCapUSD IS NULL THEN 1 ELSE 0 END) AS missing_marketcap,
                SUM(CASE WHEN Volume24hUSD IS NULL THEN 1 ELSE 0 END) AS missing_volume
            FROM Fact_Market_Metrics;
            """
        )
        missing = cur.fetchone()

        completeness_pct = None
        if missing and missing.get("total_rows"):
            completeness_pct = round(
                100 * (1 - (missing.get("missing_rows", 0) / missing["total_rows"])),
                2
            )

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
            WITH hourly_prices AS (
                SELECT
                    CurrencyID,
                    DATE_TRUNC('hour', Timestamp) AS HourBucket,
                    AVG(PriceUSD) AS PriceUSD
                FROM Fact_Market_Metrics
                WHERE PriceUSD IS NOT NULL
                GROUP BY CurrencyID, DATE_TRUNC('hour', Timestamp)
            ), deltas AS (
                SELECT
                    current_hour.CurrencyID,
                    current_hour.HourBucket AS Timestamp,
                    ((current_hour.PriceUSD - previous_hour.PriceUSD)
                        / NULLIF(previous_hour.PriceUSD, 0)) * 100 AS pct_change
                FROM hourly_prices current_hour
                LEFT JOIN hourly_prices previous_hour
                    ON previous_hour.CurrencyID = current_hour.CurrencyID
                    AND previous_hour.HourBucket = current_hour.HourBucket - INTERVAL '1 hour'
            )
            SELECT COUNT(*) AS anomaly_count
            FROM deltas
            WHERE pct_change IS NOT NULL AND ABS(pct_change) >= 50;
            """
        )
        anomalies = cur.fetchone()

        cur.execute(
            """
            WITH hourly AS (
                SELECT
                    date_trunc('hour', Timestamp) AS bucket,
                    COUNT(*) AS total_rows,
                    COUNT(*) FILTER (
                        WHERE PriceUSD IS NULL OR MarketCapUSD IS NULL OR Volume24hUSD IS NULL
                    ) AS missing_rows
                FROM Fact_Market_Metrics
                WHERE Timestamp >= CURRENT_TIMESTAMP - INTERVAL '12 hours'
                GROUP BY bucket
            )
            SELECT
                bucket,
                ROUND(100 * (1 - missing_rows::numeric / NULLIF(total_rows, 0)), 2) AS completeness_pct
            FROM hourly
            ORDER BY bucket;
            """
        )
        completeness_trend = cur.fetchall()

        cur.execute(
            """
            WITH hourly_prices AS (
                SELECT
                    CurrencyID,
                    DATE_TRUNC('hour', Timestamp) AS HourBucket,
                    AVG(PriceUSD) AS PriceUSD
                FROM Fact_Market_Metrics
                WHERE Timestamp >= CURRENT_TIMESTAMP - INTERVAL '13 hours'
                  AND PriceUSD IS NOT NULL
                GROUP BY CurrencyID, DATE_TRUNC('hour', Timestamp)
            ), deltas AS (
                SELECT
                    current_hour.CurrencyID,
                    current_hour.HourBucket AS Timestamp,
                    ((current_hour.PriceUSD - previous_hour.PriceUSD)
                        / NULLIF(previous_hour.PriceUSD, 0)) * 100 AS pct_change
                FROM hourly_prices current_hour
                LEFT JOIN hourly_prices previous_hour
                    ON previous_hour.CurrencyID = current_hour.CurrencyID
                    AND previous_hour.HourBucket = current_hour.HourBucket - INTERVAL '1 hour'
                WHERE current_hour.HourBucket >= CURRENT_TIMESTAMP - INTERVAL '12 hours'
            ),
            hourly AS (
                SELECT
                    date_trunc('hour', Timestamp) AS bucket,
                    COUNT(*) FILTER (WHERE pct_change IS NOT NULL AND ABS(pct_change) >= 50) AS outliers
                FROM deltas
                GROUP BY bucket
            )
            SELECT bucket, outliers
            FROM hourly
            ORDER BY bucket;
            """
        )
        outliers_trend = cur.fetchall()

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
            "completeness_pct": completeness_pct,
            "completeness_trend": completeness_trend,
            "outliers_trend": outliers_trend,
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
                COUNT(*) AS staging_rows,
                pg_total_relation_size('staging_api_response') AS staging_bytes
            FROM Staging_API_Response;
            """
        )
        staging = cur.fetchone()

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
            "staging": staging,
            "row_counts": row_counts
        }
    except Exception as error:
        raise HTTPException(status_code=500, detail=str(error))


@app.get("/analytics/market-cap-trends")
def get_market_cap_trends(limit: int = Query(default=500, ge=1, le=5000)):
    try:
        rows = fetch_all_rows(
            """
            SELECT *
            FROM vw_MarketCapTrends
            ORDER BY MonthStart DESC, MarketCapRank ASC
            LIMIT %s;
            """,
            (limit,)
        )
        return {"count": len(rows), "rows": rows}
    except Exception as error:
        raise HTTPException(status_code=500, detail=str(error))


@app.get("/analytics/moving-averages")
def get_moving_averages(limit: int = Query(default=500, ge=1, le=10000)):
    try:
        rows = fetch_all_rows(
            """
            SELECT *
            FROM vw_MovingAverages
            ORDER BY FullDate DESC, Currency ASC
            LIMIT %s;
            """,
            (limit,)
        )
        return {"count": len(rows), "rows": rows}
    except Exception as error:
        raise HTTPException(status_code=500, detail=str(error))


@app.get("/analytics/volatility")
def get_volatility(limit: int = Query(default=500, ge=1, le=10000)):
    try:
        rows = fetch_all_rows(
            """
            SELECT *
            FROM vw_Volatility
            ORDER BY Timestamp DESC, Currency ASC
            LIMIT %s;
            """,
            (limit,)
        )
        return {"count": len(rows), "rows": rows}
    except Exception as error:
        raise HTTPException(status_code=500, detail=str(error))


@app.get("/analytics/daily-volume-rank")
def get_daily_volume_rank(limit: int = Query(default=500, ge=1, le=10000)):
    try:
        rows = fetch_all_rows(
            """
            SELECT *
            FROM vw_DailyVolumeRank
            ORDER BY FullDate DESC, VolumeRank ASC
            LIMIT %s;
            """,
            (limit,)
        )
        return {"count": len(rows), "rows": rows}
    except Exception as error:
        raise HTTPException(status_code=500, detail=str(error))


@app.get("/analytics/price-correlation")
def get_price_correlation(
    limit: int = Query(default=400, ge=1, le=10000),
    min_overlap: int = Query(default=0, ge=0)
):
    try:
        rows = fetch_all_rows(
            """
            SELECT *
            FROM vw_PriceCorrelation
            WHERE COALESCE(OverlappingObservations, 0) >= %s
               OR BaseCurrencyID = ComparedCurrencyID
            ORDER BY BaseMarketCapRank, ComparedMarketCapRank
            LIMIT %s;
            """,
            (min_overlap, limit)
        )
        return {"count": len(rows), "rows": rows}
    except Exception as error:
        raise HTTPException(status_code=500, detail=str(error))


@app.get("/analytics/anomaly-detection")
def get_anomaly_detection(
    limit: int = Query(default=500, ge=1, le=10000),
    anomaly_only: bool = Query(default=True)
):
    try:
        if anomaly_only:
            rows = fetch_all_rows(
                """
                SELECT *
                FROM vw_AnomalyDetection
                WHERE IsAnomaly = TRUE
                ORDER BY Timestamp DESC
                LIMIT %s;
                """,
                (limit,)
            )
        else:
            rows = fetch_all_rows(
                """
                SELECT *
                FROM vw_AnomalyDetection
                ORDER BY Timestamp DESC
                LIMIT %s;
                """,
                (limit,)
            )
        return {"count": len(rows), "rows": rows}
    except Exception as error:
        raise HTTPException(status_code=500, detail=str(error))


@app.get("/analytics/market-health")
def get_market_health(limit: int = Query(default=365, ge=1, le=5000)):
    try:
        rows = fetch_all_rows(
            """
            SELECT *
            FROM vw_MarketHealth
            ORDER BY FullDate DESC
            LIMIT %s;
            """,
            (limit,)
        )
        return {"count": len(rows), "rows": rows}
    except Exception as error:
        raise HTTPException(status_code=500, detail=str(error))


@app.get("/metrics/dashboard")
def get_dashboard_metrics():
    try:
        pipeline = get_pipeline_metrics()
        data_quality = get_data_quality_metrics()
        performance = get_performance_metrics()
        return {
            "pipeline": pipeline,
            "data_quality": data_quality,
            "performance": performance
        }
    except Exception as error:
        raise HTTPException(status_code=500, detail=str(error))

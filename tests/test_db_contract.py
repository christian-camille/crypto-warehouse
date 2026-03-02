"""
Contract tests – require a live PostgreSQL instance.

These tests are automatically skipped when Postgres is unreachable
(see the db_schema / db_conn fixtures in conftest.py).

Run with: python -m pytest tests/test_db_contract.py -v
"""

import json
import pytest


# ---------------------------------------------------------------------------
# Expected schema objects
# ---------------------------------------------------------------------------

EXPECTED_TABLES = [
    "staging_api_response",
    "pipeline_run_logs",
    "dim_currency",
    "dim_date",
    "fact_market_metrics",
    "data_quality_logs",
]

# ---------------------------------------------------------------------------
# Sample staging payload (CoinGecko markets shape)
# ---------------------------------------------------------------------------

SAMPLE_PAYLOAD = [
    {
        "id": "bitcoin",
        "symbol": "btc",
        "name": "Bitcoin",
        "current_price": 50000.0,
        "market_cap": 950_000_000_000.0,
        "total_volume": 28_000_000_000.0,
        "max_supply": 21_000_000.0,
    },
    {
        "id": "ethereum",
        "symbol": "eth",
        "name": "Ethereum",
        "current_price": 3200.0,
        "market_cap": 385_000_000_000.0,
        "total_volume": 15_000_000_000.0,
        "max_supply": None,
    },
]

# ---------------------------------------------------------------------------
# View → expected columns (lowercase, matching cursor.description names)
# ---------------------------------------------------------------------------

VIEW_EXPECTED_COLUMNS = {
    "vw_movingaverages": {
        "fulldate", "currency", "priceusd", "movingavg7day",
    },
    "vw_volatility": {
        "timestamp", "currency", "priceusd", "prevhourprice", "pctchangehourly",
    },
    "vw_dailyvolumerank": {
        "fulldate", "currency", "totaldailyvolume", "volumerank",
    },
    "vw_marketcaptrends": {
        "monthstart", "currencyid", "currency", "avgmarketcapusd",
        "peakmarketcapusd", "mommarketcapchangepct", "yoymarketcapchangepct",
        "marketcaprank", "momchangerank", "yoychangerank",
    },
    "vw_pricecorrelation": {
        "basecurrencyid", "basecurrency", "comparedcurrencyid", "comparedcurrency",
        "correlationvalue", "overlappingobservations",
        "basemarketcaprank", "comparedmarketcaprank",
    },
    "vw_anomalydetection": {
        "timestamp", "currencyid", "currency", "priceusd", "volume24husd",
        "hourlyreturnpct", "pricezscore", "volumezscore",
        "p99absreturnpct", "p99volumeusd", "isanomaly", "anomalyseverity",
    },
    "vw_markethealth": {
        "fulldate", "marketvolatility", "avgabsreturn", "avgpairwisecorrelation",
        "avgvolume24husd", "volatilityscore", "correlationscore", "volumescore",
        "markethealthscore", "markethealthstate",
    },
}


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_setup_db_creates_expected_objects(db_conn):
    """
    After setup_db runs the DDL, all six core tables must exist in the
    public schema. Catches missing CREATE TABLE statements and typos.
    """
    cur = db_conn.cursor()
    cur.execute(
        """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
          AND table_type = 'BASE TABLE';
        """
    )
    existing = {row[0] for row in cur.fetchall()}
    cur.close()

    missing = set(EXPECTED_TABLES) - existing
    assert not missing, f"Tables missing from schema: {missing}"


def test_sp_parserawdata_inserts_into_dim_and_fact(db_conn):
    """
    Insert one known staging payload, call sp_ParseRawData, then verify:
    - The two coins appear in Dim_Currency.
    - Two rows land in Fact_Market_Metrics.
    - Staging_API_Response is empty afterward (proc deletes processed rows).
    The entire test is wrapped in a transaction that rolls back on teardown.
    """
    cur = db_conn.cursor()

    cur.execute(
        "INSERT INTO Staging_API_Response (RawJSON) VALUES (%s)",
        (json.dumps(SAMPLE_PAYLOAD),),
    )
    cur.execute("CALL sp_ParseRawData();")

    # Dim_Currency should contain both coins
    cur.execute(
        "SELECT CoinGeckoID FROM Dim_Currency WHERE CoinGeckoID IN ('bitcoin', 'ethereum');"
    )
    found_ids = {row[0] for row in cur.fetchall()}
    assert found_ids == {"bitcoin", "ethereum"}, (
        f"Expected both coins in Dim_Currency, got {found_ids}"
    )

    # Fact_Market_Metrics should have exactly 2 rows for those coins
    cur.execute(
        """
        SELECT COUNT(*)
        FROM Fact_Market_Metrics f
        JOIN Dim_Currency c ON f.CurrencyID = c.CurrencyID
        WHERE c.CoinGeckoID IN ('bitcoin', 'ethereum');
        """
    )
    fact_count = cur.fetchone()[0]
    assert fact_count == 2, (
        f"Expected 2 rows in Fact_Market_Metrics, got {fact_count}"
    )

    # Staging row should have been deleted by the procedure
    cur.execute("SELECT COUNT(*) FROM Staging_API_Response;")
    staging_count = cur.fetchone()[0]
    assert staging_count == 0, (
        f"Expected Staging_API_Response to be empty after sp_ParseRawData, got {staging_count}"
    )

    cur.close()


@pytest.mark.parametrize("view_name,expected_cols", VIEW_EXPECTED_COLUMNS.items())
def test_views_return_expected_columns(db_conn, view_name, expected_cols):
    """
    For each analytics view, SELECT * LIMIT 0 and compare cursor.description
    column names against the set the API contracts rely on.
    Catches silent SQL drift when views are edited.
    """
    cur = db_conn.cursor()
    cur.execute(f"SELECT * FROM {view_name} LIMIT 0;")  # noqa: S608

    actual_cols = {desc[0].lower() for desc in cur.description}
    cur.close()

    missing = expected_cols - actual_cols
    assert not missing, (
        f"View '{view_name}' is missing columns that the API expects: {missing}"
    )

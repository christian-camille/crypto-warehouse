import os
import pytest
import psycopg2


def _conn_params():
    return dict(
        host=os.getenv("DB_HOST", "localhost"),
        dbname=os.getenv("DB_NAME", "crypto_warehouse"),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASS", "password"),
        port=os.getenv("DB_PORT", "5432"),
    )


@pytest.fixture(scope="session")
def db_schema():
    """
    Run DDL, procedures, and views once per test session.
    Skips all dependant tests when Postgres is unreachable.
    """
    try:
        conn = psycopg2.connect(**_conn_params())
    except psycopg2.OperationalError as exc:
        pytest.skip(f"Postgres not available – skipping contract tests: {exc}")

    conn.autocommit = True
    cur = conn.cursor()

    root = os.path.join(os.path.dirname(__file__), "..")
    for sql_file in ["sql/01_ddl.sql", "sql/02_procedures.sql", "sql/03_views.sql"]:
        path = os.path.normpath(os.path.join(root, sql_file))
        with open(path) as f:
            cur.execute(f.read())

    # Ensure Dim_Date is populated so sp_ParseRawData can look up date keys
    cur.execute("CALL sp_PopulateDateDim('2020-01-01', '2030-12-31');")

    cur.close()
    conn.close()


@pytest.fixture()
def db_conn(db_schema):
    """
    Yield a psycopg2 connection for a single test, then roll back so each
    test starts with a clean slate (no persistent side-effects).
    """
    conn = psycopg2.connect(**_conn_params())
    conn.autocommit = False
    yield conn
    conn.rollback()
    conn.close()

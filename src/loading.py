"""
loading.py
=============
Module responsible for loading transformed stock market data
into a DuckDB analytical database.

Reads cleaned Parquet files produced by transformation.py and
creates two tables in the DuckDB database:
    - kpis        : daily financial KPIs per ticker
    - correlations: pairwise correlation between tickers

Author: Salima
Date: 07/2026
"""

import duckdb


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DB_PATH = "/mnt/e/finance_platform/data/finance_platform.duckdb"
PARQUET_KPIS_PATH = "/mnt/e/finance_platform/data/clean/stock_data_transformed.parquet"
PARQUET_CORR_PATH = "/mnt/e/finance_platform/data/clean/correlations.parquet"


# ---------------------------------------------------------------------------
# Functions
# ---------------------------------------------------------------------------

def _create_connection() -> duckdb.DuckDBPyConnection:
    """
    Create a connection to the DuckDB database.

    Args:
        None

    Returns:
        duckdb.DuckDBPyConnection: Active DuckDB connection.
    """
    return duckdb.connect(DB_PATH)


def _load_table(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
    parquet_path: str
) -> int:
    """
    Load a Parquet file into a DuckDB table.

    Creates or replaces the table with data from the given Parquet file.
    Uses CREATE OR REPLACE to handle daily pipeline re-runs gracefully.

    Args:
        con (duckdb.DuckDBPyConnection): Active DuckDB connection.
        table_name (str): Target table name in DuckDB.
        parquet_path (str): Absolute path to the source Parquet file.

    Returns:
        int: Number of rows loaded into the table.
    """
    con.execute(f"""
        CREATE OR REPLACE TABLE {table_name} AS
        SELECT * FROM read_parquet('{parquet_path}')
    """)

    row_count = con.execute(
        f"SELECT COUNT(*) FROM {table_name}"
    ).fetchone()[0]

    return row_count


def load_data() -> None:
    """
    Load all transformed data into the DuckDB analytical database.

    Connects to DuckDB and loads two tables:
        - kpis        : ~2500 rows (500 trading days x 5 tickers)
        - correlations: 10 rows (unique pairs among 5 tickers)

    The function uses CREATE OR REPLACE TABLE to safely handle
    daily pipeline re-runs without manual cleanup.

    Returns:
        None
    """
    print("Starting data loading into DuckDB...")

    con = _create_connection()

    # Load KPIs table
    print("Loading KPIs table...")
    kpi_count = _load_table(con, "kpis", PARQUET_KPIS_PATH)
    print(f"Table 'kpis' loaded successfully — {kpi_count} rows")

    # Load correlations table
    print("Loading correlations table...")
    corr_count = _load_table(con, "correlations", PARQUET_CORR_PATH)
    print(f"Table 'correlations' loaded successfully — {corr_count} rows")

    con.close()
    print("Loading complete. DuckDB connection closed.")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    load_data()
"""
batch_pipeline.py
=================
Apache Airflow DAG for the Finance Data Platform batch pipeline.

This DAG orchestrates the daily ETL pipeline for stock market data:
    1. extraction    : fetch OHLCV data from Yahoo Finance API
    2. transformation: compute financial KPIs using Apache Spark
    3. loading       : store results in DuckDB analytical database

Schedule: daily at 08:00 UTC
Start date: 2024-01-01

Author: Salima
Date: 07/2026
"""

import sys
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


# ---------------------------------------------------------------------------
# Path Configuration
# ---------------------------------------------------------------------------

# Add src/ to Python path so Airflow can locate our modules
sys.path.insert(0, "/mnt/e/finance_platform/src")

from extraction import extract_data
from transformation import transform_data
from loading import load_data


# ---------------------------------------------------------------------------
# DAG Definition
# ---------------------------------------------------------------------------

with DAG(
    dag_id="finance_batch_pipeline",
    description="Daily ETL pipeline for stock market data — Spark + DuckDB",
    schedule="0 8 * * *",   # every day at 08:00 UTC
    start_date=datetime(2024, 1, 1),
    catchup=False,           # do not backfill past runs
    tags=["finance", "batch", "spark", "duckdb"]
) as dag:

    # -----------------------------------------------------------------------
    # Task 1 — Extract
    # -----------------------------------------------------------------------
    extract_task = PythonOperator(
        task_id="extraction",
        python_callable=extract_data,
        doc_md="""
        **Extraction Task**

        Fetches 2 years of OHLCV stock data for AAPL, GOOGL, AMZN, MSFT, TSLA
        from Yahoo Finance API and saves it as Parquet format.
        """,
    )

    # -----------------------------------------------------------------------
    # Task 2 — Transform
    # -----------------------------------------------------------------------
    transform_task = PythonOperator(
        task_id="transformation",
        python_callable=transform_data,
        doc_md="""
        **Transformation Task**

        Reads raw Parquet data and computes financial KPIs using Apache Spark:
        daily returns, moving averages (7d/30d), volatility, Sharpe Ratio,
        Maximum Drawdown, RSI, and correlation matrix.
        """,
    )

    # -----------------------------------------------------------------------
    # Task 3 — Load
    # -----------------------------------------------------------------------
    load_task = PythonOperator(
        task_id="loading",
        python_callable=load_data,
        doc_md="""
        **Loading Task**

        Loads transformed Parquet files into DuckDB:
            - kpis         : daily KPIs per ticker
            - correlations : pairwise correlation matrix
        """,
    )

    # -----------------------------------------------------------------------
    # Pipeline Execution Order
    # -----------------------------------------------------------------------

    # extraction → transformation → loading
    # Each task only starts if the previous one succeeded
    extract_task >> transform_task >> load_task
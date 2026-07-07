"""
extraction.py
=============
Module responsible for extracting stock market data from Yahoo Finance API
and saving it as Parquet format for downstream Spark processing.

Author: Salima
Date: 07/2026
"""

import os
from datetime import datetime, timedelta

import pandas as pd
import yfinance as yf


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

TICKERS = ["AAPL", "GOOGL", "AMZN", "MSFT", "TSLA"]
LOOKBACK_DAYS = 730  # 2 years of trading data
OUTPUT_PATH = "/mnt/e/finance_platform/data/raw/stock_data.parquet"


# ---------------------------------------------------------------------------
# Functions
# ---------------------------------------------------------------------------

def _download_ticker(ticker: str, start_date: str, end_date: str) -> pd.DataFrame:
    """
    Download OHLCV data for a single ticker from Yahoo Finance.

    Args:
        ticker (str): Stock ticker symbol (e.g. 'AAPL').
        start_date (str): Start date in 'YYYY-MM-DD' format.
        end_date (str): End date in 'YYYY-MM-DD' format.

    Returns:
        pd.DataFrame: DataFrame with columns [date, open, high, low,
                      close, volume, ticker].
    """
    df = yf.download(ticker, start=start_date, end=end_date, progress=False)

    # Flatten MultiIndex columns returned by yfinance
    # e.g. ('Close', 'AAPL') → 'close'
    df.columns = [col[0].lower() for col in df.columns]

    df["ticker"] = ticker
    df = df.reset_index()  # convert date index to regular column

    # Normalize all column names to lowercase
    df.columns = [col.lower() for col in df.columns]

    return df


def extract_data() -> str:
    """
    Extract stock market data for all tracked tickers from Yahoo Finance.

    Fetches 2 years of OHLCV (Open, High, Low, Close, Volume) data
    for each ticker in TICKERS, combines them into a single DataFrame,
    and saves it as a Parquet file compatible with Apache Spark.

    Returns:
        str: Absolute path to the saved Parquet file.

    Example:
        >>> output_path = extract_data()
        >>> print(output_path)
        /mnt/e/finance_platform/data/raw/stock_data.parquet
    """

    # Define date range
    end_date = datetime.today().strftime("%Y-%m-%d")
    start_date = (datetime.today() - timedelta(days=LOOKBACK_DAYS)).strftime("%Y-%m-%d")

    print(f"   Period : {start_date} → {end_date}")

    # Download data for each ticker
    all_data = []
    for ticker in TICKERS:
        df = _download_ticker(ticker, start_date, end_date)
        all_data.append(df)

    # Combine all tickers into a single DataFrame
    # Each ticker contributes ~500 rows (trading days)
    df_final = pd.concat(all_data, ignore_index=True)

    # Save as Parquet with version 2.6 for Spark compatibility
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    df_final.to_parquet(OUTPUT_PATH, index=False, engine="pyarrow", version="2.6")

    print(f"Extraction completed — {len(df_final)} rows saved to {OUTPUT_PATH}")
    print(f"   Columns : {list(df_final.columns)}")

    return OUTPUT_PATH


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    extract_data()
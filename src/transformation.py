"""
transformation.py
=================
Module responsible for transforming raw stock market data using Apache Spark.

Computes key financial KPIs including:
    - Daily returns
    - Moving averages (7-day and 30-day)
    - Volatility (30-day rolling standard deviation)
    - Sharpe Ratio (risk-adjusted return)
    - Maximum Drawdown (worst peak-to-trough loss)
    - RSI - Relative Strength Index (14-day)
    - Correlation matrix between tickers

Author: Salima
Date: 07/2026
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

INPUT_PATH = "/mnt/e/finance_platform/data/raw/stock_data.parquet"
OUTPUT_PATH = "/mnt/e/finance_platform/data/clean/stock_data_transformed.parquet"
CORR_OUTPUT_PATH = "/mnt/e/finance_platform/data/clean/correlations.parquet"

TICKERS = ["AAPL", "GOOGL", "AMZN", "MSFT", "TSLA"]

# Number of trading days per year (excludes weekends and public holidays)
TRADING_DAYS_PER_YEAR = 252

# Risk-free rate (annualized) — based on US Treasury bonds
RISK_FREE_RATE_ANNUAL = 0.02

# RSI standard window (Welles Wilder, 1978)
RSI_WINDOW = 14


# ---------------------------------------------------------------------------
# Spark Session
# ---------------------------------------------------------------------------

def _create_spark_session() -> SparkSession:
    """
    Create and configure a local Apache Spark session.

    Returns:
        SparkSession: Configured Spark session using all available CPU cores.
    """
    return (
        SparkSession.builder
        .appName("FinancePlatform")
        .master("local[*]")
        .getOrCreate()
    )


# ---------------------------------------------------------------------------
# KPI Computation Functions
# ---------------------------------------------------------------------------

def _compute_daily_returns(df: DataFrame) -> DataFrame:
    """
    Compute daily percentage return for each ticker.

    Formula: (close_today - close_yesterday) / close_yesterday * 100

    Args:
        df (DataFrame): Spark DataFrame with columns [date, ticker, close].

    Returns:
        DataFrame: Input DataFrame with additional 'daily_return' column.
    """
    window = Window.partitionBy("ticker").orderBy("date")

    return df.withColumn(
        "daily_return",
        (F.col("close") - F.lag("close", 1).over(window))
        / F.lag("close", 1).over(window) * 100
    )


def _compute_moving_averages(df: DataFrame) -> DataFrame:
    """
    Compute 7-day and 30-day simple moving averages of closing price.

    Moving averages smooth price fluctuations to reveal trends:
        - MA7  : short-term trend
        - MA30 : long-term trend

    Args:
        df (DataFrame): Spark DataFrame with columns [date, ticker, close].

    Returns:
        DataFrame: Input DataFrame with additional 'ma7' and 'ma30' columns.
    """
    # rowsBetween(-N+1, 0) defines a rolling window of N trading days
    window_7 = (
        Window.partitionBy("ticker")
        .orderBy("date")
        .rowsBetween(-6, 0)
    )
    window_30 = (
        Window.partitionBy("ticker")
        .orderBy("date")
        .rowsBetween(-29, 0)
    )

    return (
        df
        .withColumn("ma7", F.avg("close").over(window_7))
        .withColumn("ma30", F.avg("close").over(window_30))
    )


def _compute_volatility(df: DataFrame) -> DataFrame:
    """
    Compute 30-day rolling volatility as the standard deviation of daily returns.

    Volatility measures price instability over a 30-day trading window.
    A higher value indicates greater price fluctuation and investment risk.

    Args:
        df (DataFrame): Spark DataFrame with 'daily_return' column.

    Returns:
        DataFrame: Input DataFrame with additional 'volatility_30d' column.
    """
    window_30 = (
        Window.partitionBy("ticker")
        .orderBy("date")
        .rowsBetween(-29, 0)
    )

    return df.withColumn(
        "volatility_30d",
        F.stddev("daily_return").over(window_30)
    )


def _compute_sharpe_ratio(df: DataFrame) -> DataFrame:
    """
    Compute the 30-day rolling Sharpe Ratio for each ticker.

    The Sharpe Ratio measures risk-adjusted return:
        sharpe = (avg_return - risk_free_rate) / volatility

    A Sharpe Ratio above 1.0 is generally considered good.
    Above 2.0 is considered excellent.

    Args:
        df (DataFrame): Spark DataFrame with 'daily_return' column.

    Returns:
        DataFrame: Input DataFrame with additional 'sharpe_ratio' column.
    """
    # Convert annual risk-free rate to daily trading rate
    daily_risk_free_rate = RISK_FREE_RATE_ANNUAL / TRADING_DAYS_PER_YEAR

    window_30 = (
        Window.partitionBy("ticker")
        .orderBy("date")
        .rowsBetween(-29, 0)
    )

    return df.withColumn(
        "sharpe_ratio",
        (F.avg("daily_return").over(window_30) - daily_risk_free_rate)
        / F.stddev("daily_return").over(window_30)
    )


def _compute_max_drawdown(df: DataFrame) -> DataFrame:
    """
    Compute the Maximum Drawdown for each ticker.

    Maximum Drawdown measures the worst peak-to-trough loss:
        drawdown = (close - peak) / peak * 100

    Where peak is the highest closing price from the beginning
    of the series up to the current date.

    Args:
        df (DataFrame): Spark DataFrame with 'close' column.

    Returns:
        DataFrame: Input DataFrame with additional 'max_drawdown' column.
        Note: Intermediate 'peak' column is dropped before returning.
    """
    # unboundedPreceding means "from the very first row" — no lookback limit
    window_max = (
        Window.partitionBy("ticker")
        .orderBy("date")
        .rowsBetween(Window.unboundedPreceding, 0)
    )

    return (
        df
        .withColumn("peak", F.max("close").over(window_max))
        .withColumn(
            "max_drawdown",
            (F.col("close") - F.col("peak")) / F.col("peak") * 100
        )
        .drop("peak")  # intermediate column, not needed in final output
    )


def _compute_rsi(df: DataFrame) -> DataFrame:
    """
    Compute the 14-day Relative Strength Index (RSI) for each ticker.

    RSI is a momentum oscillator ranging from 0 to 100:
        - RSI > 70 : overbought — potential sell signal
        - RSI < 30 : oversold  — potential buy signal
        - RSI 30-70: neutral zone

    Formula: RSI = 100 - (100 / (1 + avg_gain / avg_loss))

    The 14-day window is the industry standard introduced by
    J. Welles Wilder in 1978.

    Args:
        df (DataFrame): Spark DataFrame with 'daily_return' column.

    Returns:
        DataFrame: Input DataFrame with additional 'rsi' column.
        Note: Intermediate columns (gain, loss, avg_gain, avg_loss)
              are dropped before returning.
    """
    window_14 = (
        Window.partitionBy("ticker")
        .orderBy("date")
        .rowsBetween(-13, 0)
    )

    df = (
        df
        # Separate positive returns (gains) from negative (losses)
        .withColumn(
            "gain",
            F.when(F.col("daily_return") > 0, F.col("daily_return")).otherwise(0)
        )
        .withColumn(
            "loss",
            F.when(F.col("daily_return") < 0, F.abs(F.col("daily_return"))).otherwise(0)
        )
        .withColumn("avg_gain", F.avg("gain").over(window_14))
        .withColumn("avg_loss", F.avg("loss").over(window_14))
    )

    return (
        df
        .withColumn(
            "rsi",
            # Edge cases: all gains → RSI=100, all losses → RSI=0
            F.when(F.col("avg_loss") == 0, 100)
             .when(F.col("avg_gain") == 0, 0)
             .otherwise(100 - (100 / (1 + F.col("avg_gain") / F.col("avg_loss"))))
        )
        # Drop intermediate columns not needed in final output
        .drop("gain", "loss", "avg_gain", "avg_loss")
    )


def _compute_correlations(df: DataFrame, spark: SparkSession) -> DataFrame:
    """
    Compute pairwise Pearson correlation between all ticker daily returns.

    Correlation measures how two assets move relative to each other:
        +1.0 : perfect positive correlation (move together)
         0.0 : no correlation (independent)
        -1.0 : perfect negative correlation (move oppositely)

    Useful for portfolio diversification analysis.

    Args:
        df (DataFrame): Spark DataFrame with 'daily_return' and 'ticker' columns.
        spark (SparkSession): Active Spark session for DataFrame creation.

    Returns:
        DataFrame: DataFrame with columns [ticker1, ticker2, correlation],
                   containing all unique ticker pairs.
    """
    # Pivot to get one column per ticker for correlation computation
    df_pivot = df.groupBy("date").pivot("ticker").agg(
        F.first("daily_return")
    )

    correlations = []
    for i in range(len(TICKERS)):
        for j in range(i + 1, len(TICKERS)):
            ticker1 = TICKERS[i]
            ticker2 = TICKERS[j]
            corr_value = df_pivot.stat.corr(ticker1, ticker2)
            correlations.append((ticker1, ticker2, corr_value))

    return spark.createDataFrame(
        correlations,
        ["ticker1", "ticker2", "correlation"]
    )


# ---------------------------------------------------------------------------
# Main Transformation Function
# ---------------------------------------------------------------------------

def transform_data() -> str:
    """
    Run the full transformation pipeline on raw stock market data.

    Reads raw OHLCV Parquet data, applies all KPI computations using
    Apache Spark, and saves the results as clean Parquet files.

    Pipeline steps:
        1. Read raw Parquet file
        2. Compute daily returns
        3. Compute moving averages (7d, 30d)
        4. Compute volatility (30d)
        5. Compute Sharpe Ratio (30d)
        6. Compute Maximum Drawdown
        7. Compute RSI (14d)
        8. Compute correlation matrix
        9. Save transformed data and correlations

    Returns:
        str: Path to the transformed Parquet file.
    """
    print("Starting Spark transformation pipeline...")

    spark = _create_spark_session()

    # Read raw extracted data
    print("Reading raw Parquet data...")
    df = spark.read.parquet(INPUT_PATH)
    print(f"Schema: {df.schema.simpleString()}")

    # Apply KPI transformations sequentially
    print("Computing daily returns...")
    df = _compute_daily_returns(df)

    print("Computing moving averages (7d, 30d)...")
    df = _compute_moving_averages(df)

    print("Computing 30-day volatility...")
    df = _compute_volatility(df)

    print("Computing Sharpe Ratio...")
    df = _compute_sharpe_ratio(df)

    print("Computing Maximum Drawdown...")
    df = _compute_max_drawdown(df)

    print("Computing RSI (14-day)...")
    df = _compute_rsi(df)

    # Compute and save correlation matrix separately
    # (different structure: one row per ticker pair, not per ticker per day)
    print("Computing correlation matrix...")
    df_corr = _compute_correlations(df, spark)
    df_corr.write.mode("overwrite").parquet(CORR_OUTPUT_PATH)
    print("Correlation matrix saved.")
    df_corr.show()

    # Save final transformed KPIs
    print("Saving transformed data...")
    df.write.mode("overwrite").parquet(OUTPUT_PATH)

    print(f"Transformation complete. Output saved to: {OUTPUT_PATH}")

    spark.stop()
    return OUTPUT_PATH


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    transform_data()
"""
app.py
======
Streamlit interactive dashboard for the Finance Data Platform.

Displays financial KPIs and analytics for 5 major stock tickers
(AAPL, GOOGL, AMZN, MSFT, TSLA) stored in a DuckDB database.

Pages:
    - General Overview : historical prices, moving averages, key metrics
    - KPIs per Stock   : RSI with overbought/oversold signals
    - Correlations     : interactive heatmap and correlation table

Author: Salima
Date: 07/2026
"""

import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DB_PATH = "/mnt/e/finance_platform/data/finance_platform.duckdb"
TICKERS = ["AAPL", "GOOGL", "AMZN", "MSFT", "TSLA"]

# RSI thresholds — industry standard (Welles Wilder, 1978)
RSI_OVERBOUGHT = 70
RSI_OVERSOLD = 30


# ---------------------------------------------------------------------------
# Page Configuration
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="Finance Data Platform",
    page_icon="📈",
    layout="wide"
)


# ---------------------------------------------------------------------------
# Database Connection
# ---------------------------------------------------------------------------

@st.cache_resource
def get_connection() -> duckdb.DuckDBPyConnection:
    """
    Create and cache a DuckDB database connection.

    Uses Streamlit's cache_resource to maintain a single connection
    across all user interactions, avoiding repeated reconnections.

    Returns:
        duckdb.DuckDBPyConnection: Active DuckDB connection.
    """
    return duckdb.connect(DB_PATH)


con = get_connection()


# ---------------------------------------------------------------------------
# Data Loading Functions
# ---------------------------------------------------------------------------

@st.cache_data
def load_kpis(_con: duckdb.DuckDBPyConnection, ticker: str) -> pd.DataFrame:
    """
    Load KPI data for a specific ticker from DuckDB.

    Args:
        _con (duckdb.DuckDBPyConnection): Active DuckDB connection.
            Prefixed with underscore to prevent Streamlit from hashing it.
        ticker (str): Stock ticker symbol (e.g. 'AAPL').

    Returns:
        pd.DataFrame: DataFrame with all KPI columns ordered by date.
    """
    return _con.execute(f"""
        SELECT *
        FROM kpis
        WHERE ticker = '{ticker}'
        ORDER BY date
    """).df()


@st.cache_data
def load_correlations(_con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """
    Load the full correlation matrix from DuckDB.

    Args:
        _con (duckdb.DuckDBPyConnection): Active DuckDB connection.

    Returns:
        pd.DataFrame: DataFrame with columns [ticker1, ticker2, correlation].
    """
    return _con.execute("SELECT * FROM correlations").df()


# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------

st.sidebar.title("Finance Data Platform")
st.sidebar.markdown("---")

page = st.sidebar.selectbox(
    "Navigation",
    ["General Overview", "KPIs per Stock", "Correlations"]
)

st.sidebar.markdown("---")

selected_ticker = st.sidebar.selectbox(
    "Select a stock",
    TICKERS
)

# Load data for selected ticker
df = load_kpis(con, selected_ticker)
df_corr = load_correlations(con)


# ---------------------------------------------------------------------------
# Page 1 — General Overview
# ---------------------------------------------------------------------------

if page == "General Overview":
    st.title("General Overview")

    # Key metrics row
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric(
            label="Current Price",
            value=f"${df['close'].iloc[-1]:.2f}",
            delta=f"{df['daily_return'].iloc[-1]:.2f}%"
        )
    with col2:
        st.metric(
            label="Sharpe Ratio",
            value=f"{df['sharpe_ratio'].iloc[-1]:.2f}"
        )
    with col3:
        st.metric(
            label="30d Volatility",
            value=f"{df['volatility_30d'].iloc[-1]:.2f}%"
        )
    with col4:
        st.metric(
            label="Max Drawdown",
            value=f"{df['max_drawdown'].iloc[-1]:.2f}%"
        )

    st.markdown("---")

    # Historical price chart with moving averages
    st.subheader(f"Historical Price — {selected_ticker}")

    fig = go.Figure()

    # Closing price — solid line (real data)
    fig.add_trace(go.Scatter(
        x=df["date"],
        y=df["close"],
        name="Close Price",
        line=dict(color="blue", width=2)
    ))

    # 7-day moving average — dashed (computed indicator)
    fig.add_trace(go.Scatter(
        x=df["date"],
        y=df["ma7"],
        name="MA7",
        line=dict(color="orange", width=1, dash="dash")
    ))

    # 30-day moving average — dashed (computed indicator)
    fig.add_trace(go.Scatter(
        x=df["date"],
        y=df["ma30"],
        name="MA30",
        line=dict(color="red", width=1, dash="dash")
    ))

    fig.update_layout(
        xaxis_title="Date",
        yaxis_title="Price (USD)",
        hovermode="x unified",
        height=500
    )

    st.plotly_chart(fig, width="stretch")


# ---------------------------------------------------------------------------
# Page 2 — KPIs per Stock
# ---------------------------------------------------------------------------

elif page == "KPIs per Stock":
    st.title(f"KPIs — {selected_ticker}")

    # RSI Chart
    st.subheader("Relative Strength Index (RSI)")
    st.markdown("""
    - **RSI > 70** : overbought — potential sell signal
    - **RSI < 30** : oversold — potential buy signal
    """)

    fig_rsi = go.Figure()

    fig_rsi.add_trace(go.Scatter(
        x=df["date"],
        y=df["rsi"],
        name="RSI",
        line=dict(color="purple", width=2)
    ))

    # Overbought threshold
    fig_rsi.add_hline(
        y=RSI_OVERBOUGHT,
        line_dash="dash",
        line_color="red",
        annotation_text=f"Overbought ({RSI_OVERBOUGHT})"
    )

    # Oversold threshold
    fig_rsi.add_hline(
        y=RSI_OVERSOLD,
        line_dash="dash",
        line_color="green",
        annotation_text=f"Oversold ({RSI_OVERSOLD})"
    )

    fig_rsi.update_layout(
        yaxis_title="RSI",
        height=400,
        yaxis=dict(range=[0, 100])
    )

    st.plotly_chart(fig_rsi, width="stretch")

    # Additional KPIs
    st.markdown("---")
    st.subheader("Additional KPIs")

    col1, col2 = st.columns(2)

    with col1:
        # Volatility chart
        st.markdown("**30-day Rolling Volatility**")
        fig_vol = px.line(
            df, x="date", y="volatility_30d",
            labels={"volatility_30d": "Volatility (%)", "date": "Date"}
        )
        st.plotly_chart(fig_vol, width="stretch")

    with col2:
        # Drawdown chart
        st.markdown("**Maximum Drawdown**")
        fig_dd = px.area(
            df, x="date", y="max_drawdown",
            labels={"max_drawdown": "Drawdown (%)", "date": "Date"},
            color_discrete_sequence=["red"]
        )
        st.plotly_chart(fig_dd, width="stretch")


# ---------------------------------------------------------------------------
# Page 3 — Correlations
# ---------------------------------------------------------------------------

elif page == "Correlations":
    st.title("Correlation Matrix")

    st.markdown("""
    **How to read this matrix:**
    - Close to **+1** : assets move together (positive correlation)
    - Close to **0**  : assets are independent
    - Close to **-1** : assets move in opposite directions
    """)

    # Build correlation matrix from pairwise data
    matrix = pd.DataFrame(index=TICKERS, columns=TICKERS, dtype=float)

    for _, row in df_corr.iterrows():
        matrix.loc[row["ticker1"], row["ticker2"]] = row["correlation"]
        matrix.loc[row["ticker2"], row["ticker1"]] = row["correlation"]

    # Diagonal = 1.0 (each asset is perfectly correlated with itself)
    for ticker in TICKERS:
        matrix.loc[ticker, ticker] = 1.0

    # Interactive heatmap
    fig_corr = px.imshow(
        matrix,
        color_continuous_scale="RdBu_r",
        zmin=-1,
        zmax=1,
        text_auto=".2f",
        title="Pairwise Correlation — Daily Returns"
    )

    fig_corr.update_layout(height=500)
    st.plotly_chart(fig_corr, width="stretch")

    # Sorted correlation table
    st.subheader("Correlation Details")
    df_corr_sorted = df_corr.sort_values("correlation", ascending=False)
    st.dataframe(df_corr_sorted, width="stretch")
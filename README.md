# Finance Data Platform

A production-grade batch data pipeline processing real-time stock market data
for 5 major companies (AAPL, GOOGL, AMZN, MSFT, TSLA).

## Architecture

## Tech Stack

| Tool | Purpose |
|------|---------|
| Python | Core language |
| PySpark | Large-scale data transformation |
| Apache Airflow | Pipeline orchestration |
| DuckDB | Analytical storage |
| Streamlit | Interactive dashboard |
| Plotly | Data visualization |
| yfinance | Market data extraction |
| Parquet | Columnar storage format |

## Financial KPIs

| KPI | Description |
|-----|-------------|
| Daily Returns | Day-over-day price change (%) |
| Moving Averages | 7-day and 30-day price trends |
| Volatility | 30-day rolling standard deviation |
| Sharpe Ratio | Risk-adjusted return metric |
| Maximum Drawdown | Worst peak-to-trough loss |
| RSI | Relative Strength Index (14-day) |
| Correlation Matrix | Cross-asset correlation analysis |

## Project Structure

## Getting Started

### Prerequisites

- Python 3.12+
- Java 8+ (required for PySpark)
- Apache Airflow 3.x

### Installation

```bash
# Clone the repository
git clone https://github.com/YOUR_USERNAME/finance-platform.git
cd finance-platform

# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### Run the Pipeline Manually

```bash
# Step 1 — Extract
python3 src/extraction.py

# Step 2 — Transform
python3 src/transformation.py

# Step 3 — Load
python3 src/loading.py
```

### Run with Airflow

```bash
export AIRFLOW_HOME=$(pwd)
airflow standalone
```

Open **http://localhost:8080** and trigger `finance_batch_pipeline`.

### Launch Dashboard

```bash
streamlit run dashboard/app.py
```

Open **http://localhost:8501** in your browser.

## Dashboard Preview

### General Overview
![General Overview](screenshots/overview.jpeg)

### KPIs per Stock
![KPIs](screenshots/kpis.jpeg)

### Correlation Matrix
![Correlations](screenshots/correlations.jpeg)

## Author

Salematou Youla — [LinkedIn](https://www.linkedin.com/in/salematou-youla-b7784790) | [GitHub](https://github.com/SalimaYoula)


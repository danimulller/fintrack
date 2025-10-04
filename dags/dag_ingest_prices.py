# dags/dag_ingest_prices.py
from __future__ import annotations
from datetime import datetime
from airflow import DAG
from airflow.decorators import task
from pathlib import Path
import sys

# garante que "src" está no PYTHONPATH
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from src.ingestion.yfinance_loader import fetch_ticker_data
from src.ingestion.utils import save_df_parquet_market_prices

DEFAULT_ARGS = {
    "owner": "daniel",
    "retries": 0,
}

with DAG(
    dag_id="ingest_market_prices_raw",
    description="Ingestão RAW de preços via yfinance",
    start_date=datetime(2025, 10, 1),
    schedule_interval="@daily",
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["raw", "market_prices"],
) as dag:

    TICKERS = ["VOO", "VT", "VB", "IBIT"]  # pode vir de variável/conn

    @task
    def ingest_ticker(ticker: str) -> str:
        print(f"Ingesting {ticker}..")

        df = fetch_ticker_data(ticker)

        path = save_df_parquet_market_prices(df, source="yfinance", asset=ticker)

        print(f"{ticker} ingested! Saved to {path}")

        return path  # útil para debugging/auditoria

    ingest_ticker.expand(ticker=TICKERS)

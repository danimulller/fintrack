# src/ingestion/utils.py
from datetime import date
from pathlib import Path
import pandas as pd
import os

def save_df_parquet_market_prices(df: pd.DataFrame, source: str, asset: str):
    """ Salva DF em Parquet no layout RAW: raw/<domain>/source=.../asset=.../dt=YYYY-MM-DD/part-000.parquet """
    
    run_date = date.today().isoformat()

    p = Path("./minio_data") / "raw" / "market_prices" / f"source={source}" / f"asset={asset}" / f"dt={run_date}"

    # Cria o path se não existir
    if not p.exists():
        p.mkdir(parents=True, exist_ok=True)

    df.to_parquet(p / "part-000.parquet", index=False)

    return str(p / "part-000.parquet")

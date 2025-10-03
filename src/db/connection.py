# src/db/connection.py
import os
from functools import lru_cache
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from dotenv import load_dotenv

load_dotenv()

@lru_cache(maxsize=1)
def get_engine() -> Engine:
    user = os.getenv("POSTGRES_USER")
    pwd  = os.getenv("POSTGRES_PASSWORD")
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    db   = os.getenv("POSTGRES_DB")

    url = f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db}"
    # pool_pre_ping evita conexões mortas; future=True é default no SA 2.x
    return create_engine(url, pool_pre_ping=True)

# src/db/queries.py
from __future__ import annotations

import re
from typing import Optional, Mapping, Any, Tuple
import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import CursorResult
from .connection import get_engine


# ---------- helpers ----------
_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

def _split_schema_table(table_name: str) -> Tuple[Optional[str], str]:
    """
    Divide 'schema.table' em (schema, table). Se não houver schema, retorna (None, table).
    """
    parts = table_name.split(".", 1)
    if len(parts) == 2:
        schema, table = parts[0].strip('"'), parts[1].strip('"')
        return (schema or None), table
    
    return "public", table_name.strip('"')

def _validate_ident(name: str) -> None:
    """Valida identificadores (schema/tabela) para evitar SQL injection em DDL."""
    if not _NAME_RE.match(name):
        raise ValueError(f"Identificador inválido: {name!r}. Use apenas [A-Za-z_][A-Za-z0-9_]*.")


# ---------- 1) SELECT → DataFrame ----------
def run_query_df(sql: str, params: Optional[Mapping[str, Any]] = None) -> pd.DataFrame:
    """
    Executa SELECT e retorna um pandas.DataFrame.
    Ex.: run_query_df("SELECT * FROM dim_asset WHERE is_active=:act", {"act": True})
    """
    engine = get_engine()
    with engine.connect() as conn:
        df = pd.read_sql_query(sql=text(sql), con=conn, params=params or {})
    return df


# ---------- 2) DDL/DML → nº de linhas afetadas ----------
def execute_sql(sql: str, params: Optional[Mapping[str, Any]] = None) -> int:
    """
    Executa INSERT/UPDATE/DELETE/DDL e retorna o nº de linhas afetadas (quando aplicável).
    """
    engine = get_engine()
    with engine.begin() as conn:
        result: CursorResult = conn.execute(text(sql), params or {})
        return max(result.rowcount or 0, 0)


# ---------- 3) DataFrame → tabela (replace) ----------
def df_to_table(df: pd.DataFrame, table_name: str) -> int:
    """
    Envia um DataFrame para o Postgres criando/substituindo a tabela (schema opcional).
    Retorna o nº de linhas carregadas.

    - Se table_name = "stg.lookup_ativos", usa schema 'stg' e tabela 'lookup_ativos'.
    - Se table_name = "minha_tabela", cria no schema padrão do usuário.

    Observação: para grandes volumes, troque para um caminho com COPY se necessário.
    """
    if df is None or df.empty:
        return 0

    schema, table = _split_schema_table(table_name)
    if schema:
        _validate_ident(schema)
    _validate_ident(table)

    engine = get_engine()
    with engine.begin() as conn:
        df.to_sql(
            name=table,
            con=conn,
            schema=schema,
            if_exists="replace",   # substitui a tabela; para append, altere aqui
            index=False,
            method="multi",        # INSERT multi-rows (melhor throughput)
            chunksize=10_000,
        )
    return int(len(df))


# ---------- 4) DROP TABLE IF EXISTS ----------
def delete_table(table_name: str) -> None:
    """
    Exclui a tabela informada (DROP TABLE IF EXISTS), com validação de identificadores.
    Aceita 'schema.tabela' ou apenas 'tabela'.
    """
    schema, table = _split_schema_table(table_name)
    if schema:
        _validate_ident(schema)
    _validate_ident(table)

    fqtn = f'"{schema}"."{table}"' if schema else f'"{table}"'

    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {fqtn} CASCADE;"))

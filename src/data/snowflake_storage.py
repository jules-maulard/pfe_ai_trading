"""
Snowflake storage backend – drop-in replacement for DuckDbCsvStorage.

Requires env vars (or .env file):
    SNOWFLAKE_ACCOUNT   – e.g. xy12345.eu-west-1
    SNOWFLAKE_USER
    SNOWFLAKE_PASSWORD
    SNOWFLAKE_DATABASE  – default: PFE_TRADING
    SNOWFLAKE_SCHEMA    – default: PUBLIC
    SNOWFLAKE_WAREHOUSE – default: COMPUTE_WH
"""
from __future__ import annotations

import os
from typing import List, Optional

import pandas as pd
from dotenv import load_dotenv

# Load .env first, then .env.snowflake (overrides if both exist)
load_dotenv()
load_dotenv(".env.snowflake", override=True)

try:
    import snowflake.connector
    from snowflake.connector.pandas_tools import write_pandas
except ImportError:
    raise ImportError(
        "snowflake-connector-python is required. "
        "Install it with:  pip install snowflake-connector-python[pandas]"
    )

OHLCV_TABLE = "OHLCV"


class SnowflakeStorage:
    """Same public API as DuckDbCsvStorage, backed by Snowflake."""

    def __init__(
        self,
        account: str | None = None,
        user: str | None = None,
        password: str | None = None,
        database: str | None = None,
        schema: str | None = None,
        warehouse: str | None = None,
    ):
        self.account = account or os.getenv("SNOWFLAKE_ACCOUNT")
        self.user = user or os.getenv("SNOWFLAKE_USER")
        self.password = password or os.getenv("SNOWFLAKE_PASSWORD")
        self.database = database or os.getenv("SNOWFLAKE_DATABASE", "PFE_TRADING")
        self.schema = schema or os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC")
        self.warehouse = warehouse or os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")

        # Validate required credentials
        missing = []
        if not self.account:
            missing.append("SNOWFLAKE_ACCOUNT")
        if not self.user:
            missing.append("SNOWFLAKE_USER")
        if not self.password:
            missing.append("SNOWFLAKE_PASSWORD")
        if missing:
            raise ValueError(
                f"Missing Snowflake credentials: {', '.join(missing)}. "
                "Set them in your .env file or pass them as arguments."
            )

    # ── connection helper ────────────────────────────────────────────
    def _connect(self):
        return snowflake.connector.connect(
            account=self.account,
            user=self.user,
            password=self.password,
            database=self.database,
            schema=self.schema,
            warehouse=self.warehouse,
        )

    # ── DDL : create database, schema & table if needed ─────────────
    def ensure_table(self):
        with self._connect() as conn:
            cur = conn.cursor()
            cur.execute(f"CREATE DATABASE IF NOT EXISTS {self.database}")
            cur.execute(f"USE DATABASE {self.database}")
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {self.schema}")
            cur.execute(f"USE SCHEMA {self.schema}")
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {OHLCV_TABLE} (
                    symbol   VARCHAR(20),
                    date     DATE,
                    open     FLOAT,
                    high     FLOAT,
                    low      FLOAT,
                    close    FLOAT,
                    volume   BIGINT
                )
            """)

    # ── Write ────────────────────────────────────────────────────────
    def save_prices(self, df: pd.DataFrame) -> str:
        """Upload a DataFrame to the OHLCV table (append)."""
        self.ensure_table()

        # Snowflake write_pandas expects uppercase column names
        upload = df.copy()
        upload.columns = [c.upper() for c in upload.columns]

        # Convert datetime/timestamp to date strings for Snowflake DATE columns
        if "DATE" in upload.columns:
            upload["DATE"] = pd.to_datetime(upload["DATE"]).dt.strftime("%Y-%m-%d")

        with self._connect() as conn:
            success, _nchunks, nrows, _output = write_pandas(
                conn, upload, OHLCV_TABLE
            )
        return f"Snowflake: inserted {nrows} rows into {OHLCV_TABLE}"

    # ── Read ─────────────────────────────────────────────────────────
    def load_prices(
        self,
        symbols: Optional[List[str]] = None,
        start: Optional[str] = None,
        end: Optional[str] = None,
    ) -> pd.DataFrame:
        sql = f"SELECT * FROM {OHLCV_TABLE}"
        conditions = []
        if symbols:
            syms = ", ".join(f"'{s}'" for s in symbols)
            conditions.append(f"SYMBOL IN ({syms})")
        if start:
            conditions.append(f"DATE >= '{start}'")
        if end:
            conditions.append(f"DATE <= '{end}'")
        if conditions:
            sql += " WHERE " + " AND ".join(conditions)

        with self._connect() as conn:
            cur = conn.cursor().execute(sql)
            df = cur.fetch_pandas_all()

        # Lowercase column names to match DuckDB output
        df.columns = [c.lower() for c in df.columns]
        return df

    # ── Generic query ────────────────────────────────────────────────
    def query(self, sql: str) -> pd.DataFrame:
        with self._connect() as conn:
            cur = conn.cursor().execute(sql)
            # DML statements (DELETE, INSERT, UPDATE) don't return a fetchable result
            if cur.description is None:
                return pd.DataFrame()
            try:
                df = cur.fetch_pandas_all()
            except Exception:
                return pd.DataFrame()
        df.columns = [c.lower() for c in df.columns]
        return df

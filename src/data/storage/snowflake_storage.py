from __future__ import annotations

import os
from typing import List, Optional

import pandas as pd

from data.storage.base_storage import BaseStorage

SNOWFLAKE_OHLCV_TABLE = "ohlcv"
SNOWFLAKE_ASSET_TABLE = "asset"
SNOWFLAKE_DIVIDEND_TABLE = "dividend"


class SnowflakeStorage(BaseStorage):

    def __init__(
        self,
        account: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        warehouse: Optional[str] = None,
        database: Optional[str] = None,
        schema: Optional[str] = None,
    ):
        try:
            import snowflake.connector
            from snowflake.connector.pandas_tools import write_pandas
        except ImportError as exc:
            raise ImportError(
                "snowflake-connector-python is required for SnowflakeStorage. "
                "Install it with: pip install snowflake-connector-python"
            ) from exc

        try:
            from dotenv import load_dotenv
            load_dotenv()
        except ImportError:
            pass

        self._sf = snowflake.connector
        self._write_pandas = write_pandas

        self.account = account or os.environ["SNOWFLAKE_ACCOUNT"]
        self.user = user or os.environ["SNOWFLAKE_USER"]
        self.password = password or os.environ["SNOWFLAKE_PASSWORD"]
        self.database = database or os.environ.get("SNOWFLAKE_DATABASE", "PFE_TRADING")
        self.schema = schema or os.environ.get("SNOWFLAKE_SCHEMA", "PUBLIC")
        self.warehouse = warehouse or os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")

    def _connect(self):
        return self._sf.connect(
            account=self.account,
            user=self.user,
            password=self.password,
            warehouse=self.warehouse,
            database=self.database,
            schema=self.schema,
        )

    def _write(self, df: pd.DataFrame, table: str) -> str:
        conn = self._connect()
        try:
            if "date" in df.columns:
                df["date"] = pd.to_datetime(df["date"]).dt.date
            df.columns = [c.upper() for c in df.columns]
            self._write_pandas(conn, df, table.upper(), auto_create_table=True, overwrite=False)
        finally:
            conn.close()
        return f"{self.database}.{self.schema}.{table.upper()}"

    def _read(
        self,
        table: str,
        symbols: Optional[List[str]] = None,
        start: Optional[str] = None,
        end: Optional[str] = None,
    ) -> pd.DataFrame:
        sql = f"SELECT * FROM {table.upper()}"
        conditions = []
        if symbols:
            syms = ", ".join(f"'{s}'" for s in symbols)
            conditions.append(f"symbol IN ({syms})")
        if start:
            conditions.append(f"date >= '{start}'")
        if end:
            conditions.append(f"date <= '{end}'")
        if conditions:
            sql += " WHERE " + " AND ".join(conditions)

        conn = self._connect()
        try:
            cursor = conn.cursor()
            cursor.execute(sql)
            df = cursor.fetch_pandas_all()
            df.columns = [c.lower() for c in df.columns]
            return df
        finally:
            conn.close()

    def _upsert(self, df: pd.DataFrame, table: str) -> str:
        symbols = list(df["symbol"].unique())
        syms_sql = ", ".join(f"'{s}'" for s in symbols)
        conn = self._connect()
        try:
            cursor = conn.cursor()
            cursor.execute(f"DELETE FROM {table.upper()} WHERE symbol IN ({syms_sql})")
        finally:
            conn.close()
        return self._write(df, table)

    def save_ohlcv(self, df: pd.DataFrame) -> str:
        return self._upsert(df, SNOWFLAKE_OHLCV_TABLE)

    def append_ohlcv(self, df: pd.DataFrame) -> str:
        return self._write(df, SNOWFLAKE_OHLCV_TABLE)

    def upsert_ohlcv(self, df: pd.DataFrame) -> str:
        return self._upsert(df, SNOWFLAKE_OHLCV_TABLE)

    def load_ohlcv(
        self,
        symbols: Optional[List[str]] = None,
        start: Optional[str] = None,
        end: Optional[str] = None,
    ) -> pd.DataFrame:
        return self._read(SNOWFLAKE_OHLCV_TABLE, symbols=symbols, start=start, end=end)

    def save_asset(self, df: pd.DataFrame) -> str:
        return self._upsert(df, SNOWFLAKE_ASSET_TABLE)

    def load_asset(self, symbols: Optional[List[str]] = None) -> pd.DataFrame:
        return self._read(SNOWFLAKE_ASSET_TABLE, symbols=symbols)

    def save_dividend(self, df: pd.DataFrame) -> str:
        return self._upsert(df, SNOWFLAKE_DIVIDEND_TABLE)

    def append_dividend(self, df: pd.DataFrame) -> str:
        return self._write(df, SNOWFLAKE_DIVIDEND_TABLE)

    def upsert_dividend(self, df: pd.DataFrame) -> str:
        return self._upsert(df, SNOWFLAKE_DIVIDEND_TABLE)

    def load_dividend(
        self,
        symbols: Optional[List[str]] = None,
        start: Optional[str] = None,
        end: Optional[str] = None,
    ) -> pd.DataFrame:
        return self._read(SNOWFLAKE_DIVIDEND_TABLE, symbols=symbols, start=start, end=end)

    def get_last_date(self, table: str, symbol: str) -> Optional[str]:
        sql = (
            f"SELECT MAX(date) AS last_date FROM {table.upper()} "
            f"WHERE symbol = '{symbol}'"
        )
        conn = self._connect()
        try:
            cursor = conn.cursor()
            cursor.execute(sql)
            row = cursor.fetchone()
            if row is None or row[0] is None:
                return None
            return str(row[0])
        finally:
            conn.close()

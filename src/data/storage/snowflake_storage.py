from __future__ import annotations

import tempfile
import uuid
from pathlib import Path
from typing import List, Optional

import pandas as pd

from .base_storage import BaseStorage
from ...utils import get_logger

logger = get_logger(__name__)

SNOWFLAKE_OHLCV_TABLE = "OHLCV"
SNOWFLAKE_ASSET_TABLE = "ASSET"
SNOWFLAKE_DIVIDEND_TABLE = "DIVIDEND"
SNOWFLAKE_INDICATORS_TABLE = "INDICATORS"

SNOWFLAKE_STAGE = "TRADING_STAGE"
SNOWFLAKE_PARQUET_FORMAT = "PARQUET_FORMAT"
SNOWFLAKE_INDICATORS_TASK = "TASK_COMPUTE_INDICATORS"


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
        except ImportError as exc:
            raise ImportError(
                "snowflake-connector-python is required for SnowflakeStorage. "
                "Install it with: pip install snowflake-connector-python"
            ) from exc

        from ..config.settings import get_config
        cfg = get_config()

        self._sf = snowflake.connector

        self.account = account or cfg.snowflake_account
        self.user = user or cfg.snowflake_user
        self.password = password or cfg.snowflake_password
        self.database = database or cfg.snowflake_database
        self.schema = schema or cfg.snowflake_schema
        self.warehouse = warehouse or cfg.snowflake_warehouse

    def _connect(self):
        return self._sf.connect(
            account=self.account,
            user=self.user,
            password=self.password,
            warehouse=self.warehouse,
            database=self.database,
            schema=self.schema,
        )

    def _stage_and_copy(self, df: pd.DataFrame, table: str) -> str:
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"]).dt.date
        df.columns = [c.upper() for c in df.columns]

        fqn = f"{self.database}.{self.schema}.{table}"
        tmp_dir = tempfile.mkdtemp()
        parquet_name = f"{table}_{uuid.uuid4().hex}.parquet"
        parquet_path = Path(tmp_dir) / parquet_name
        df.to_parquet(parquet_path, index=False, engine="pyarrow")

        conn = self._connect()
        try:
            cursor = conn.cursor()
            put_sql = (
                f"PUT 'file://{parquet_path.as_posix()}' @{SNOWFLAKE_STAGE}/{table}/ "
                f"AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
            )
            cursor.execute(put_sql)
            logger.info("PUT %s -> @%s/%s/", parquet_path.name, SNOWFLAKE_STAGE, table)

            cols = ", ".join(df.columns)
            select_cols = ", ".join(
                f"$1:{c}" for c in df.columns
            )
            copy_sql = (
                f"COPY INTO {fqn} ({cols}) "
                f"FROM (SELECT {select_cols} FROM @{SNOWFLAKE_STAGE}/{table}/) "
                f"FILE_FORMAT = (FORMAT_NAME = '{SNOWFLAKE_PARQUET_FORMAT}') "
                f"MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE "
                f"PURGE = TRUE"
            )
            cursor.execute(copy_sql)
            logger.info("COPY INTO %s completed", fqn)
        finally:
            conn.close()
            parquet_path.unlink(missing_ok=True)
        return fqn

    def _read(
        self,
        table: str,
        symbols: Optional[List[str]] = None,
        start: Optional[str] = None,
        end: Optional[str] = None,
    ) -> pd.DataFrame:
        sql = f"SELECT * FROM {table}"
        conditions: list[str] = []
        params: list[str] = []
        if symbols:
            placeholders = ", ".join(["%s"] * len(symbols))
            conditions.append(f"symbol IN ({placeholders})")
            params.extend(symbols)
        if start:
            conditions.append("date >= %s")
            params.append(start)
        if end:
            conditions.append("date <= %s")
            params.append(end)
        if conditions:
            sql += " WHERE " + " AND ".join(conditions)

        conn = self._connect()
        try:
            cursor = conn.cursor()
            cursor.execute(sql, params)
            df = cursor.fetch_pandas_all()
            df.columns = [c.lower() for c in df.columns]
            return df
        finally:
            conn.close()

    def _upsert(self, df: pd.DataFrame, table: str) -> str:
        symbols = list(df["symbol"].unique())
        placeholders = ", ".join(["%s"] * len(symbols))
        conn = self._connect()
        try:
            cursor = conn.cursor()
            cursor.execute(
                f"DELETE FROM {table} WHERE symbol IN ({placeholders})",
                symbols,
            )
        finally:
            conn.close()
        return self._stage_and_copy(df, table)

    def save_ohlcv(self, df: pd.DataFrame) -> str:
        return self._upsert(df, SNOWFLAKE_OHLCV_TABLE)

    def load_ohlcv(
        self,
        symbols: Optional[List[str]] = None,
        start: Optional[str] = None,
        end: Optional[str] = None,
    ) -> pd.DataFrame:
        return self._read(SNOWFLAKE_OHLCV_TABLE, symbols=symbols, start=start, end=end)

    def save_dividend(self, df: pd.DataFrame) -> str:
        return self._upsert(df, SNOWFLAKE_DIVIDEND_TABLE)

    def load_dividend(
        self,
        symbols: Optional[List[str]] = None,
        start: Optional[str] = None,
        end: Optional[str] = None,
    ) -> pd.DataFrame:
        return self._read(SNOWFLAKE_DIVIDEND_TABLE, symbols=symbols, start=start, end=end)

    def save_asset(self, df: pd.DataFrame) -> str:
        return self._upsert(df, SNOWFLAKE_ASSET_TABLE)

    def load_asset(self, symbols: Optional[List[str]] = None) -> pd.DataFrame:
        return self._read(SNOWFLAKE_ASSET_TABLE, symbols=symbols)

    def save_indicators(self, df: pd.DataFrame) -> str:
        return self._upsert(df, SNOWFLAKE_INDICATORS_TABLE)

    def load_indicators(
        self,
        symbols: Optional[List[str]] = None,
        start: Optional[str] = None,
        end: Optional[str] = None,
    ) -> pd.DataFrame:
        return self._read(SNOWFLAKE_INDICATORS_TABLE, symbols=symbols, start=start, end=end)

    def get_last_date(self, table: str, symbol: str) -> Optional[str]:
        sql = f"SELECT MAX(date) AS last_date FROM {table} WHERE symbol = %s"
        conn = self._connect()
        try:
            cursor = conn.cursor()
            cursor.execute(sql, (symbol,))
            row = cursor.fetchone()
            if row is None or row[0] is None:
                return None
            return str(row[0])
        finally:
            conn.close()

    def update_indicators(
        self,
        symbols: Optional[List[str]] = None,
        start: Optional[str] = None,
        end: Optional[str] = None,
    ) -> str:
        conn = self._connect()
        try:
            cursor = conn.cursor()
            cursor.execute(f"EXECUTE TASK {SNOWFLAKE_INDICATORS_TASK}")
            logger.info("Launched Snowflake task %s", SNOWFLAKE_INDICATORS_TASK)
        finally:
            conn.close()
        return SNOWFLAKE_INDICATORS_TASK

from __future__ import annotations

from typing import List, Optional
import pandas as pd

from .base_storage import BaseStorage
from ...utils import get_logger

logger = get_logger(__name__)

SNOWFLAKE_OHLCV_TABLE = "ohlcv"
SNOWFLAKE_ASSET_TABLE = "asset"
SNOWFLAKE_DIVIDEND_TABLE = "dividend"
SNOWFLAKE_INDICATORS_TABLE = "indicators"

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
            from snowflake.connector.pandas_tools import write_pandas
        except ImportError as exc:
            raise ImportError(
                "snowflake-connector-python is required for SnowflakeStorage. "
                "Install it with: pip install snowflake-connector-python"
            ) from exc

        from ..config.settings import get_config
        cfg = get_config()

        self._sf = snowflake.connector
        self._write_pandas = write_pandas

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

from __future__ import annotations

import tempfile
import uuid
from pathlib import Path
from typing import List, Optional
import pandas as pd
import snowflake.connector

from .base_storage import BaseStorage
from ..config.settings import get_config
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
        role: Optional[str] = None,
        warehouse: Optional[str] = None,
        database: Optional[str] = None,
        schema: Optional[str] = None,
    ):
        config = get_config()
        self.account = account or config.snowflake_account
        self.user = user or config.snowflake_user
        self.password = password or config.snowflake_password
        self.role = role or config.snowflake_role
        self.database = database or config.snowflake_database
        self.schema = schema or config.snowflake_schema
        self.warehouse = warehouse or config.snowflake_warehouse

    def save_ohlcv(self, df: pd.DataFrame) -> str:
        return self._upsert_data(df, SNOWFLAKE_OHLCV_TABLE)

    def load_ohlcv(
        self,
        symbols: Optional[List[str]] = None,
        start: Optional[str] = None,
        end: Optional[str] = None,
    ) -> pd.DataFrame:
        return self._read_data(SNOWFLAKE_OHLCV_TABLE, symbols=symbols, start=start, end=end)

    def save_dividend(self, df: pd.DataFrame) -> str:
        return self._upsert_data(df, SNOWFLAKE_DIVIDEND_TABLE)

    def load_dividend(
        self,
        symbols: Optional[List[str]] = None,
        start: Optional[str] = None,
        end: Optional[str] = None,
    ) -> pd.DataFrame:
        return self._read_data(SNOWFLAKE_DIVIDEND_TABLE, symbols=symbols, start=start, end=end)

    def save_asset(self, df: pd.DataFrame) -> str:
        return self._upsert_data(df, SNOWFLAKE_ASSET_TABLE)

    def load_asset(self, symbols: Optional[List[str]] = None) -> pd.DataFrame:
        return self._read_data(SNOWFLAKE_ASSET_TABLE, symbols=symbols)

    def save_indicators(self, df: pd.DataFrame) -> str:
        return self._upsert_data(df, SNOWFLAKE_INDICATORS_TABLE)

    def load_indicators(
        self,
        symbols: Optional[List[str]] = None,
        start: Optional[str] = None,
        end: Optional[str] = None,
    ) -> pd.DataFrame:
        return self._read_data(SNOWFLAKE_INDICATORS_TABLE, symbols=symbols, start=start, end=end)

    def get_last_date(self, table: str, symbol: str) -> Optional[str]:
        latest_dates = self._get_latest_dates(table, [symbol])
        return latest_dates.get(symbol)

    def update_indicators(
        self,
        symbols: Optional[List[str]] = None,
        start: Optional[str] = None,
        end: Optional[str] = None,
    ) -> str:
        query = f"EXECUTE TASK {SNOWFLAKE_INDICATORS_TASK}"
        with self._connect() as connection, connection.cursor() as cursor:
            cursor.execute(query)
        return SNOWFLAKE_INDICATORS_TASK

    def _connect(self) -> snowflake.connector.SnowflakeConnection:
        connection_parameters = dict(
            account=self.account,
            user=self.user,
            password=self.password,
            warehouse=self.warehouse,
            database=self.database,
            schema=self.schema,
        )
        if self.role:
            connection_parameters["role"] = self.role
            
        return snowflake.connector.connect(**connection_parameters)

    def _read_data(
        self,
        table: str,
        symbols: Optional[List[str]] = None,
        start: Optional[str] = None,
        end: Optional[str] = None,
    ) -> pd.DataFrame:
        query = f"SELECT * FROM {table}"
        conditions: list[str] = []
        parameters: list[str] = []

        if symbols:
            placeholders = ", ".join(["%s"] * len(symbols))
            conditions.append(f"symbol IN ({placeholders})")
            parameters.extend(symbols)
        if start:
            conditions.append("date >= %s")
            parameters.append(start)
        if end:
            conditions.append("date <= %s")
            parameters.append(end)

        if conditions:
            query += " WHERE " + " AND ".join(conditions)

        with self._connect() as connection, connection.cursor() as cursor:
            cursor.execute(query, parameters)
            result_data = cursor.fetch_pandas_all()

        result_data.columns = [column_name.lower() for column_name in result_data.columns]
        return result_data

    def _get_latest_dates(self, table: str, symbols: List[str]) -> dict[str, str]:
        if not symbols:
            return {}

        placeholders = ", ".join(["%s"] * len(symbols))
        query = f"SELECT symbol, MAX(date) FROM {table} WHERE symbol IN ({placeholders}) GROUP BY symbol"
        
        with self._connect() as connection, connection.cursor() as cursor:
            cursor.execute(query, symbols)
            results = cursor.fetchall()
            
        return {row[0]: str(row[1]) for row in results if row[1] is not None}

    def _upsert_data(self, data: pd.DataFrame, table: str) -> str:
        if data.empty:
            return f"{self.database}.{self.schema}.{table}"

        working_data = data.copy()
        working_data["date"] = pd.to_datetime(working_data["date"]).dt.date
        unique_symbols = working_data["symbol"].unique().tolist()
        
        latest_dates = self._get_latest_dates(table, unique_symbols)

        if latest_dates:
            working_data["last_date"] = working_data["symbol"].map(latest_dates)
            working_data["last_date"] = pd.to_datetime(working_data["last_date"]).dt.date
            
            is_new_symbol = working_data["last_date"].isna()
            is_recent_date = working_data["date"] > working_data["last_date"]
            
            working_data = working_data[is_new_symbol | is_recent_date].drop(columns=["last_date"])

        if working_data.empty:
            return f"{self.database}.{self.schema}.{table}"

        return self._execute_load(working_data, table)

    def _execute_load(self, data: pd.DataFrame, table: str) -> str:
        working_data = self._prepare_dataframe_for_snowflake(data)
        with tempfile.TemporaryDirectory() as temporary_directory:
            file_path = self._serialize_to_parquet(working_data, table, temporary_directory)
            self._put_file_to_stage(file_path, table)
            return self._copy_into_table(working_data.columns.tolist(), table)

    def _prepare_dataframe_for_snowflake(self, data: pd.DataFrame) -> pd.DataFrame:
        working_data = data.copy()
        if "date" in working_data.columns:
            working_data["date"] = pd.to_datetime(working_data["date"]).dt.date
        working_data.columns = [column_name.upper() for column_name in working_data.columns]
        return working_data

    def _serialize_to_parquet(self, data: pd.DataFrame, table: str, directory: str) -> Path:
        file_name = f"{table}_{uuid.uuid4().hex}.parquet"
        file_path = Path(directory) / file_name
        data.to_parquet(file_path, index=False, engine="pyarrow")
        return file_path

    def _put_file_to_stage(self, file_path: Path, table: str) -> None:
        put_query = (
            f"PUT 'file://{file_path.as_posix()}' @{SNOWFLAKE_STAGE}/{table}/ "
            "AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
        )
        with self._connect() as connection, connection.cursor() as cursor:
            cursor.execute(put_query)

    def _copy_into_table(self, columns: List[str], table: str) -> str:
        fully_qualified_name = f"{self.database}.{self.schema}.{table}"
        columns_list = ", ".join(columns)
        select_list = ", ".join(f"$1:{column_name}" for column_name in columns)

        copy_query = (
            f"COPY INTO {fully_qualified_name} ({columns_list}) "
            f"FROM (SELECT {select_list} FROM @{SNOWFLAKE_STAGE}/{table}/) "
            f"FILE_FORMAT = (FORMAT_NAME = '{SNOWFLAKE_PARQUET_FORMAT}') "
            "PURGE = TRUE"
        )
        with self._connect() as connection, connection.cursor() as cursor:
            cursor.execute(copy_query)

        return fully_qualified_name
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

SNOWFLAKE_INDICATOR_TABLES: dict[str, str] = {
    "rsi": "INDICATOR_RSI",
    "macd": "INDICATOR_MACD",
    "pivot": "INDICATOR_PIVOT",
}

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

    def save_ohlcv(self, df: pd.DataFrame, force_insert: bool = False) -> str:
        return self._upsert_data(df, SNOWFLAKE_OHLCV_TABLE, force_insert=force_insert)

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

    def save_indicator(self, df: pd.DataFrame, indicator_name: str) -> str:
        table = SNOWFLAKE_INDICATOR_TABLES[indicator_name]
        return self._upsert_data(df, table)

    def load_indicator(
        self,
        indicator_name: str,
        symbols: Optional[List[str]] = None,
        start: Optional[str] = None,
        end: Optional[str] = None,
    ) -> pd.DataFrame:
        table = SNOWFLAKE_INDICATOR_TABLES[indicator_name]
        return self._read_data(table, symbols=symbols, start=start, end=end)

    def list_symbols(self, table: str = "ohlcv") -> List[str]:
        table_name = table.upper()
        query = f"SELECT DISTINCT symbol FROM {table_name} ORDER BY symbol"
        with self._connect() as connection, connection.cursor() as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()
        return [row[0] for row in rows]

    def get_last_date(self, table: str, symbol: str) -> Optional[str]:
        latest_dates = self._get_latest_dates(table, [symbol])
        return latest_dates.get(symbol)

    def get_last_dates(self, table: str, symbols: List[str]) -> dict:
        return self._get_latest_dates(table, symbols)

    def get_last_indicator_dates(self, indicator_name: str, symbols: List[str]) -> dict:
        table = SNOWFLAKE_INDICATOR_TABLES[indicator_name]
        return self._get_latest_dates(table, symbols)

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

    def _get_existing_date_pairs(self, table: str, symbols: List[str], min_date, max_date) -> set:
        if not symbols:
            return set()
        placeholders = ", ".join(["%s"] * len(symbols))
        query = (
            f"SELECT symbol, date FROM {table} "
            f"WHERE symbol IN ({placeholders}) AND date >= %s AND date <= %s"
        )
        with self._connect() as connection, connection.cursor() as cursor:
            cursor.execute(query, symbols + [min_date, max_date])
            rows = cursor.fetchall()
        return {(row[0], row[1]) for row in rows}

    def _get_latest_dates(self, table: str, symbols: List[str]) -> dict[str, str]:
        if not symbols:
            return {}

        placeholders = ", ".join(["%s"] * len(symbols))
        query = f"SELECT symbol, MAX(date) FROM {table} WHERE symbol IN ({placeholders}) GROUP BY symbol"
        
        with self._connect() as connection, connection.cursor() as cursor:
            cursor.execute(query, symbols)
            results = cursor.fetchall()
            
        return {row[0]: str(row[1]) for row in results if row[1] is not None}

    def _upsert_data(self, data: pd.DataFrame, table: str, force_insert: bool = False) -> str:
        if data.empty:
            logger.info("[UPSERT] No data to upsert into '%s' (empty DataFrame).", table)
            return f"{self.database}.{self.schema}.{table}"

        working_data = data.copy()
        has_date = "date" in working_data.columns
        if has_date:
            working_data["date"] = pd.to_datetime(working_data["date"]).dt.date
        unique_symbols = working_data["symbol"].unique().tolist()
        logger.info(
            "[UPSERT] Starting upsert into '%s' — %d row(s), %d symbol(s): %s",
            table, len(working_data), len(unique_symbols), unique_symbols,
        )

        if has_date:
            if force_insert:
                min_date = working_data["date"].min()
                max_date = working_data["date"].max()
                existing_pairs = self._get_existing_date_pairs(table, unique_symbols, min_date, max_date)
                if existing_pairs:
                    mask = pd.Series(
                        [
                            (sym, dt) not in existing_pairs
                            for sym, dt in zip(working_data["symbol"], working_data["date"])
                        ],
                        index=working_data.index,
                    )
                    working_data = working_data[mask]
                    logger.info("[UPSERT] %d new row(s) to insert after excluding existing (symbol, date) pairs.", len(working_data))
                else:
                    logger.info("[UPSERT] No existing data found in '%s' for these symbols — full insert.", table)
            else:
                latest_dates = self._get_latest_dates(table, unique_symbols)
                if latest_dates:
                    logger.info("[UPSERT] Latest dates in '%s': %s", table, latest_dates)
                else:
                    logger.info("[UPSERT] No existing data found in '%s' for these symbols — full insert.", table)

                if latest_dates:
                    working_data["last_date"] = working_data["symbol"].map(latest_dates)
                    working_data["last_date"] = pd.to_datetime(working_data["last_date"]).dt.date

                    is_new_symbol = working_data["last_date"].isna()
                    is_recent_date = working_data["date"] > working_data["last_date"]

                    working_data = working_data[is_new_symbol | is_recent_date].drop(columns=["last_date"])
                    logger.info("[UPSERT] %d new row(s) to insert after filtering duplicates.", len(working_data))
        else:
            logger.info("[UPSERT] No date column in '%s' — inserting all rows (symbol-keyed table).", table)

        if working_data.empty:
            logger.info("[UPSERT] All rows already present in '%s'. Nothing to insert.", table)
            return f"{self.database}.{self.schema}.{table}"

        return self.upload_dataframe_to_snowflake(working_data, table)

    def upload_dataframe_to_snowflake(self, data: pd.DataFrame, table: str) -> str:
        working_data = self.normalize_dataframe_for_snowflake(data)
        with tempfile.TemporaryDirectory() as temporary_directory:
            file_path = self.save_dataframe_as_parquet(working_data, table, temporary_directory)
            self.upload_file_to_stage(file_path, table)
            return self.load_stage_into_table(working_data.columns.tolist(), table)

    def normalize_dataframe_for_snowflake(self, data: pd.DataFrame) -> pd.DataFrame:
        working_data = data.copy()
        if "date" in working_data.columns:
            working_data["date"] = pd.to_datetime(working_data["date"]).dt.date
        working_data.columns = [column_name.upper() for column_name in working_data.columns]
        return working_data

    def save_dataframe_as_parquet(self, data: pd.DataFrame, table: str, directory: str) -> Path:
        file_name = f"{table}_{uuid.uuid4().hex}.parquet"
        file_path = Path(directory) / file_name
        data.to_parquet(file_path, index=False, engine="pyarrow")
        return file_path

    def upload_file_to_stage(self, file_path: Path, table: str) -> None:
        put_query = (
            f"PUT 'file://{file_path.as_posix()}' @{SNOWFLAKE_STAGE}/{table}/ "
            "AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
        )
        logger.info("[PUT] Uploading '%s' → stage @%s/%s/", file_path.name, SNOWFLAKE_STAGE, table)
        with self._connect() as connection, connection.cursor() as cursor:
            cursor.execute(put_query)
            rows = cursor.fetchall()
            for row in rows:
                logger.debug("[PUT] %s", row)
        logger.info("[PUT] Upload complete for table '%s'.", table)

    def load_stage_into_table(self, columns: List[str], table: str) -> str:
        fully_qualified_name = f"{self.database}.{self.schema}.{table}"
        columns_list = ", ".join(columns)
        select_list = ", ".join(f"$1:{column_name}" for column_name in columns)

        copy_query = (
            f"COPY INTO {fully_qualified_name} ({columns_list}) "
            f"FROM (SELECT {select_list} FROM @{SNOWFLAKE_STAGE}/{table}/) "
            f"FILE_FORMAT = (FORMAT_NAME = '{SNOWFLAKE_PARQUET_FORMAT}') "
            "PURGE = TRUE"
        )
        logger.info(
            "[COPY INTO] Loading stage @%s/%s/ → %s (%d column(s): %s)",
            SNOWFLAKE_STAGE, table, fully_qualified_name, len(columns), columns_list,
        )
        with self._connect() as connection, connection.cursor() as cursor:
            cursor.execute(copy_query)
            rows = cursor.fetchall()
            for row in rows:
                logger.debug("[COPY INTO] %s", row)
        logger.info("[COPY INTO] Done for table '%s'.", table)

        return fully_qualified_name
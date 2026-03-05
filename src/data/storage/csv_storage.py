from __future__ import annotations

from pathlib import Path
from typing import List, Optional

import duckdb
import pandas as pd

from data.storage.base_storage import BaseStorage
from utils.logger import get_logger

logger = get_logger(__name__)

DEFAULT_BASE_DIR = "database/csv"

_TABLE_COLUMNS: dict[str, list[str]] = {
    "ohlcv": ["symbol", "date", "open", "high", "low", "close", "volume"],
    "dividend": ["symbol", "date", "amount"],
    "asset": ["symbol"],
}


class CsvStorage(BaseStorage):

    def __init__(self, base_dir: str = DEFAULT_BASE_DIR):
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(parents=True, exist_ok=True)
        for table, cols in _TABLE_COLUMNS.items():
            path = self._path(table)
            if not path.exists():
                pd.DataFrame(columns=cols).to_csv(path, index=False)

    def _path(self, table: str) -> Path:
        return self.base_dir / f"{table}.csv"

    def _save(self, df: pd.DataFrame, table: str, sort_cols: List[str]) -> str:
        path = self._path(table)
        logger.debug("Saving %d rows to %s", len(df), path)
        df.to_csv(path, index=False)
        return str(path)

    def _load(
        self,
        table: str,
        symbols: Optional[List[str]] = None,
        start: Optional[str] = None,
        end: Optional[str] = None,
    ) -> pd.DataFrame:
        path = self._path(table)
        if not path.exists():
            raise FileNotFoundError(f"Data file not found: {path}")

        logger.debug("Loading %s (symbols=%s, start=%s, end=%s)", table, symbols, start, end)

        source = f"'{path.as_posix()}'"
        sql = f"SELECT * FROM read_csv_auto({source})"

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

        return duckdb.sql(sql).df()

    def _append(self, df: pd.DataFrame, table: str, sort_cols: List[str]) -> str:
        path = self._path(table)
        df.to_csv(path, mode="a", header=False, index=False)
        return str(path)

    def _upsert(self, df: pd.DataFrame, table: str, sort_cols: List[str]) -> str:
        path = self._path(table)
        symbols = list(df["symbol"].unique())
        if path.exists():
            syms_sql = ", ".join(f"'{s}'" for s in symbols)
            source = f"'{path.as_posix()}'"
            others = duckdb.sql(
                f"SELECT * FROM read_csv_auto({source}) WHERE symbol NOT IN ({syms_sql})"
            ).df()
            combined = pd.concat([others, df], ignore_index=True)
        else:
            combined = df
        return self._save(combined, table, sort_cols)

    def save_ohlcv(self, df: pd.DataFrame) -> str:
        return self._upsert(df, "ohlcv", ["symbol", "date"])

    def append_ohlcv(self, df: pd.DataFrame) -> str:
        return self._append(df, "ohlcv", ["symbol", "date"])

    def upsert_ohlcv(self, df: pd.DataFrame) -> str:
        return self._upsert(df, "ohlcv", ["symbol", "date"])

    def load_ohlcv(
        self,
        symbols: Optional[List[str]] = None,
        start: Optional[str] = None,
        end: Optional[str] = None,
    ) -> pd.DataFrame:
        return self._load("ohlcv", symbols=symbols, start=start, end=end)

    def save_asset(self, df: pd.DataFrame) -> str:
        return self._upsert(df, "asset", ["symbol"])

    def load_asset(self, symbols: Optional[List[str]] = None) -> pd.DataFrame:
        return self._load("asset", symbols=symbols)

    def save_dividend(self, df: pd.DataFrame) -> str:
        return self._upsert(df, "dividend", ["symbol", "date"])

    def append_dividend(self, df: pd.DataFrame) -> str:
        return self._append(df, "dividend", ["symbol", "date"])

    def upsert_dividend(self, df: pd.DataFrame) -> str:
        return self._upsert(df, "dividend", ["symbol", "date"])

    def load_dividend(
        self,
        symbols: Optional[List[str]] = None,
        start: Optional[str] = None,
        end: Optional[str] = None,
    ) -> pd.DataFrame:
        return self._load("dividend", symbols=symbols, start=start, end=end)

    def get_last_date(self, table: str, symbol: str) -> Optional[str]:
        path = self._path(table)
        if not path.exists():
            return None
        source = f"'{path.as_posix()}'"
        sql = (
            f"SELECT MAX(date) AS last_date FROM read_csv_auto({source}) "
            f"WHERE symbol = '{symbol}'"
        )
        result = duckdb.sql(sql).df()
        val = result["last_date"].iloc[0]
        if pd.isna(val):
            return None
        return str(val)

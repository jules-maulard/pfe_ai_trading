from __future__ import annotations

from pathlib import Path
from typing import List, Optional

import duckdb
import pandas as pd

from data.storage.base_storage import BaseStorage

DEFAULT_BASE_DIR = "database/parquet"


class ParquetStorage(BaseStorage):

    def __init__(self, base_dir: str = DEFAULT_BASE_DIR):
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self.tables = ["ohlcv", "asset", "dividend"]
        for table in self.tables:
            path = self._path(table)
            if not path.exists():
                if table == "ohlcv":
                    columns = ["symbol", "date", "open", "high", "low", "close", "volume"]
                elif table == "asset":
                    columns = [
                        "symbol", "company_name", "sector", "industry", "currency",
                        "country", "exchange", "long_business_summary", "website"
                    ]
                elif table == "dividend":
                    columns = ["symbol", "date", "amount"]
                else:
                    columns = []
                df = pd.DataFrame(columns=columns)
                df.to_parquet(path, index=False, engine="pyarrow")

    def _path(self, table: str) -> Path:
        return self.base_dir / f"{table}.parquet"

    def _save(self, df: pd.DataFrame, table: str, sort_cols: List[str]) -> str:
        path = self._path(table)
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"], errors="coerce")
            if hasattr(df["date"].dt, "tz") and df["date"].dt.tz is not None:
                df["date"] = df["date"].dt.tz_convert(None)
            df["date"] = df["date"].dt.tz_localize(None)
        df.sort_values(sort_cols).to_parquet(path, index=False, engine="pyarrow")
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

        source = f"'{path.as_posix()}'"
        sql = f"SELECT * FROM read_parquet({source})"

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

    def _upsert(self, df: pd.DataFrame, table: str, sort_cols: List[str]) -> str:
        path = self._path(table)
        symbols = list(df["symbol"].unique())
        if path.exists():
            syms_sql = ", ".join(f"'{s}'" for s in symbols)
            source = f"'{path.as_posix()}'"
            others = duckdb.sql(
                f"SELECT * FROM read_parquet({source}) WHERE symbol NOT IN ({syms_sql})"
            ).df()
            combined = pd.concat([others, df], ignore_index=True)
        else:
            combined = df
        return self._save(combined, table, sort_cols)

    def save_ohlcv(self, df: pd.DataFrame) -> str:
        return self._upsert(df, "ohlcv", ["symbol", "date"])

    def append_ohlcv(self, df: pd.DataFrame) -> str:
        return self._upsert(df, "ohlcv", ["symbol", "date"])

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
        return self._upsert(df, "dividend", ["symbol", "date"])

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
            f"SELECT MAX(date) AS last_date FROM read_parquet({source}) "
            f"WHERE symbol = '{symbol}'"
        )
        result = duckdb.sql(sql).df()
        val = result["last_date"].iloc[0]
        if pd.isna(val):
            return None
        return str(val)

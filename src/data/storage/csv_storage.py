from __future__ import annotations

from pathlib import Path
from typing import List, Optional
import duckdb
import pandas as pd

from .base_storage import BaseStorage
from ...utils import get_logger

logger = get_logger(__name__)

DEFAULT_BASE_DIR = "database/csv"

_TABLE_COLUMNS: dict[str, list[str]] = {
    "ohlcv": ["symbol", "date", "open", "high", "low", "close", "volume"],
    "dividend": ["symbol", "date", "amount"],
    "asset": ["symbol"],
    "indicators": ["symbol", "date", "rsi", "macd", "macd_signal", "macd_hist"],
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

    def _save(self, df: pd.DataFrame, table: str) -> str:
        path = self._path(table)
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

    def _upsert(self, df: pd.DataFrame, table: str) -> str:
        path = self._path(table)
        if path.exists():
            source = f"'{path.as_posix()}'"
            existing = duckdb.sql(f"SELECT * FROM read_csv_auto({source})").df()
            combined = pd.concat([existing, df], ignore_index=True)
            key_cols = [c for c in ["symbol", "date"] if c in combined.columns]
            if key_cols:
                combined = combined.drop_duplicates(subset=key_cols, keep="last")
        else:
            combined = df
        return self._save(combined, table)

    def save_ohlcv(self, df: pd.DataFrame) -> str:
        return self._upsert(df, "ohlcv")

    def load_ohlcv(
        self,
        symbols: Optional[List[str]] = None,
        start: Optional[str] = None,
        end: Optional[str] = None,
    ) -> pd.DataFrame:
        return self._load("ohlcv", symbols=symbols, start=start, end=end)

    def save_dividend(self, df: pd.DataFrame) -> str:
        return self._upsert(df, "dividend")

    def load_dividend(
        self,
        symbols: Optional[List[str]] = None,
        start: Optional[str] = None,
        end: Optional[str] = None,
    ) -> pd.DataFrame:
        return self._load("dividend", symbols=symbols, start=start, end=end)

    def save_asset(self, df: pd.DataFrame) -> str:
        return self._upsert(df, "asset")

    def load_asset(self, symbols: Optional[List[str]] = None) -> pd.DataFrame:
        return self._load("asset", symbols=symbols)

    def save_indicators(self, df: pd.DataFrame) -> str:
        return self._upsert(df, "indicators")

    def load_indicators(
        self,
        symbols: Optional[List[str]] = None,
        start: Optional[str] = None,
        end: Optional[str] = None,
    ) -> pd.DataFrame:
        return self._load("indicators", symbols=symbols, start=start, end=end)

    def list_symbols(self, table: str = "ohlcv") -> List[str]:
        path = self._path(table)
        if not path.exists():
            return []
        source = f"'{path.as_posix()}'"
        sql = f"SELECT DISTINCT symbol FROM read_csv_auto({source}) ORDER BY symbol"
        result = duckdb.sql(sql).df()
        return result["symbol"].tolist()

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

    def get_last_dates(self, table: str, symbols: List[str]) -> dict:
        path = self._path(table)
        if not path.exists() or not symbols:
            return {}
        source = f"'{path.as_posix()}'"
        syms = ", ".join(f"'{s}'" for s in symbols)
        sql = (
            f"SELECT symbol, MAX(date) AS last_date FROM read_csv_auto({source}) "
            f"WHERE symbol IN ({syms}) GROUP BY symbol"
        )
        result = duckdb.sql(sql).df()
        return {
            row["symbol"]: str(row["last_date"])
            for _, row in result.iterrows()
            if not pd.isna(row["last_date"])
        }

    def update_indicators(
        self,
        symbols: Optional[List[str]] = None,
        start: Optional[str] = None,
        end: Optional[str] = None,
        indicator_names: Optional[List[str]] = None,
    ) -> str:
        from ..indicators import INDICATOR_REGISTRY, compute_indicators

        ohlcv = self.load_ohlcv(symbols=symbols, start=start, end=end)
        if ohlcv.empty:
            logger.warning("No OHLCV data found for indicator computation.")
            return ""

        names = indicator_names or list(INDICATOR_REGISTRY.keys())
        result = compute_indicators(ohlcv, names)
        if result.empty:
            logger.warning("Indicator computation produced no rows.")
            return ""

        saved = self.save_indicators(result)
        logger.info("Computed and saved %d indicator rows to %s", len(result), saved)
        return saved

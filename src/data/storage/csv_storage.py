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
    "indicator_rsi": ["symbol", "date", "rsi"],
    "indicator_macd": ["symbol", "date", "macd", "macd_signal", "macd_hist"],
    "indicator_pivot": ["symbol", "date", "pivot", "r1", "s1", "r2", "s2", "r3", "s3"],
    "income_statement": [
        "symbol", "date", "period_type",
        "total_revenue", "cost_of_revenue", "gross_profit",
        "operating_income", "operating_expense", "net_income",
        "ebitda", "eps_basic", "eps_diluted",
    ],
    "balance_sheet": [
        "symbol", "date", "period_type",
        "total_assets", "total_liabilities", "stockholders_equity",
        "cash_and_equivalents", "total_debt", "net_debt",
        "current_assets", "current_liabilities",
    ],
    "cash_flow": [
        "symbol", "date", "period_type",
        "operating_cash_flow", "investing_cash_flow", "financing_cash_flow",
        "free_cash_flow", "capital_expenditure",
    ],
    "financial_ratios": [
        "symbol", "date", "period_type",
        "gross_margin", "operating_margin", "net_margin",
        "return_on_equity", "return_on_assets",
        "debt_to_equity", "current_ratio",
    ],
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
            existing_count = len(existing)
            combined = pd.concat([existing, df], ignore_index=True)
            key_cols = [c for c in ["symbol", "date"] if c in combined.columns]
            if key_cols:
                combined = combined.drop_duplicates(subset=key_cols, keep="last")
            new_rows = len(combined) - existing_count
            logger.info("[UPSERT] %d new row(s) to insert into '%s'.", new_rows, table)
        else:
            combined = df
            logger.info("[UPSERT] No existing file for '%s' — full insert of %d row(s).", table, len(combined))
        return self._save(combined, table)

    def save_ohlcv(self, df: pd.DataFrame, force_insert: bool = False) -> str:
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

    def save_indicator(self, df: pd.DataFrame, indicator_name: str) -> str:
        table = f"indicator_{indicator_name}"
        return self._upsert(df, table)

    def load_indicator(
        self,
        indicator_name: str,
        symbols: Optional[List[str]] = None,
        start: Optional[str] = None,
        end: Optional[str] = None,
    ) -> pd.DataFrame:
        table = f"indicator_{indicator_name}"
        return self._load(table, symbols=symbols, start=start, end=end)

    def save_fundamental(self, df: pd.DataFrame, statement_type: str) -> str:
        return self._upsert(df, statement_type)

    def load_fundamental(
        self,
        statement_type: str,
        symbols: Optional[List[str]] = None,
        start: Optional[str] = None,
        end: Optional[str] = None,
    ) -> pd.DataFrame:
        return self._load(statement_type, symbols=symbols, start=start, end=end)

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

    def get_last_indicator_dates(self, indicator_name: str, symbols: List[str]) -> dict:
        return self.get_last_dates(f"indicator_{indicator_name}", symbols)

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
        saved_paths: list[str] = []
        for name in names:
            result = compute_indicators(ohlcv, [name])
            if result.empty:
                logger.warning("Indicator '%s' computation produced no rows.", name)
                continue
            path = self.save_indicator(result, name)
            logger.info("Computed and saved %d rows for '%s' to %s", len(result), name, path)
            saved_paths.append(path)
        return ", ".join(saved_paths)

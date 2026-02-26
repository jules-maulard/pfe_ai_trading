from __future__ import annotations

from pathlib import Path
from typing import List, Optional

import duckdb
import pandas as pd

DEFAULT_DB_PATH = "database/ohlcv.csv"


class DuckDbCsvStorage:
    def __init__(self, db_path: str = DEFAULT_DB_PATH):
        self.db_path = Path(db_path)

    def save_prices(self, df: pd.DataFrame) -> str:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        df.sort_values(["symbol", "date"]).to_csv(self.db_path, index=False)
        return str(self.db_path)

    def load_prices(
        self,
        symbols: Optional[List[str]] = None,
        start: Optional[str] = None,
        end: Optional[str] = None,
    ) -> pd.DataFrame:
        if not self.db_path.exists():
            raise FileNotFoundError(f"Database not found: {self.db_path}")

        source = f"'{self.db_path.as_posix()}'"
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

    def save_indicator(self, df: pd.DataFrame, path: str) -> str:
        out = Path(path)
        out.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(out, index=False)
        return str(out)

    def query(self, sql: str) -> pd.DataFrame:
        return duckdb.sql(sql).df()
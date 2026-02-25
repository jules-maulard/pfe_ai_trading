from __future__ import annotations

import os
from pathlib import Path
from typing import List, Optional

import duckdb
import pandas as pd


class DuckDbCsvStorage:
    def __init__(self, base_dir: str = "data/prices"):
        self.base_dir = base_dir

    def save_prices(self, df: pd.DataFrame, partition_by_symbol: bool = True) -> str:
        os.makedirs(self.base_dir, exist_ok=True)
        if partition_by_symbol:
            for sym, group in df.groupby("symbol", dropna=False):
                sym_clean = str(sym).replace("/", "-")
                sym_dir = os.path.join(self.base_dir, sym_clean)
                os.makedirs(sym_dir, exist_ok=True)
                path = os.path.join(sym_dir, f"prices_{sym_clean}.csv")
                group.sort_values("date").to_csv(path, index=False)
            return self.base_dir
        else:
            path = os.path.join(self.base_dir, "prices.csv")
            df.sort_values(["symbol", "date"]).to_csv(path, index=False)
            return path

    def load_prices(
        self,
        path: Optional[str] = None,
        symbols: Optional[List[str]] = None,
        start: Optional[str] = None,
        end: Optional[str] = None,
    ) -> pd.DataFrame:
        p = Path(path or self.base_dir)
        if not p.exists():
            raise FileNotFoundError(f"Path not found: {p}")

        if p.is_file():
            source = f"'{p.as_posix()}'"
        else:
            csv_files = list(p.glob("**/*.csv"))
            if not csv_files:
                raise FileNotFoundError(f"No CSV files in {p}")
            source = "[" + ", ".join(f"'{f.as_posix()}'" for f in csv_files) + "]"

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

    def save_indicator(
        self,
        df: pd.DataFrame,
        path: str,
        partition_by_symbol: bool = False,
    ) -> str:
        out = Path(path)
        out.parent.mkdir(parents=True, exist_ok=True)
        if partition_by_symbol:
            for sym, group in df.groupby("symbol", dropna=False):
                sym_dir = out.parent / str(sym)
                sym_dir.mkdir(parents=True, exist_ok=True)
                group.to_csv(sym_dir / f"{out.stem}_{sym}.csv", index=False)
            return str(out.parent)
        else:
            df.to_csv(out, index=False)
            return str(out)

    def query(self, sql: str) -> pd.DataFrame:
        return duckdb.sql(sql).df()
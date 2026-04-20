from __future__ import annotations

import pandas as pd
from typing import Any, Dict, List, Optional

from ..data import BaseStorage
from ..data.config.settings import get_storage
from ..utils import get_logger

logger = get_logger(__name__)

STATEMENT_TYPES = ["income_statement", "balance_sheet", "cash_flow", "financial_ratios"]

_SUMMARY_COLS = {
    "income_statement": ["symbol", "date", "period_type", "total_revenue", "net_income", "ebitda", "eps_diluted"],
    "balance_sheet": ["symbol", "date", "period_type", "total_assets", "total_liabilities", "stockholders_equity", "net_debt"],
    "cash_flow": ["symbol", "date", "period_type", "operating_cash_flow", "free_cash_flow", "capital_expenditure"],
    "financial_ratios": ["symbol", "date", "period_type", "gross_margin", "operating_margin", "net_margin", "return_on_equity", "debt_to_equity"],
}


def _round_numeric(df: pd.DataFrame, decimals: int = 2) -> pd.DataFrame:
    num_cols = df.select_dtypes(include="number").columns
    df[num_cols] = df[num_cols].round(decimals)
    return df


def _trim(df: pd.DataFrame, limit: int) -> pd.DataFrame:
    return df.sort_values(["symbol", "date"], ascending=[True, False]).head(limit).reset_index(drop=True)


class FundamentalService:
    def __init__(self, storage: BaseStorage = None):
        self.storage = storage or get_storage()

    def get_income_statement(
        self,
        symbols: List[str],
        period_type: Optional[str] = None,
        limit: int = 8,
    ) -> Dict[str, Any]:
        return self._load_statement("income_statement", symbols, period_type, limit)

    def get_balance_sheet(
        self,
        symbols: List[str],
        period_type: Optional[str] = None,
        limit: int = 8,
    ) -> Dict[str, Any]:
        return self._load_statement("balance_sheet", symbols, period_type, limit)

    def get_cash_flow(
        self,
        symbols: List[str],
        period_type: Optional[str] = None,
        limit: int = 8,
    ) -> Dict[str, Any]:
        return self._load_statement("cash_flow", symbols, period_type, limit)

    def get_financial_ratios(
        self,
        symbols: List[str],
        period_type: Optional[str] = None,
        limit: int = 8,
    ) -> Dict[str, Any]:
        return self._load_statement("financial_ratios", symbols, period_type, limit)

    def get_dividends(
        self,
        symbols: List[str],
        limit: int = 10,
    ) -> Dict[str, Any]:
        df = self.storage.load_dividend(symbols=symbols)
        if df.empty:
            return {"status": "ok", "count": 0, "data": []}
        df = _round_numeric(df)
        df = _trim(df, limit)
        return {
            "status": "ok",
            "count": len(df),
            "data": df.to_dict(orient="records"),
        }

    def get_fundamental_summary(
        self,
        symbols: List[str],
        period_type: Optional[str] = "annual",
    ) -> Dict[str, Any]:
        summary: Dict[str, Any] = {"status": "ok", "symbols": symbols}
        for stmt in STATEMENT_TYPES:
            result = self._load_statement(stmt, symbols, period_type, limit=2)
            summary[stmt] = result.get("data", [])
        return summary

    def _load_statement(
        self,
        statement_type: str,
        symbols: List[str],
        period_type: Optional[str],
        limit: int,
    ) -> Dict[str, Any]:
        df = self.storage.load_fundamental(statement_type, symbols=symbols)
        if df.empty:
            return {"status": "ok", "count": 0, "data": []}

        if period_type and "period_type" in df.columns:
            df = df[df["period_type"] == period_type]

        cols = _SUMMARY_COLS.get(statement_type)
        if cols:
            available = [c for c in cols if c in df.columns]
            df = df[available]

        df = _round_numeric(df)
        df = _trim(df, limit)
        return {
            "status": "ok",
            "count": len(df),
            "data": df.to_dict(orient="records"),
        }

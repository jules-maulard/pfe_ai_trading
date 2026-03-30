from __future__ import annotations

from abc import ABC, abstractmethod
from typing import List, Optional
import pandas as pd


class BaseStorage(ABC):

    @abstractmethod
    def save_ohlcv(self, df: pd.DataFrame) -> str:
        ...

    @abstractmethod
    def load_ohlcv(
        self,
        symbols: Optional[List[str]] = None,
        start: Optional[str] = None,
        end: Optional[str] = None,
    ) -> pd.DataFrame:
        ...

    @abstractmethod
    def save_dividend(self, df: pd.DataFrame) -> str:
        ...

    @abstractmethod
    def load_dividend(
        self,
        symbols: Optional[List[str]] = None,
        start: Optional[str] = None,
        end: Optional[str] = None,
    ) -> pd.DataFrame:
        ...

    @abstractmethod
    def save_asset(self, df: pd.DataFrame) -> str:
        ...

    @abstractmethod
    def load_asset(self, symbols: Optional[List[str]] = None) -> pd.DataFrame:
        ...

    @abstractmethod
    def save_indicators(self, df: pd.DataFrame) -> str:
        ...

    @abstractmethod
    def load_indicators(
        self,
        symbols: Optional[List[str]] = None,
        start: Optional[str] = None,
        end: Optional[str] = None,
    ) -> pd.DataFrame:
        ...

    @abstractmethod
    def list_symbols(self, table: str = "ohlcv") -> List[str]:
        ...

    @abstractmethod
    def get_last_date(self, table: str, symbol: str) -> Optional[str]:
        ...

    @abstractmethod
    def get_last_dates(self, table: str, symbols: List[str]) -> dict:
        """Return {symbol: last_date_str} for all given symbols in one query."""
        ...

    @abstractmethod
    def update_indicators(
        self,
        symbols: Optional[List[str]] = None,
        start: Optional[str] = None,
        end: Optional[str] = None,
    ) -> str:
        ...

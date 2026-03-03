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
    def save_asset(self, df: pd.DataFrame) -> str:
        ...

    @abstractmethod
    def load_asset(self, symbols: Optional[List[str]] = None) -> pd.DataFrame:
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
    def append_ohlcv(self, df: pd.DataFrame) -> str:
        ...

    @abstractmethod
    def upsert_ohlcv(self, df: pd.DataFrame) -> str:
        ...

    @abstractmethod
    def append_dividend(self, df: pd.DataFrame) -> str:
        ...

    @abstractmethod
    def upsert_dividend(self, df: pd.DataFrame) -> str:
        ...

    @abstractmethod
    def get_last_date(self, table: str, symbol: str) -> Optional[str]:
        ...

from __future__ import annotations

import os
from typing import List

from dotenv import load_dotenv

from .retrievers import CAC40_TICKERS, YFinanceRetriever
from .storage import BaseStorage
from ..utils import get_logger

logger = get_logger(__name__)

START_DATE = "2016-01-01"

def run_pipeline(symbols: List[str], storage: BaseStorage) -> None:
    retriever = YFinanceRetriever()
    ohlcv = retriever.get_ohlcv(symbols, start=START_DATE)

    if ohlcv.empty:
        logger.warning("No OHLCV data fetched. Pipeline aborted.")
        return

    storage.save_ohlcv(ohlcv)
    storage.update_indicators(symbols=symbols)

def _build_storage() -> BaseStorage:
        load_dotenv()
        backend = os.environ.get("STORAGE_BACKEND", "csv").lower()
        logger.info(f"Using storage backend: {backend}")

        if backend == "csv":
            from .storage import CsvStorage
            return CsvStorage()
        if backend == "snowflake":
            from .storage import SnowflakeStorage
            return SnowflakeStorage()
        raise ValueError(f"Unknown STORAGE_BACKEND: {backend}")

def main():
    storage = _build_storage()
    run_pipeline(symbols=CAC40_TICKERS, storage=storage)


if __name__ == "__main__":
    main()


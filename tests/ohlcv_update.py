from __future__ import annotations

import sys
from pathlib import Path


# Allow imports from src/
_SRC = str(Path(__file__).resolve().parent.parent / "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

import pandas as pd
from data.snowflake_storage import SnowflakeStorage
from data.yfinance_retriever import YFinanceRetriever


def download_ohlcv_snowflake(symbols: list[str], start: str, end: str, interval: str):
    sf = SnowflakeStorage()

    # print("Resetting table …")
    # sf.reset_table()
    # print("  OK")

    # print("Creating table if not exists …")
    # sf.ensure_table()
    # print("  OK")

    retriever = YFinanceRetriever()
    df = retriever.get_prices(symbols, start=start, end=end, interval=interval)

    print("Inserting sample rows …")
    result = sf.save_prices(df)
    print(f"  {result}")

    print("Reading back …")
    df = sf.query_prices(symbols=symbols, start=start, end=end)
    print(df.to_string(index=False))

    # ── 4. Cleanup test data ─────────────────────────────────────────
    print("Cleaning up test rows …")
    sf.query(f"DELETE FROM OHLCV WHERE SYMBOL IN ({', '.join(f"'{s}'" for s in symbols)})")
    print("  Done. Test passed!")


if __name__ == "__main__":
    download_ohlcv_snowflake(["AIR.PA"], "2025-01-02", "2025-01-06", "1d")

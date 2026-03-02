"""
Quick test: seed a few rows into Snowflake and read them back.

Usage:
    1. Fill your .env with Snowflake credentials (see snowflake_storage.py)
    2. Run:  python tests/test_snowflake_quick.py
"""
from __future__ import annotations

import sys
from pathlib import Path

# Allow imports from src/
_SRC = str(Path(__file__).resolve().parent.parent / "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

import pandas as pd
from data.snowflake_storage import SnowflakeStorage


def main():
    sf = SnowflakeStorage()

    # ── 1. Create table ──────────────────────────────────────────────
    print("Creating table if not exists …")
    sf.ensure_table()
    print("  OK")

    # ── 2. Insert sample data ────────────────────────────────────────
    sample = pd.DataFrame(
        {
            "symbol": ["TEST.PA", "TEST.PA", "TEST.PA"],
            "date": pd.to_datetime(["2025-01-02", "2025-01-03", "2025-01-06"]),
            "open": [100.0, 101.5, 102.0],
            "high": [102.0, 103.0, 104.0],
            "low": [99.0, 100.0, 101.0],
            "close": [101.0, 102.0, 103.5],
            "volume": [1000, 1500, 1200],
        }
    )
    print("Inserting sample rows …")
    result = sf.save_prices(sample)
    print(f"  {result}")

    # ── 3. Read back ─────────────────────────────────────────────────
    print("Reading back …")
    df = sf.query_prices(symbols=["TEST.PA"])
    print(df.to_string(index=False))

    # # ── 4. Cleanup test data ─────────────────────────────────────────
    print("Cleaning up test rows …")
    sf.query("DELETE FROM OHLCV WHERE SYMBOL = 'TEST.PA'")
    print("  Done. Test passed!")


if __name__ == "__main__":
    main()

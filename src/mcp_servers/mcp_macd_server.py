from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastmcp import FastMCP

_SRC = str(Path(__file__).resolve().parent.parent)
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

from services.macd_service import MACDService

mcp = FastMCP("MACD Tools")
macd_service = MACDService()


@mcp.tool(name="health_check", description="Check server health.")
def health_check() -> Dict[str, Any]:
    return {"status": "ok"}


@mcp.tool(
    name="compute_macd",
    description="Compute MACD for symbols from local OHLCV data.",
)
def compute_macd_tool(
    fast: int = 12,
    slow: int = 26,
    signal: int = 9,
    price_col: str = "close",
    symbols: Optional[List[str]] = None,
    start: Optional[str] = None,
    end: Optional[str] = None,
    sample_rows: int = 5,
) -> Dict[str, Any]:
    return macd_service.compute(
        fast=fast, slow=slow, signal=signal,
        price_col=price_col, symbols=symbols, start=start, end=end,
        sample_rows=sample_rows,
    )


if __name__ == "__main__":
    mcp.run()

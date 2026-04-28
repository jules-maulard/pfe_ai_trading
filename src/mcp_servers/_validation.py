from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional, Set

import pandas as pd

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
_OHLCV_PATH = _PROJECT_ROOT / "database" / "csv" / "ohlcv.csv"

_available_symbols: Optional[Set[str]] = None


def _load_available_symbols() -> Set[str]:
    global _available_symbols
    if _available_symbols is None:
        _available_symbols = set(
            pd.read_csv(_OHLCV_PATH, usecols=["symbol"])["symbol"].unique()
        )
    return _available_symbols


def validate_symbols(
    symbols: Optional[List[str]],
    required: bool = True,
) -> Optional[Dict[str, Any]]:
    """Return an error dict if symbols are missing or invalid, else None."""
    if not symbols:
        if required:
            return {
                "status": "error",
                "message": "Parameter 'symbols' is required. Pass a list like ['AIR.PA'].",
            }
        return None

    available = _load_available_symbols()
    invalid = [s for s in symbols if s not in available]
    if invalid:
        return {
            "status": "error",
            "message": f"Unknown symbols: {invalid}. Use list_symbols() to get valid tickers.",
            "invalid_symbols": invalid,
        }
    return None

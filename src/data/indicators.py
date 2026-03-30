from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, List

import pandas as pd

from ..utils import get_logger

logger = get_logger(__name__)


@dataclass(frozen=True)
class IndicatorDef:
    compute: Callable[[pd.DataFrame], pd.DataFrame]
    columns: List[str]


def _compute_rsi(ohlcv: pd.DataFrame) -> pd.DataFrame:
    from ..mcp_servers.rsi_service import RSIService
    df = RSIService.compute_rsi_wilder(ohlcv)
    df = df.rename(columns={"rsi14": "rsi"})
    return df


def _compute_macd(ohlcv: pd.DataFrame) -> pd.DataFrame:
    from ..mcp_servers.macd_service import MACDService
    return MACDService.compute_macd(ohlcv)


INDICATOR_REGISTRY: dict[str, IndicatorDef] = {
    "rsi": IndicatorDef(compute=_compute_rsi, columns=["rsi"]),
    "macd": IndicatorDef(compute=_compute_macd, columns=["macd", "macd_signal", "macd_hist"]),
}


def compute_indicators(
    ohlcv: pd.DataFrame,
    indicator_names: List[str],
) -> pd.DataFrame:
    """Apply requested indicators and return a DataFrame with symbol, date, and indicator columns."""
    unknown = [n for n in indicator_names if n not in INDICATOR_REGISTRY]
    if unknown:
        raise ValueError(
            f"Unknown indicator(s): {unknown}. "
            f"Available: {list(INDICATOR_REGISTRY.keys())}"
        )

    result = ohlcv[["symbol", "date"]].copy()
    all_columns: list[str] = []

    for name in indicator_names:
        defn = INDICATOR_REGISTRY[name]
        logger.info("Computing indicator '%s'…", name)
        computed = defn.compute(ohlcv)
        for col in defn.columns:
            if col in computed.columns:
                result[col] = computed[col].values
                all_columns.append(col)
            else:
                logger.warning("Indicator '%s' did not produce column '%s'", name, col)

    result = result.dropna(subset=all_columns, how="all")
    result = result.sort_values(["symbol", "date"]).reset_index(drop=True)
    logger.info("Computed %d indicator rows (%s)", len(result), all_columns)
    return result

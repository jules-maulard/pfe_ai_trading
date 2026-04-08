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


def _compute_pivot(ohlcv: pd.DataFrame) -> pd.DataFrame:
    from ..mcp_servers.pivot_service import PivotService
    return PivotService.compute_pivots(ohlcv)


INDICATOR_REGISTRY: dict[str, IndicatorDef] = {
    "rsi": IndicatorDef(compute=_compute_rsi, columns=["rsi"]),
    "macd": IndicatorDef(compute=_compute_macd, columns=["macd", "macd_signal", "macd_hist"]),
    "pivot": IndicatorDef(compute=_compute_pivot, columns=["pivot", "r1", "s1", "r2", "s2", "r3", "s3"]),
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
        logger.debug("Computing indicator '%s' on %d OHLCV rows…", name, len(ohlcv))
        computed = defn.compute(ohlcv)
        available_cols = [col for col in defn.columns if col in computed.columns]
        missing_cols = [col for col in defn.columns if col not in computed.columns]
        for col in missing_cols:
            logger.warning("Indicator '%s' did not produce column '%s'", name, col)
        if available_cols:
            result = result.merge(
                computed[["symbol", "date"] + available_cols],
                on=["symbol", "date"],
                how="left",
            )
            all_columns.extend(available_cols)

    result = result.dropna(subset=all_columns, how="any")
    result = result.sort_values(["symbol", "date"]).reset_index(drop=True)
    logger.debug("compute_indicators: produced %d rows before new-rows filter (%s)", len(result), all_columns)
    return result

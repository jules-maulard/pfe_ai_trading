# src/ta_indicators.py
"""
Technical indicators utilities (minimal, dependency-light).

Currently includes:
- compute_rsi_wilder(): RSI calculation per Wilder's method

Expected input schema: tidy DataFrame with at least ['symbol', 'date', price_col].
Output: same DataFrame with an added RSI column (e.g., 'rsi14').
"""
from __future__ import annotations
import pandas as pd

def compute_rsi_wilder(df: pd.DataFrame, price_col: str = "adj_close", window: int = 14) -> pd.DataFrame:
    """
    Compute Wilder's RSI for each symbol in a tidy OHLCV DataFrame.

    Parameters
    ----------
    df : pandas.DataFrame
        Must contain columns ['symbol', 'date', price_col].
    price_col : str
        Which column to use for price changes (e.g., 'adj_close' or 'close').
    window : int
        RSI lookback window (default 14).

    Returns
    -------
    pandas.DataFrame
        Copy of input with an additional column f"rsi{window}".
    """
    required = {"symbol", "date", price_col}
    if not required.issubset(df.columns):
        missing = required - set(df.columns)
        raise ValueError(f"Missing required columns: {sorted(missing)}")

    out = df.copy()
    out = out.sort_values(["symbol", "date"])  # ensure chronological by symbol

    def _one(g: pd.DataFrame) -> pd.DataFrame:
        delta = g[price_col].diff()
        gain = delta.clip(lower=0)
        loss = -delta.clip(upper=0)
        # Wilder's smoothing via EMA with alpha = 1/window
        avg_gain = gain.ewm(alpha=1/window, adjust=False, min_periods=window).mean()
        avg_loss = loss.ewm(alpha=1/window, adjust=False, min_periods=window).mean()
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        # Guard against division by zero and flat series
        rsi = rsi.where(avg_loss != 0, 100.0)
        rsi = rsi.where(~((avg_gain == 0) & (avg_loss == 0)), pd.NA)
        g[f"rsi{window}"] = rsi
        return g

    out = out.groupby("symbol", group_keys=False).apply(_one)
    return out
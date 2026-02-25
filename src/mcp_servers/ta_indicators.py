from __future__ import annotations

import pandas as pd


def compute_rsi_wilder(df: pd.DataFrame, price_col: str = "close", window: int = 14) -> pd.DataFrame:
    required = {"symbol", "date", price_col}
    if not required.issubset(df.columns):
        raise ValueError(f"Missing required columns: {sorted(required - set(df.columns))}")

    out = df.copy().sort_values(["symbol", "date"])
    parts = []
    for _, g in out.groupby("symbol"):
        g = g.copy()
        delta = g[price_col].diff()
        gain = delta.clip(lower=0)
        loss = -delta.clip(upper=0)
        avg_gain = gain.ewm(alpha=1 / window, adjust=False, min_periods=window).mean()
        avg_loss = loss.ewm(alpha=1 / window, adjust=False, min_periods=window).mean()
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        rsi = rsi.where(avg_loss != 0, 100.0)
        rsi = rsi.where(~((avg_gain == 0) & (avg_loss == 0)), pd.NA)
        g[f"rsi{window}"] = rsi
        parts.append(g)
    return pd.concat(parts, ignore_index=True)


def compute_macd(
    df: pd.DataFrame,
    price_col: str = "close",
    fast: int = 12,
    slow: int = 26,
    signal: int = 9,
) -> pd.DataFrame:
    required = {"symbol", "date", price_col}
    if not required.issubset(df.columns):
        raise ValueError(f"Missing required columns: {sorted(required - set(df.columns))}")

    out = df.copy().sort_values(["symbol", "date"])
    parts = []
    for _, g in out.groupby("symbol"):
        g = g.copy()
        ema_fast = g[price_col].ewm(span=fast, adjust=False, min_periods=fast).mean()
        ema_slow = g[price_col].ewm(span=slow, adjust=False, min_periods=slow).mean()
        macd_line = ema_fast - ema_slow
        signal_line = macd_line.ewm(span=signal, adjust=False, min_periods=signal).mean()
        g["macd"] = macd_line
        g["macd_signal"] = signal_line
        g["macd_hist"] = macd_line - signal_line
        parts.append(g)
    return pd.concat(parts, ignore_index=True)
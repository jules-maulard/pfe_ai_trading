"""
RSI MCP Tool Server (FastMCP)

- Loads tidy OHLCV saved by your yfinance ingestor (CSV or Parquet).
- Computes RSI (Wilder) per symbol with configurable window and price column.
- Returns a compact JSON payload (status, count, columns, sample) and can save output.

Run (dev inspector):
    fastmcp dev src/mcp_rsi_server.py

Run (stdio, e.g., for MCP host/clients):
    python src/mcp_rsi_server.py
"""
from __future__ import annotations

# from curses import window
from pathlib import Path
from typing import List, Optional, Dict, Any
from urllib.request import Request
import asyncio

from fastapi.responses import PlainTextResponse
import pandas as pd
from fastmcp import Client, Context, FastMCP

try:
    from .ta_indicators import compute_rsi_wilder, compute_macd
except ImportError:
    from ta_indicators import compute_rsi_wilder, compute_macd

mcp = FastMCP("RSI Tools")

# mcp = FastMCP(
#     name = "RSI Tools", 
#     instructions="""
#         This server provides data analysis tools. 
#         Call compute_rsi() to compute the Relative Strength Index (RSI) from local OHLCV data.
#     """,
# )

# ----------------------
# Data loading helpers
# ----------------------
def _read_one(fp: Path) -> pd.DataFrame:
    if fp.suffix.lower() == ".csv":
        return pd.read_csv(fp, parse_dates=["date"])
    elif fp.suffix.lower() in {".parquet", ".pq"}:
        return pd.read_parquet(fp)
    else:
        raise ValueError(f"Unsupported file extension: {fp}")

def _load_prices_any(path: str) -> pd.DataFrame:
    """
    Load tidy prices from a directory or a file.

    Expected tidy schema from yfinance ingestor:
    [symbol, date, open, high, low, close, adj_close, volume]
    """
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Path not found: {path}")

    if p.is_file():
        return _read_one(p)

    # Directory: prefer consolidated files first
    for cand in (p / "prices.csv", p / "prices.parquet"):
        if cand.exists():
            return _read_one(cand)

    # Else read partitioned files
    files = list(p.glob("*/prices_*.csv")) + list(p.glob("*/prices_*.parquet"))
    if not files:
        raise FileNotFoundError(
            f"No prices found in '{path}'. Expected prices.csv, prices.parquet, or */prices_*.csv"
        )
    parts = [_read_one(fp) for fp in files]
    return pd.concat(parts, ignore_index=True)

# ----------------------
# MCP Tool
# ----------------------
@mcp.tool(
    name="health_check",
    description="Check the health of the RSI MCP server."
)
def health_check() -> Dict[str, Any]:
    return {"status": "ok"}

@mcp.tool(
    name="compute_rsi",
    description=(
        "Compute Wilder's RSI for one or more symbols from local OHLCV data. "
        "Works with CSV/Parquet produced by the yfinance ingestor. "
        "Returns a compact JSON sample and can save the full result."
    ),
)
def compute_rsi(
    data_path: str,
    window: int = 14,
    price_col: str = "close",          # or "close"
    symbols: Optional[List[str]] = None,   # None = all symbols found
    start: Optional[str] = None,           # "YYYY-MM-DD"
    end: Optional[str] = None,             # "YYYY-MM-DD"
    save: bool = False,
    save_path: str = "data/indicators/rsi14.csv",
    save_format: str = "csv",              # "csv" or "parquet"
    partition_by_symbol: bool = False,
    sample_rows: int = 5,
) -> Dict[str, Any]:
    # Load
    df = _load_prices_any(data_path)

    # Validate minimal columns
    needed = {"symbol", "date", price_col}
    missing = needed - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {sorted(missing)}")

    # Filter
    if symbols:
        df = df[df["symbol"].isin(symbols)].copy()
        if df.empty:
            raise ValueError("No rows for requested symbols.")
    if start:
        df = df[df["date"] >= pd.to_datetime(start)].copy()
    if end:
        df = df[df["date"] <= pd.to_datetime(end)].copy()




    out = compute_rsi_wilder(df, price_col=price_col, window=window)
    rsi_col = f"rsi{window}"

    # Uniformiser la sortie en DataFrame
    if isinstance(out, pd.Series):
        out = out.to_frame(name=rsi_col)
    elif not isinstance(out, pd.DataFrame):
        raise ValueError(f"compute_rsi_wilder returned unsupported type: {type(out)}")

    # Déterminer le nom de la colonne RSI si différent
    if rsi_col not in out.columns:
        candidates = [c for c in out.columns if str(c).lower().startswith("rsi")]
        if len(candidates) == 1:
            rsi_col = candidates[0]
        elif len(candidates) > 1:
            exact = [c for c in candidates if str(c).lower() == f"rsi{window}"]
            rsi_col = exact[0] if exact else candidates[0]
        else:
            raise ValueError(f"RSI column '{rsi_col}' not found in output columns: {list(out.columns)}")

    # Toujours remettre l’index en colonnes (même si RangeIndex)
    out = out.reset_index()

    # Normaliser les noms de colonnes en minuscules pour matcher facilement
    lower_map = {c: str(c).lower() for c in out.columns}
    out.rename(columns=lower_map, inplace=True)

    # Essayer de mapper les alias fréquents -> symbol/date
    alias_map = {
        "ticker": "symbol",
        "isin": "symbol",
        "asset": "symbol",
        "security": "symbol",
        "datetime": "date",
        "timestamp": "date",
        "level_0": "symbol",  # MultiIndex sans noms: souvent level_0 / level_1
        "level_1": "date",
        "index": "date",
        "time": "date",
    }
    for k, v in alias_map.items():
        if k in out.columns and v not in out.columns:
            out.rename(columns={k: v}, inplace=True)

    # Si toujours pas symbol/date, tenter d’inférer par type (objet vs datetime)
    if "date" not in out.columns:
        for c in out.columns:
            if pd.api.types.is_datetime64_any_dtype(out[c]):
                out.rename(columns={c: "date"}, inplace=True)
                break

    if "symbol" not in out.columns:
        # Heuristique: colonne de type object/catégoriel non 'rsi' qui peut être le symbole
        obj_candidates = [c for c in out.columns
                        if c not in {rsi_col, "date"} and out[c].dtype == "object"]
        if obj_candidates:
            out.rename(columns={obj_candidates[0]: "symbol"}, inplace=True)

    # Dernier fallback: réaligner avec le df source si mêmes longueurs
    if "symbol" not in out.columns or "date" not in out.columns:
        df_sorted = df.sort_values(["symbol", "date"]).reset_index(drop=True)[["symbol", "date"]]
        # On tente d’aligner la taille
        if len(df_sorted) == len(out):
            out = pd.concat(
                [df_sorted, out[[rsi_col]].reset_index(drop=True)],
                axis=1
            )
        else:
            # Impossible d’inférer proprement -> diagnostic utile
            raise ValueError(
                "Unable to reconstruct 'symbol'/'date' columns from compute_rsi_wilder output. "
                f"Output columns: {list(out.columns)}, index names: {getattr(out.index, 'names', None)}, "
                f"len(df)={len(df)}, len(out)={len(out)}"
            )

    # Maintenant on peut construire le résultat
    result = (
        out[["symbol", "date", rsi_col]]
        .dropna(subset=[rsi_col])
        .sort_values(["symbol", "date"])
        .reset_index(drop=True)
    )




    
    
    # Optional save
    saved_to = None
    if save:
        out_path = Path(save_path)
        out_path.parent.mkdir(parents=True, exist_ok=True)

        if partition_by_symbol:
            for sym, g in result.groupby("symbol", dropna=False):
                subdir = out_path.parent / str(sym)
                subdir.mkdir(parents=True, exist_ok=True)
                if save_format.lower() == "csv":
                    g.to_csv(subdir / f"rsi_{sym}.csv", index=False)
                elif save_format.lower() in {"parquet", "pq"}:
                    g.to_parquet(subdir / f"rsi_{sym}.parquet", index=False, engine="pyarrow")
                else:
                    raise ValueError("Unsupported save_format; use 'csv' or 'parquet'.")
            saved_to = str(out_path.parent)
        else:
            if save_format.lower() == "csv":
                result.to_csv(out_path, index=False)
            elif save_format.lower() in {"parquet", "pq"}:
                result.to_parquet(out_path, index=False, engine="pyarrow")
            else:
                raise ValueError("Unsupported save_format; use 'csv' or 'parquet'.")
            saved_to = str(out_path)

    # Compact preview for the tool response
    sample = []
    if sample_rows and sample_rows > 0:
        tail = result.tail(sample_rows).copy()
        if not tail.empty and pd.api.types.is_datetime64_any_dtype(tail["date"]):
            tail["date"] = tail["date"].dt.tz_localize(None).astype(str)
        sample = tail.to_dict(orient="records")

    return {
        "status": "ok",
        "count": int(len(result)),
        "saved_to": saved_to,
        "columns": ["symbol", "date", rsi_col],
        "sample": sample,
    }

@mcp.tool(
    name="compute_macd",
    description=(
        "Compute MACD (Moving Average Convergence Divergence) for one or more symbols "
        "from local OHLCV data. Returns macd, macd_signal, and macd_hist columns. "
        "Works with CSV/Parquet produced by the yfinance ingestor."
    ),
)
def compute_macd_tool(
    data_path: str,
    fast: int = 12,
    slow: int = 26,
    signal: int = 9,
    price_col: str = "close",
    symbols: Optional[List[str]] = None,
    start: Optional[str] = None,
    end: Optional[str] = None,
    save: bool = False,
    save_path: str = "data/indicators/macd.csv",
    save_format: str = "csv",
    partition_by_symbol: bool = False,
    sample_rows: int = 5,
) -> Dict[str, Any]:
    # Load
    df = _load_prices_any(data_path)

    # Validate minimal columns
    needed = {"symbol", "date", price_col}
    missing = needed - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {sorted(missing)}")

    # Filter
    if symbols:
        df = df[df["symbol"].isin(symbols)].copy()
        if df.empty:
            raise ValueError("No rows for requested symbols.")
    if start:
        df = df[df["date"] >= pd.to_datetime(start)].copy()
    if end:
        df = df[df["date"] <= pd.to_datetime(end)].copy()

    out = compute_macd(df, price_col=price_col, fast=fast, slow=slow, signal=signal)

    result = (
        out[["symbol", "date", "macd", "macd_signal", "macd_hist"]]
        .dropna(subset=["macd", "macd_signal", "macd_hist"])
        .sort_values(["symbol", "date"])
        .reset_index(drop=True)
    )

    # Optional save
    saved_to = None
    if save:
        out_path = Path(save_path)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        if partition_by_symbol:
            for sym, g in result.groupby("symbol", dropna=False):
                subdir = out_path.parent / str(sym)
                subdir.mkdir(parents=True, exist_ok=True)
                if save_format.lower() == "csv":
                    g.to_csv(subdir / f"macd_{sym}.csv", index=False)
                elif save_format.lower() in {"parquet", "pq"}:
                    g.to_parquet(subdir / f"macd_{sym}.parquet", index=False, engine="pyarrow")
                else:
                    raise ValueError("Unsupported save_format; use 'csv' or 'parquet'.")
            saved_to = str(out_path.parent)
        else:
            if save_format.lower() == "csv":
                result.to_csv(out_path, index=False)
            elif save_format.lower() in {"parquet", "pq"}:
                result.to_parquet(out_path, index=False, engine="pyarrow")
            else:
                raise ValueError("Unsupported save_format; use 'csv' or 'parquet'.")
            saved_to = str(out_path)

    # Compact preview
    sample = []
    if sample_rows and sample_rows > 0:
        tail = result.tail(sample_rows).copy()
        if not tail.empty and pd.api.types.is_datetime64_any_dtype(tail["date"]):
            tail["date"] = tail["date"].dt.tz_localize(None).astype(str)
        sample = tail.to_dict(orient="records")

    return {
        "status": "ok",
        "count": int(len(result)),
        "saved_to": saved_to,
        "columns": ["symbol", "date", "macd", "macd_signal", "macd_hist"],
        "sample": sample,
    }


@mcp.prompt(
    name="compute_rsi_prompt",
    description="prompt for computing RSI with the compute_rsi tool"
)
def compute_rsi_prompt(symbol: str) -> str:
    return f"""
    Please compute the RSI for {symbol} using the compute_rsi tool.
    Make sure to specify the correct data path and parameters.
    The response should include the RSI values along with the corresponding dates.
    """


@mcp.tool(
    name="compute_rsi_sampling_test",
    description="Tool that returns a prompt for testing sampling of prompts with tool calls."
)
async def compute_rsi_sampling_test(
    symbol: str, 
    data_path: str,
    ctx: Context
) -> str:
    rsi = compute_rsi(data_path, symbols=[symbol], sample_rows=1)["sample"][0]["rsi14"]
    result = await ctx.sample(f"Please interpret this result: rsi = {rsi}")
    return result.text



if __name__ == "__main__":
    # Default: stdio transport (works with MCP hosts/clients).
    # You can also run the handy dev inspector: `fastmcp dev src/mcp_rsi_server.py`
    mcp.run()
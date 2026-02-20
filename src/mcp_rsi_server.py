# # src/mcp_rsi_server.py
# """
# Minimal MCP-like RSI tool server (HTTP) using FastAPI.

# What it does
# ------------
# - Loads local OHLCV data produced by your yfinance ingestor (CSV or Parquet).
# - Computes RSI (Wilder) per symbol with a given window and price column.
# - Exposes a simple HTTP endpoint `/tools/rsi` that returns JSON.

# Dependencies
# ------------
#     pip install fastapi uvicorn pandas pyarrow

# Run
# ---
# PowerShell or bash:
#     uvicorn src.mcp_rsi_server:app --host 0.0.0.0 --port 8085 --reload

# Example request
# ---------------
# POST http://localhost:8085/tools/rsi
# Body (JSON):
# {
#   "data_path": "data/prices",
#   "symbols": ["AIR.PA", "SU.PA"],
#   "window": 14,
#   "price_col": "adj_close",
#   "start": "2020-01-01",
#   "end": null,
#   "save": false,
#   "save_path": "data/indicators/rsi14.csv",
#   "save_format": "csv",
#   "partition_by_symbol": false,
#   "sample_rows": 5
# }
# """
# from __future__ import annotations

# from pathlib import Path
# from typing import List, Optional

# import pandas as pd
# from fastapi import FastAPI, HTTPException
# from pydantic import BaseModel, Field

# from .ta_indicators import compute_rsi_wilder

# # ----------------------
# # Data loading helpers
# # ----------------------
# def _load_prices_any(path: str) -> pd.DataFrame:
#     """Load tidy prices from a directory or a file.

#     Expected schema from your yfinance ingestor:
#     [symbol, date, open, high, low, close, adj_close, volume]
#     """
#     p = Path(path)
#     if not p.exists():
#         raise FileNotFoundError(f"Path not found: {path}")

#     def _read_one(fp: Path) -> pd.DataFrame:
#         if fp.suffix.lower() == ".csv":
#             return pd.read_csv(fp, parse_dates=["date"])  # expects tidy schema
#         elif fp.suffix.lower() in {".parquet", ".pq"}:
#             return pd.read_parquet(fp)
#         else:
#             raise ValueError(f"Unsupported file extension: {fp}")

#     if p.is_file():
#         return _read_one(p)

#     # Directory: try consolidated first
#     cand_csv = p / "prices.csv"
#     cand_pq = p / "prices.parquet"
#     if cand_csv.exists():
#         return _read_one(cand_csv)
#     if cand_pq.exists():
#         return _read_one(cand_pq)

#     # Else: try partitioned files per symbol
#     files = list(p.glob("*/prices_*.csv")) + list(p.glob("*/prices_*.parquet"))
#     if not files:
#         raise FileNotFoundError(
#             f"No prices found in directory '{path}'. Expected prices.csv, prices.parquet, or */prices_*.csv"
#         )
#     parts = [_read_one(fp) for fp in files]
#     return pd.concat(parts, ignore_index=True)

# # ----------------------
# # Request/Response models
# # ----------------------
# class RSIRequest(BaseModel):
#     data_path: str = Field(..., description="Path to prices file or directory (from yfinance ingestor)")
#     symbols: Optional[List[str]] = Field(None, description="Subset of symbols to compute; None = all")
#     window: int = Field(14, ge=2, le=252, description="RSI lookback window")
#     price_col: str = Field("adj_close", description="Which price column to use (adj_close or close)")
#     start: Optional[str] = Field(None, description="Filter start date YYYY-MM-DD")
#     end: Optional[str] = Field(None, description="Filter end date YYYY-MM-DD")

#     save: bool = Field(False, description="Whether to save output to disk")
#     save_path: str = Field("data/indicators/rsi14.csv", description="Output path if save=True")
#     save_format: str = Field("csv", description="csv or parquet")
#     partition_by_symbol: bool = Field(False, description="If saving, write one file per symbol")

#     sample_rows: int = Field(5, ge=0, le=50, description="How many rows to return as sample in response")

# class RSIResponse(BaseModel):
#     status: str
#     count: int
#     saved_to: Optional[str]
#     columns: List[str]
#     sample: List[dict]

# # ----------------------
# # FastAPI app
# # ----------------------
# app = FastAPI(title="RSI MCP Tool Server", version="0.1.0")

# @app.post("/tools/rsi", response_model=RSIResponse)
# def compute_rsi(req: RSIRequest) -> RSIResponse:
#     try:
#         df = _load_prices_any(req.data_path)
#     except Exception as e:
#         raise HTTPException(status_code=400, detail=f"Load error: {e}")

#     # Basic validation and filtering
#     expected_cols = {"symbol", "date", req.price_col}
#     missing = expected_cols - set(df.columns)
#     if missing:
#         raise HTTPException(status_code=400, detail=f"Missing required columns: {sorted(missing)}")

#     if req.symbols:
#         df = df[df["symbol"].isin(req.symbols)].copy()
#         if df.empty:
#             raise HTTPException(status_code=400, detail="No rows for requested symbols.")

#     if req.start:
#         df = df[df["date"] >= pd.to_datetime(req.start)].copy()
#     if req.end:
#         df = df[df["date"] <= pd.to_datetime(req.end)].copy()

#     # Compute RSI
#     try:
#         out = compute_rsi_wilder(df, price_col=req.price_col, window=req.window)
#     except Exception as e:
#         raise HTTPException(status_code=400, detail=f"RSI compute error: {e}")

#     rsi_col = f"rsi{req.window}"
#     result = out[["symbol", "date", rsi_col]].dropna(subset=[rsi_col])
#     result = result.sort_values(["symbol", "date"]).reset_index(drop=True)

#     saved_to = None
#     if req.save:
#         out_path = Path(req.save_path)
#         out_path.parent.mkdir(parents=True, exist_ok=True)
#         if req.partition_by_symbol:
#             for sym, g in result.groupby("symbol", dropna=False):
#                 subdir = out_path.parent / str(sym)
#                 subdir.mkdir(parents=True, exist_ok=True)
#                 if req.save_format.lower() == "csv":
#                     g.to_csv(subdir / f"rsi_{sym}.csv", index=False)
#                 elif req.save_format.lower() in {"parquet", "pq"}:
#                     g.to_parquet(subdir / f"rsi_{sym}.parquet", index=False, engine="pyarrow")
#                 else:
#                     raise HTTPException(status_code=400, detail="Unsupported save_format; use 'csv' or 'parquet'.")
#             saved_to = str(out_path.parent)
#         else:
#             if req.save_format.lower() == "csv":
#                 result.to_csv(out_path, index=False)
#             elif req.save_format.lower() in {"parquet", "pq"}:
#                 result.to_parquet(out_path, index=False, engine="pyarrow")
#             else:
#                 raise HTTPException(status_code=400, detail="Unsupported save_format; use 'csv' or 'parquet'.")
#             saved_to = str(out_path)

#     # Sample preview (dates en string pour JSON)
#     sample = []
#     if req.sample_rows:
#         sample_df = result.head(req.sample_rows).copy()
#         if not sample_df.empty and pd.api.types.is_datetime64_any_dtype(sample_df["date"]):
#             sample_df["date"] = sample_df["date"].dt.tz_localize(None).astype(str)
#         sample = sample_df.to_dict(orient="records")

#     return RSIResponse(
#         status="ok",
#         count=len(result),
#         saved_to=saved_to,
#         columns=result.columns.tolist(),
#         sample=sample,
#     )




# src/mcp_rsi_server.py
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

from pathlib import Path
from typing import List, Optional, Dict, Any

import pandas as pd
from fastmcp import FastMCP

try:
    # quand on lance en package: python -m src.mcp_rsi_server
    from .ta_indicators import compute_rsi_wilder
except ImportError:
    # quand on lance par chemin: fastmcp run .\src\mcp_rsi_server.py
    from ta_indicators import compute_rsi_wilder

mcp = FastMCP("RSI Tools")

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
    price_col: str = "adj_close",          # or "close"
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

    # Compute RSI
    out = compute_rsi_wilder(df, price_col=price_col, window=window)
    rsi_col = f"rsi{window}"
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
        head = result.head(sample_rows).copy()
        if not head.empty and pd.api.types.is_datetime64_any_dtype(head["date"]):
            head["date"] = head["date"].dt.tz_localize(None).astype(str)
        sample = head.to_dict(orient="records")

    return {
        "status": "ok",
        "count": int(len(result)),
        "saved_to": saved_to,
        "columns": ["symbol", "date", rsi_col],
        "sample": sample,
    }

if __name__ == "__main__":
    # Default: stdio transport (works with MCP hosts/clients).
    # You can also run the handy dev inspector: `fastmcp dev src/mcp_rsi_server.py`
    mcp.run()
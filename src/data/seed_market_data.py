from __future__ import annotations

import argparse
import sys
from pathlib import Path

_SRC = str(Path(__file__).resolve().parent.parent)
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

import pandas as pd

from data.yfinance_retriever import YFinanceRetriever
from data.duckdb_csv_storage import DuckDbCsvStorage

CAC40_TICKERS = [
    "AI.PA",     # Air Liquide
    "AIR.PA",    # Airbus
    "ALO.PA",    # Alstom
    "MT.AS",     # ArcelorMittal
    "CS.PA",     # AXA
    "BNP.PA",    # BNP Paribas
    "EN.PA",     # Bouygues
    "CAP.PA",    # Capgemini
    "CA.PA",     # Carrefour
    "ACA.PA",    # Crédit Agricole
    "BN.PA",     # Danone
    "DSY.PA",    # Dassault Systèmes
    "ENGI.PA",   # Engie
    "EL.PA",     # EssilorLuxottica
    "ERF.PA",    # Eurofins Scientific
    "RMS.PA",    # Hermès
    "KER.PA",    # Kering
    "LR.PA",     # Legrand
    "OR.PA",     # L'Oréal
    "MC.PA",     # LVMH
    "ML.PA",     # Michelin
    "ORA.PA",    # Orange
    "RI.PA",     # Pernod Ricard
    "PUB.PA",    # Publicis
    "SAF.PA",    # Safran
    "SGO.PA",    # Saint-Gobain
    "SAN.PA",    # Sanofi
    "SU.PA",     # Schneider Electric
    "GLE.PA",    # Société Générale
    "STLAP.PA",  # Stellantis
    "STM.PA",    # STMicroelectronics
    "TEP.PA",    # Teleperformance
    "HO.PA",     # Thales
    "TTE.PA",    # TotalEnergies
    "URW.AS",    # Unibail-Rodamco-Westfield
    "VIE.PA",    # Veolia
    "DG.PA",     # Vinci
    "VIV.PA",    # Vivendi
    "WLN.PA",    # Worldline
]

BATCH_SIZE = 10


def main():
    parser = argparse.ArgumentParser(description="Seed market data from Yahoo Finance")
    parser.add_argument(
        "--preset", choices=["cac40"], default="cac40",
        help="Preset ticker list (default: cac40)",
    )
    parser.add_argument(
        "--tickers", type=str, default=None,
        help="Comma-separated tickers (overrides --preset)",
    )
    parser.add_argument("--start", type=str, default="2016-01-01")
    parser.add_argument("--end", type=str, default=None)
    parser.add_argument("--interval", type=str, default="1d")
    parser.add_argument("--db-path", type=str, default="database/ohlcv.csv")
    args = parser.parse_args()

    if args.tickers:
        symbols = [s.strip() for s in args.tickers.split(",") if s.strip()]
    else:
        symbols = CAC40_TICKERS

    print(f"Fetching {len(symbols)} symbols from {args.start} ...")

    retriever = YFinanceRetriever()
    storage = DuckDbCsvStorage(db_path=args.db_path)

    all_dfs = []
    for i in range(0, len(symbols), BATCH_SIZE):
        batch = symbols[i : i + BATCH_SIZE]
        print(f"  Batch {i // BATCH_SIZE + 1}: {batch}")
        try:
            df = retriever.get_prices(batch, start=args.start, end=args.end, interval=args.interval)
            all_dfs.append(df)
            print(f"    -> {len(df)} rows")
        except Exception as e:
            print(f"    -> Error: {e}")

    if not all_dfs:
        print("No data fetched.")
        sys.exit(1)

    combined = pd.concat(all_dfs, ignore_index=True)
    saved = storage.save_prices(combined)

    summary = combined.groupby("symbol").size()
    print(f"\nSaved {len(combined)} rows for {len(summary)} symbols to {saved}")
    print(summary.to_string())


if __name__ == "__main__":
    main()

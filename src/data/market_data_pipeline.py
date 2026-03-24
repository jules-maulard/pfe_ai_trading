from __future__ import annotations

import argparse
from typing import List, Optional

from .config.settings import get_config
from .retrievers import CAC40_TICKERS, YFinanceRetriever
from .storage import BaseStorage
from ..utils import get_logger

logger = get_logger(__name__)


def run_pipeline(symbols: List[str], storage: BaseStorage) -> None:
    start_date = get_config().start_date
    logger.info("Starting pipeline for %d symbol(s): %s", len(symbols), symbols)
    logger.info("Fetching OHLCV data from %s to today", start_date)

    retriever = YFinanceRetriever()
    ohlcv = retriever.get_ohlcv(symbols, start=start_date)

    if ohlcv.empty:
        logger.warning("No OHLCV data fetched. Pipeline aborted.")
        return

    logger.info(
        "Fetched %d OHLCV rows covering %s → %s",
        len(ohlcv),
        ohlcv["date"].min() if "date" in ohlcv.columns else "?",
        ohlcv["date"].max() if "date" in ohlcv.columns else "?",
    )

    logger.info("Saving OHLCV data to storage...")
    storage.save_ohlcv(ohlcv)
    logger.info("OHLCV data saved successfully.")
    # storage.update_indicators(symbols=symbols)

def _build_storage(backend: Optional[str] = None) -> BaseStorage:
    cfg = get_config()
    backend = backend or cfg.storage
    logger.info(f"Using storage backend: {backend}")
    if backend == "csv":
        from .storage import CsvStorage
        return CsvStorage()
    if backend == "snowflake":
        from .storage import SnowflakeStorage
        return SnowflakeStorage()
    raise ValueError(f"Unknown storage backend: {backend!r}")


def load_cli_config() -> dict:
    p = argparse.ArgumentParser()
    p.add_argument('--backend', default=None, help='Override storage backend (csv or snowflake)')
    p.add_argument('--symbols', nargs='+', default=None, help='Override list of symbols to fetch')
    args = p.parse_args()
    return vars(args)


def get_params() -> dict:
    cli = load_cli_config()
    cfg = get_config()
    # Precedence: CLI arg > config (env/YAML)
    backend = cli['backend'] or cfg.storage
    symbols = cli['symbols'] or cfg.symbols or CAC40_TICKERS
    params = dict(backend=backend, symbols=symbols, start=cfg.start_date)
    logger.info(
        "Pipeline parameters — backend: %s | symbols (%d): %s | start: %s",
        params['backend'],
        len(params['symbols']),
        params['symbols'],
        params['start'],
    )
    return params

def main():
    params = get_params()
    storage = _build_storage(params.get('backend'))
    run_pipeline(symbols=params['symbols'], storage=storage)


if __name__ == "__main__":
    main()


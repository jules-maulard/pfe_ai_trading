from __future__ import annotations

import argparse
import os
from typing import List
from dotenv import load_dotenv
import yaml

from .retrievers import CAC40_TICKERS, YFinanceRetriever
from .storage import BaseStorage
from ..utils import get_logger

logger = get_logger(__name__)

START_DATE = "2016-01-01"

def run_pipeline(symbols: List[str], storage: BaseStorage) -> None:
    retriever = YFinanceRetriever()
    ohlcv = retriever.get_ohlcv(symbols, start=START_DATE)

    if ohlcv.empty:
        logger.warning("No OHLCV data fetched. Pipeline aborted.")
        return

    storage.save_ohlcv(ohlcv)
    storage.update_indicators(symbols=symbols)

def _build_storage() -> BaseStorage:
        load_dotenv()
        backend = os.environ.get("STORAGE_BACKEND", "csv").lower()
        logger.info(f"Using storage backend: {backend}")

        if backend == "csv":
            from .storage import CsvStorage
            return CsvStorage()
        if backend == "snowflake":
            from .storage import SnowflakeStorage
            return SnowflakeStorage()
        raise ValueError(f"Unknown STORAGE_BACKEND: {backend}")

def load_yaml_config(path: str) -> dict:
    with open(path) as f:
        return yaml.safe_load(f)

def load_cli_config() -> dict:
    p = argparse.ArgumentParser()
    p.add_argument('--config', default='retriever.yaml', help='Path to YAML config file')
    p.add_argument('--backend', default=None, help='Override storage backend (e.g. csv, snowflake)')
    p.add_argument('--symbols', nargs='+', default=None, help='Override list of symbols to fetch')
    args = p.parse_args()
    return vars(args)

def get_params() -> dict:
    cli_config = load_cli_config()
    yaml_cfg = load_yaml_config(cli_config['config'])

    # merge precedence: CLI > ENV > config
    backend = cli_config['backend'] or os.getenv('STORAGE_BACKEND') or yaml_cfg.get('storage')
    symbols = cli_config['symbols'] or yaml_cfg.get('symbols') or CAC40_TICKERS
    start = yaml_cfg.get('start_date')

    return dict(backend=backend, symbols=symbols, start=start) 

def main():
    params = get_params()
    storage = _build_storage()
    run_pipeline(symbols=params['symbols'], storage=storage)


if __name__ == "__main__":
    main()


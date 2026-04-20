from __future__ import annotations

import argparse
from datetime import datetime, timedelta
from typing import Dict, List, Optional

import pandas as pd

from .config.pipeline_config import PipelineConfig, load_profile
from .indicators import compute_indicators
from .retrievers import YFinanceRetriever, FundamentalRetriever
from .storage import BaseStorage
from ..utils import get_logger

logger = get_logger(__name__)

_DEFAULT_INCREMENTAL_FALLBACK = "2016-01-01"


def _build_storage(backend: str) -> BaseStorage:
    logger.info("Using storage backend: %s", backend)
    if backend == "csv":
        from .storage import CsvStorage
        return CsvStorage()
    if backend == "snowflake":
        from .storage import SnowflakeStorage
        return SnowflakeStorage()
    raise ValueError(f"Unknown storage backend: {backend!r}")


def _resolve_start_dates(
    config: PipelineConfig,
    symbols: List[str],
    storage: BaseStorage,
    table: str = "ohlcv",
) -> Dict[str, Optional[str]]:
    """Return a {symbol: start_date} mapping based on the dates mode."""
    mode = config.dates.mode

    if mode == "full":
        return {s: None for s in symbols}

    if mode == "fixed":
        return {s: config.dates.start_date for s in symbols}

    # incremental — query storage for last known date per symbol
    start_map: dict[str, Optional[str]] = {}
    for sym in symbols:
        last = storage.get_last_date(table, sym)
        if last:
            next_day = (
                datetime.strptime(str(last)[:10], "%Y-%m-%d") + timedelta(days=1)
            ).strftime("%Y-%m-%d")
            start_map[sym] = next_day
        else:
            start_map[sym] = config.dates.start_date or _DEFAULT_INCREMENTAL_FALLBACK
    return start_map


def _group_symbols_by_start(start_map: Dict[str, Optional[str]]) -> Dict[Optional[str], List[str]]:
    """Group symbols that share the same start_date for batch downloading."""
    groups: dict[Optional[str], list[str]] = {}
    for sym, dt in start_map.items():
        groups.setdefault(dt, []).append(sym)
    return groups


def _fetch_ohlcv(
    retriever: YFinanceRetriever,
    symbols: List[str],
    start_map: Dict[str, Optional[str]],
    end_date: Optional[str],
    storage: BaseStorage,
    force_insert: bool = False,
) -> None:
    groups = _group_symbols_by_start(start_map)
    for start, syms in groups.items():
        logger.info("Fetching OHLCV for %d symbol(s) from %s: %s", len(syms), start or "all-time", syms)
        ohlcv = retriever.get_ohlcv(syms, start=start or "2000-01-01", end=end_date)
        if ohlcv.empty:
            logger.warning("No OHLCV data returned for %s", syms)
            continue
        logger.info(
            "Fetched %d OHLCV rows (%s → %s)",
            len(ohlcv),
            ohlcv["date"].min() if "date" in ohlcv.columns else "?",
            ohlcv["date"].max() if "date" in ohlcv.columns else "?",
        )
        storage.save_ohlcv(ohlcv, force_insert=force_insert)
        logger.info("OHLCV saved.")


def _fetch_dividends(
    retriever: YFinanceRetriever,
    symbols: List[str],
    storage: BaseStorage,
) -> None:
    all_divs: list[pd.DataFrame] = []
    for sym in symbols:
        logger.info("Fetching dividends for %s", sym)
        df = retriever.get_dividends(sym)
        if not df.empty:
            all_divs.append(df)
    if all_divs:
        combined = pd.concat(all_divs, ignore_index=True)
        storage.save_dividend(combined)
        logger.info("Saved %d dividend rows.", len(combined))


def _fetch_asset_info(
    retriever: YFinanceRetriever,
    symbols: List[str],
    storage: BaseStorage,
) -> None:
    all_assets: list[pd.DataFrame] = []
    for sym in symbols:
        logger.info("Fetching asset info for %s", sym)
        df = retriever.get_asset_info(sym)
        if not df.empty:
            all_assets.append(df)
    if all_assets:
        combined = pd.concat(all_assets, ignore_index=True)
        storage.save_asset(combined)
        logger.info("Saved %d asset rows.", len(combined))


def _fetch_fundamentals(
    config: PipelineConfig,
    symbols: List[str],
    storage: BaseStorage,
) -> None:
    retriever = FundamentalRetriever()
    period = config.fundamentals.period
    statement_fetchers = {
        "income_statement": retriever.get_income_statement,
        "balance_sheet": retriever.get_balance_sheet,
        "cash_flow": retriever.get_cash_flow,
        "financial_ratios": retriever.get_financial_ratios,
    }
    for statement_type in config.fundamentals.statements:
        fetcher = statement_fetchers.get(statement_type)
        if not fetcher:
            logger.warning("Unknown fundamental statement type: %s", statement_type)
            continue
        logger.info("Fetching %s (%s) for %d symbol(s)", statement_type, period, len(symbols))
        df = fetcher(symbols, period=period)
        if df.empty:
            logger.warning("No %s data returned.", statement_type)
            continue
        storage.save_fundamental(df, statement_type)
        logger.info("Saved %d %s rows.", len(df), statement_type)


_INDICATOR_LOOKBACK_DAYS = 90


def _filter_new_indicator_rows(
    result: pd.DataFrame,
    last_dates: Dict[str, str],
) -> pd.DataFrame:
    df = result.copy()
    df["date"] = pd.to_datetime(df["date"]).dt.date
    df["_last_date"] = pd.to_datetime(df["symbol"].map(last_dates), errors="coerce").dt.date
    mask = df["_last_date"].isna() | (df["date"] > df["_last_date"])
    return df[mask].drop(columns=["_last_date"]).reset_index(drop=True)


def _compute_and_save_indicators(
    config: PipelineConfig,
    symbols: List[str],
    storage: BaseStorage,
) -> None:
    indicator_symbols = config.indicators.symbols or symbols
    logger.info(
        "Computing indicators %s for %d symbol(s) (dates.mode=%s)",
        config.indicators.compute, len(indicator_symbols), config.dates.mode,
    )

    for name in config.indicators.compute:
        last_indicator_dates = storage.get_last_indicator_dates(name, indicator_symbols)
        last_ohlcv_dates = storage.get_last_dates("ohlcv", indicator_symbols)

        symbols_to_update = [
            sym for sym in indicator_symbols
            if last_ohlcv_dates.get(sym) and (
                sym not in last_indicator_dates
                or last_ohlcv_dates[sym][:10] > last_indicator_dates[sym][:10]
            )
        ]

        if not symbols_to_update:
            logger.info("Indicator '%s': all symbols are up-to-date, skipping.", name)
            continue

        logger.info("Indicator '%s': %d symbol(s) need update: %s", name, len(symbols_to_update), symbols_to_update)

        if config.dates.mode == "incremental":
            earliest_last = min(
                datetime.strptime(last_indicator_dates[sym][:10], "%Y-%m-%d")
                for sym in symbols_to_update
                if sym in last_indicator_dates
            ) if any(sym in last_indicator_dates for sym in symbols_to_update) else None
            ohlcv_start = (
                (earliest_last - timedelta(days=_INDICATOR_LOOKBACK_DAYS)).strftime("%Y-%m-%d")
                if earliest_last else None
            )
        else:
            ohlcv_start = None

        ohlcv = storage.load_ohlcv(symbols=symbols_to_update, start=ohlcv_start)

        if ohlcv.empty:
            logger.warning("No OHLCV data in storage for indicator '%s'.", name)
            continue

        result = compute_indicators(ohlcv, [name])
        if result.empty:
            logger.warning("Indicator '%s' computation produced no rows.", name)
            continue

        if last_indicator_dates:
            result = _filter_new_indicator_rows(result, last_indicator_dates)

        if result.empty:
            logger.info("Indicator '%s': no new rows to insert.", name)
            continue

        storage.save_indicator(result, name)


def run_pipeline(config: PipelineConfig, storage: BaseStorage) -> None:
    # Resolve symbols: from storage or from config
    if config.symbols.source == "storage":
        symbols = storage.list_symbols()
        if not symbols:
            logger.warning("No symbols found in storage. Pipeline aborted.")
            return
        logger.info("Resolved %d symbol(s) from storage.", len(symbols))
    else:
        symbols = config.resolve_symbols()

    logger.info(
        "Pipeline '%s' — %d symbol(s), dates.mode=%s, storage=%s",
        config.name, len(symbols), config.dates.mode, config.storage,
    )

    has_fetch = any([
        config.fetch.ohlcv,
        config.fetch.dividends,
        config.fetch.asset_info,
        config.fetch.fundamentals,
    ])

    if has_fetch:
        retriever = YFinanceRetriever()
        start_map = _resolve_start_dates(config, symbols, storage)
        end_date = config.dates.end_date or datetime.now().date().strftime("%Y-%m-%d")

        if config.fetch.ohlcv:
            _fetch_ohlcv(retriever, symbols, start_map, end_date, storage, force_insert=(config.dates.mode == "fixed"))
        if config.fetch.dividends:
            _fetch_dividends(retriever, symbols, storage)
        if config.fetch.asset_info:
            _fetch_asset_info(retriever, symbols, storage)
        if config.fetch.fundamentals:
            _fetch_fundamentals(config, symbols, storage)
    else:
        logger.info("No data fetch configured — skipping retrieval.")

    if config.indicators.enabled and config.indicators.compute:
        _compute_and_save_indicators(config, symbols, storage)
    else:
        logger.info("Indicator computation not enabled — skipping.")

    logger.info("Pipeline '%s' completed.", config.name)


def main():
    parser = argparse.ArgumentParser(
        description="Market data pipeline — run with a named profile",
    )
    parser.add_argument(
        "--profile", "-p",
        required=True,
        help="Name of the pipeline profile (e.g. daily, add_symbols, catchup, compute_indicators)",
    )
    args = parser.parse_args()

    config = load_profile(args.profile)
    storage = _build_storage(config.storage)
    run_pipeline(config, storage)


if __name__ == "__main__":
    main()


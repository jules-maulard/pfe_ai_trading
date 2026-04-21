import pytest
from pydantic import ValidationError

from src.data.config.pipeline_config import (
    DatesConfig,
    PipelineConfig,
    SymbolsConfig,
    IndicatorsConfig,
)


class TestDatesConfig:
    def test_default_mode_is_incremental(self):
        cfg = DatesConfig()
        assert cfg.mode == "incremental"

    def test_fixed_mode_requires_start_date(self):
        with pytest.raises(ValidationError, match="start_date is required"):
            DatesConfig(mode="fixed")

    def test_fixed_mode_with_start_date_is_valid(self):
        cfg = DatesConfig(mode="fixed", start_date="2024-01-01")
        assert cfg.start_date == "2024-01-01"

    def test_incremental_mode_without_start_date_is_valid(self):
        cfg = DatesConfig(mode="incremental")
        assert cfg.start_date is None

    def test_full_mode_without_start_date_is_valid(self):
        cfg = DatesConfig(mode="full")
        assert cfg.mode == "full"

    def test_end_date_defaults_to_none(self):
        cfg = DatesConfig()
        assert cfg.end_date is None


class TestSymbolsConfig:
    def test_default_source_is_list(self):
        cfg = SymbolsConfig()
        assert cfg.source == "list"

    def test_default_tickers_is_empty(self):
        cfg = SymbolsConfig()
        assert cfg.tickers == []

    def test_tickers_set_correctly(self):
        cfg = SymbolsConfig(tickers=["AAPL", "MSFT"])
        assert cfg.tickers == ["AAPL", "MSFT"]


class TestIndicatorsConfig:
    def test_disabled_by_default(self):
        cfg = IndicatorsConfig()
        assert cfg.enabled is False

    def test_compute_defaults_to_empty(self):
        cfg = IndicatorsConfig()
        assert cfg.compute == []


class TestPipelineConfig:
    def test_default_storage_is_csv(self):
        cfg = PipelineConfig()
        assert cfg.storage == "csv"

    def test_default_name(self):
        cfg = PipelineConfig()
        assert cfg.name == "default"

    def test_resolve_symbols_returns_tickers(self):
        cfg = PipelineConfig(symbols=SymbolsConfig(tickers=["AAPL", "GOOG"]))
        assert cfg.resolve_symbols() == ["AAPL", "GOOG"]

    def test_resolve_symbols_storage_source_raises(self):
        cfg = PipelineConfig(symbols=SymbolsConfig(source="storage"))
        with pytest.raises(RuntimeError, match="source='storage'"):
            cfg.resolve_symbols()

    def test_resolve_symbols_empty_list(self):
        cfg = PipelineConfig(symbols=SymbolsConfig(tickers=[]))
        assert cfg.resolve_symbols() == []

    def test_nested_dates_validation_propagates(self):
        with pytest.raises(ValidationError):
            PipelineConfig(dates={"mode": "fixed"})

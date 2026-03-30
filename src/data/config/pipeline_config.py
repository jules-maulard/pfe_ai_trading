from __future__ import annotations

from pathlib import Path
from typing import List, Literal, Optional

import yaml
from pydantic import BaseModel, model_validator

from ...utils import get_logger

logger = get_logger(__name__)

_PROFILES_DIR = Path(__file__).parent / "profiles"

SYMBOL_PRESETS: dict[str, list[str]] = {
    "cac40": [
        "AI.PA", "AIR.PA", "ALO.PA", "MT.AS", "CS.PA", "BNP.PA", "EN.PA",
        "CAP.PA", "CA.PA", "ACA.PA", "BN.PA", "DSY.PA", "ENGI.PA", "EL.PA",
        "ERF.PA", "RMS.PA", "KER.PA", "LR.PA", "OR.PA", "MC.PA", "ML.PA",
        "ORA.PA", "RI.PA", "PUB.PA", "SAF.PA", "SGO.PA", "SAN.PA", "SU.PA",
        "GLE.PA", "STLAP.PA", "STM.PA", "TEP.PA", "HO.PA", "TTE.PA",
        "URW.AS", "VIE.PA", "DG.PA", "VIV.PA", "WLN.PA",
    ],
}


class SymbolsConfig(BaseModel):
    source: Literal["list", "storage"] = "list"
    tickers: List[str] = []


class DatesConfig(BaseModel):
    mode: Literal["incremental", "fixed", "full"] = "incremental"
    start_date: Optional[str] = None
    end_date: Optional[str] = None

    @model_validator(mode="after")
    def _check_fixed_requires_start(self) -> "DatesConfig":
        if self.mode == "fixed" and not self.start_date:
            raise ValueError("dates.start_date is required when mode is 'fixed'")
        return self


class FetchConfig(BaseModel):
    ohlcv: bool = True
    dividends: bool = False
    asset_info: bool = False


class IndicatorsConfig(BaseModel):
    enabled: bool = False
    compute: List[str] = []
    symbols: Optional[List[str]] = None


class PipelineConfig(BaseModel):
    name: str = "default"
    symbols: SymbolsConfig = SymbolsConfig()
    dates: DatesConfig = DatesConfig()
    storage: Literal["csv", "snowflake"] = "csv"
    fetch: FetchConfig = FetchConfig()
    indicators: IndicatorsConfig = IndicatorsConfig()

    def resolve_symbols(self) -> List[str]:
        """Return the final list of symbols from explicit tickers.

        When source='storage', the pipeline itself must query storage
        and this method should not be called directly.
        """
        if self.symbols.source == "storage":
            raise RuntimeError(
                "Cannot resolve symbols locally when source='storage'. "
                "The pipeline must query the storage backend."
            )
        return list(self.symbols.tickers)


def load_profile(name: str) -> PipelineConfig:
    """Load a pipeline profile YAML by name from the profiles directory."""
    path = _PROFILES_DIR / f"{name}.yaml"
    if not path.exists():
        raise FileNotFoundError(
            f"Pipeline profile '{name}' not found at {path}. "
            f"Available profiles: {[p.stem for p in _PROFILES_DIR.glob('*.yaml')]}"
        )
    raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    raw.setdefault("name", name)
    logger.info("Loaded pipeline profile '%s' from %s", name, path)
    return PipelineConfig(**raw)

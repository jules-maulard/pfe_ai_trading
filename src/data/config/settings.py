from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Literal

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

_ROOT = Path(__file__).resolve().parents[3]
_ENV_FILE = _ROOT / ".env"


class AppConfig(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=str(_ENV_FILE),
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    symbols: list[str] = Field(default_factory=list)
    start_date: str = "2016-01-01"
    storage: Literal["csv", "snowflake"] = "snowflake"

    snowflake_account: str | None = None
    snowflake_user: str | None = None
    snowflake_password: str | None = None
    snowflake_role: str | None = None
    snowflake_database: str = "TRADING_AI"
    snowflake_schema: str = "MARKET_DATA"
    snowflake_warehouse: str = "COMPUTE_WH"

    @model_validator(mode="before")
    @classmethod
    def _load_yaml_defaults(cls, data: dict) -> dict:
        if "storage_backend" in data and "storage" not in data:
            data["storage"] = data.pop("storage_backend")
        return data

    @model_validator(mode="after")
    def _require_snowflake_credentials(self) -> "AppConfig":
        if self.storage == "snowflake":
            missing = [
                var
                for var, val in {
                    "SNOWFLAKE_ACCOUNT": self.snowflake_account,
                    "SNOWFLAKE_USER": self.snowflake_user,
                    "SNOWFLAKE_PASSWORD": self.snowflake_password,
                }.items()
                if not val
            ]
            if missing:
                raise ValueError(
                    "Storage backend is 'snowflake' but the following required "
                    f"environment variables are not set: {', '.join(missing)}"
                )
        return self


@lru_cache(maxsize=1)
def get_config() -> AppConfig:
    return AppConfig()


def get_storage():
    """Return a storage instance based on STORAGE_BACKEND in .env.

    Used by MCP servers and any read-only component that should not
    hard-code a backend. The pipeline uses its own profile storage field.
    """
    from ..storage import CsvStorage, SnowflakeStorage

    backend = get_config().storage
    if backend == "snowflake":
        return SnowflakeStorage()
    return CsvStorage()

from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    app_name: str = "Stock Volatility API"

    # yfinance defaults
    default_period: str = "5y"
    default_interval: str = "1d"

    # caching
    price_cache_ttl_seconds: int = 900


settings = Settings()

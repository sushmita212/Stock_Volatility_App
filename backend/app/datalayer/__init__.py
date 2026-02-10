"""Data access layer.

Contains:
- Provider clients (e.g. Alpha Vantage)
- Local persistence (CSV/metadata store)
- Refresh/staleness policy helpers

Keeping this as a package lets the FastAPI app import with absolute imports like
`from backend.app.datalayer import ...`.
"""

from .alphavantage import ProviderError, fetch_time_series_daily, normalize_daily_ohlcv
from .refresh_policy import is_stale_daily
from .storage import StorePaths, load_metadata, persist_symbol_rows, read_bars_tail

__all__ = [
    "ProviderError",
    "fetch_time_series_daily",
    "normalize_daily_ohlcv",
    "is_stale_daily",
    "StorePaths",
    "load_metadata",
    "persist_symbol_rows",
    "read_bars_tail",
]

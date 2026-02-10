from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import requests


@dataclass(frozen=True)
class ProviderError(Exception):
    """Raised when the data provider cannot return usable data."""

    status_code: int
    message: str

    def __str__(self) -> str:  # pragma: no cover
        return self.message


def fetch_time_series_daily(
    symbol: str,
    api_key: str,
    outputsize: str = "compact",
    timeout_s: int = 30,
) -> dict[str, dict[str, str]]:
    """Fetch TIME_SERIES_DAILY from Alpha Vantage.

    Returns:
        Mapping of date string (YYYY-MM-DD) -> AV row payload.

    Raises:
        ProviderError: rate limit, invalid parameters, premium restriction, or unexpected payload.
    """

    url = "https://www.alphavantage.co/query"
    params = {
        "function": "TIME_SERIES_DAILY",
        "symbol": symbol,
        "outputsize": outputsize,
        "datatype": "json",
        "apikey": api_key,
    }

    r = requests.get(url, params=params, timeout=timeout_s)

    try:
        data: dict[str, Any] = r.json()
    except ValueError as e:
        raise ProviderError(status_code=502, message=f"Alpha Vantage returned non-JSON (status {r.status_code}).") from e

    # Alpha Vantage often returns errors as JSON with HTTP 200
    if "Note" in data:
        raise ProviderError(status_code=429, message=f"Alpha Vantage rate limit: {data['Note']}")
    if "Information" in data:
        raise ProviderError(status_code=403, message=f"Alpha Vantage info: {data['Information']}")
    if "Error Message" in data:
        raise ProviderError(status_code=400, message=f"Alpha Vantage error: {data['Error Message']}")

    ts_key = "Time Series (Daily)"
    ts = data.get(ts_key)
    if not isinstance(ts, dict) or not ts:
        raise ProviderError(status_code=502, message=f"Unexpected Alpha Vantage response keys: {list(data.keys())}")

    return ts  # type: ignore[return-value]


def normalize_daily_ohlcv(ts: dict[str, dict[str, str]]) -> list[dict[str, str]]:
    """Normalize AV TIME_SERIES_DAILY payload to canonical OHLCV rows.

    Output row keys:
        date, open, high, low, close, volume

    All values are strings for easy CSV persistence.
    """

    out: list[dict[str, str]] = []
    for d in sorted(ts.keys()):
        row = ts[d]
        out.append(
            {
                "date": d,
                "open": str(row.get("1. open", "")),
                "high": str(row.get("2. high", "")),
                "low": str(row.get("3. low", "")),
                "close": str(row.get("4. close", "")),
                "volume": str(row.get("5. volume", "")),
            }
        )
    return out

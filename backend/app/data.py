from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, Optional

import numpy as np
import pandas as pd
import yfinance as yf
from cachetools import TTLCache


@dataclass(frozen=True)
class PriceQuery:
    symbol: str
    period: str
    interval: str
    auto_adjust: bool = True


_price_cache: TTLCache = TTLCache(maxsize=256, ttl=900)


def set_cache_ttl(seconds: int) -> None:
    global _price_cache
    _price_cache = TTLCache(maxsize=256, ttl=int(seconds))


def fetch_ohlcv(query: PriceQuery) -> pd.DataFrame:
    """Fetch OHLCV from yfinance.

    Returns a DataFrame with columns: Open, High, Low, Close, Volume and a DatetimeIndex.
    """

    key = (query.symbol.upper(), query.period, query.interval, query.auto_adjust)
    cached = _price_cache.get(key)
    if cached is not None:
        return cached.copy()

    df = yf.download(
        tickers=query.symbol,
        period=query.period,
        interval=query.interval,
        auto_adjust=query.auto_adjust,
        progress=False,
        threads=False,
    )

    if df is None or df.empty:
        raise ValueError(f"No data returned for symbol={query.symbol}")

    # yfinance sometimes returns a column MultiIndex for multiple tickers; we only request one.
    if isinstance(df.columns, pd.MultiIndex):
        df = df.xs(query.symbol, axis=1, level=0)

    df = df.rename_axis("date").sort_index()
    df = df[[c for c in ["Open", "High", "Low", "Close", "Volume"] if c in df.columns]]

    _price_cache[key] = df
    return df.copy()


def log_returns(close: pd.Series) -> pd.Series:
    close = close.astype(float)
    r = np.log(close / close.shift(1))
    return r.dropna()


def realized_vol_parkinson(ohlc: pd.DataFrame, window: int = 10, trading_days: int = 252) -> pd.Series:
    """Parkinson realized volatility estimator, annualized.

    sigma^2 = (1/(4 ln2)) * (ln(H/L))^2
    then rolling mean, then annualize.
    """

    high = ohlc["High"].astype(float)
    low = ohlc["Low"].astype(float)
    rs = (1.0 / (4.0 * np.log(2.0))) * (np.log(high / low) ** 2)
    # daily variance estimate -> rolling average -> annualized std
    var = rs.rolling(window).mean() * trading_days
    return np.sqrt(var).dropna().rename("realized_vol")

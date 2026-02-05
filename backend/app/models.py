from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

import numpy as np
import pandas as pd
from arch import arch_model


@dataclass(frozen=True)
class ForecastResult:
    model: Literal["garch11", "histvol"]
    horizon_days: int
    annualized: bool
    forecast_vol: float


def forecast_hist_vol(returns: pd.Series, window: int = 20, trading_days: int = 252) -> float:
    """Rolling historical volatility baseline, annualized."""
    r = returns.dropna().astype(float)
    if len(r) < window + 1:
        raise ValueError(f"Need at least {window+1} returns, got {len(r)}")
    daily_std = r.tail(window).std(ddof=1)
    return float(daily_std * np.sqrt(trading_days))


def forecast_garch11(returns: pd.Series, horizon_days: int = 1, trading_days: int = 252) -> float:
    """Fit GARCH(1,1) on returns and forecast next h-day conditional volatility.

    Returns annualized volatility.

    Note: arch expects returns typically in percent scale; we scale by 100 for stability.
    """

    r = returns.dropna().astype(float)
    if len(r) < 100:
        raise ValueError("Need at least 100 return observations for GARCH")

    am = arch_model(r * 100.0, mean="Constant", vol="GARCH", p=1, q=1, dist="normal")
    res = am.fit(disp="off")
    # variance forecasts (percent^2)
    f = res.forecast(horizon=horizon_days, reindex=False)
    # take last row (t+1..t+h)
    var_h = f.variance.values[-1, -1]

    # convert percent^2 to raw-return variance
    daily_vol = np.sqrt(var_h) / 100.0
    annual_vol = float(daily_vol * np.sqrt(trading_days))
    return annual_vol

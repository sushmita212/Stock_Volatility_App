from __future__ import annotations

from typing import Literal, Optional

from pydantic import BaseModel, Field


class HealthResponse(BaseModel):
    status: str = "ok"


class ForecastRequest(BaseModel):
    symbol: str = Field(..., examples=["AAPL", "SPY"])
    model: Literal["garch11", "histvol"] = "garch11"

    # data
    period: str = "5y"
    interval: str = "1d"
    auto_adjust: bool = True

    # forecast
    horizon_days: int = Field(1, ge=1, le=30)

    # realized vol (for charting)
    realized_window: int = Field(10, ge=2, le=252)


class ForecastResponse(BaseModel):
    symbol: str
    model: Literal["garch11", "histvol"]
    horizon_days: int
    annualized: bool = True

    # point forecast (annualized vol)
    forecast_vol: float

    # most recent realized vol point (annualized)
    last_realized_vol: Optional[float] = None
    last_price_date: Optional[str] = None

    # small payload for dashboard plotting
    realized_vol_tail: list[dict] = Field(default_factory=list, description="List of {date, realized_vol}")
    close_tail: list[dict] = Field(default_factory=list, description="List of {date, close}")

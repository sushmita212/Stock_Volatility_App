from __future__ import annotations

from fastapi import FastAPI, HTTPException

from .config import settings
from .data import PriceQuery, fetch_ohlcv, log_returns, realized_vol_parkinson, set_cache_ttl
from .models import forecast_garch11, forecast_hist_vol
from .schemas import ForecastRequest, ForecastResponse, HealthResponse


def create_app() -> FastAPI:
    app = FastAPI(title=settings.app_name)

    set_cache_ttl(settings.price_cache_ttl_seconds)

    @app.get("/health", response_model=HealthResponse)
    def health() -> HealthResponse:
        return HealthResponse()

    @app.post("/forecast", response_model=ForecastResponse)
    def forecast(req: ForecastRequest) -> ForecastResponse:
        try:
            ohlc = fetch_ohlcv(
                PriceQuery(
                    symbol=req.symbol,
                    period=req.period,
                    interval=req.interval,
                    auto_adjust=req.auto_adjust,
                )
            )

            if "Close" not in ohlc.columns:
                raise ValueError("Missing Close column")

            r = log_returns(ohlc["Close"])

            if req.model == "garch11":
                fvol = forecast_garch11(r, horizon_days=req.horizon_days)
            else:
                # horizon_days is ignored for histvol baseline (point estimate)
                fvol = forecast_hist_vol(r)

            realized = None
            last_date = None
            realized_tail = []
            close_tail = []

            if {"High", "Low"}.issubset(set(ohlc.columns)):
                rv = realized_vol_parkinson(ohlc, window=req.realized_window)
                if not rv.empty:
                    realized = float(rv.iloc[-1])
                    realized_tail = [
                        {"date": idx.strftime("%Y-%m-%d"), "realized_vol": float(v)}
                        for idx, v in rv.tail(120).items()
                    ]

            if not ohlc.empty:
                last_date = ohlc.index[-1].strftime("%Y-%m-%d")
                close_tail = [
                    {"date": idx.strftime("%Y-%m-%d"), "close": float(v)}
                    for idx, v in ohlc["Close"].tail(120).items()
                ]

            return ForecastResponse(
                symbol=req.symbol.upper(),
                model=req.model,
                horizon_days=req.horizon_days,
                forecast_vol=float(fvol),
                last_realized_vol=realized,
                last_price_date=last_date,
                realized_vol_tail=realized_tail,
                close_tail=close_tail,
            )
        except Exception as e:
            raise HTTPException(status_code=400, detail=str(e))

    return app


app = create_app()

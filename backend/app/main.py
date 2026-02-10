from fastapi import FastAPI, HTTPException
import os
from pathlib import Path
from datetime import datetime, timezone

from dotenv import load_dotenv
from pydantic import BaseModel

from backend.app.datalayer.alphavantage import ProviderError, fetch_time_series_daily, normalize_daily_ohlcv
from backend.app.datalayer.storage import StorePaths, load_metadata, read_bars_tail, persist_symbol_rows
from backend.app.datalayer.refresh_policy import is_stale_daily

load_dotenv()
app = FastAPI()

# Resolve repo root -> data folder (works regardless of where uvicorn is launched)
REPO_ROOT = Path(__file__).resolve().parents[2]
STORE_PATHS = StorePaths(repo_root=REPO_ROOT)


class PriceBar(BaseModel):
    """Single daily OHLCV price bar for one symbol."""

    date: str  # YYYY-MM-DD
    open: float
    high: float
    low: float
    close: float
    volume: float


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@app.get("/")
def read_root():
    return {
        "service": "Stock Volatility API",
        "docs": "/docs",
        "openapi": "/openapi.json",
        "endpoints": {
            "health": "/health",
            "prices": "/prices?symbol=MSFT&limit=200",
        },
    }


@app.get("/health")
def health():
    return {
        "status": "ok",
        "time_utc": _utc_now_iso(),
        "repo_root": str(REPO_ROOT),
        "data_dir": str(STORE_PATHS.data_dir),
        "stock_data_dir_exists": STORE_PATHS.stock_data_dir.exists(),
        "metadata_exists": STORE_PATHS.metadata_path.exists(),
    }


@app.get("/prices", response_model=list[PriceBar])
def get_prices(symbol: str, outputsize: str = "compact", limit: int = 200):
    if limit < 1:
        raise HTTPException(status_code=400, detail="limit must be >= 1")
    if limit > 5000:
        raise HTTPException(status_code=400, detail="limit too large (max 5000)")

    meta = load_metadata(STORE_PATHS)

    # If local is fresh, serve from disk without hitting Alpha Vantage
    if not is_stale_daily(symbol, meta):
        local_rows = read_bars_tail(STORE_PATHS, symbol, limit=limit)
        if local_rows:
            return [
                PriceBar(
                    date=r["date"],
                    open=float(r["open"]),
                    high=float(r["high"]),
                    low=float(r["low"]),
                    close=float(r["close"]),
                    volume=float(r["volume"]),
                )
                for r in local_rows
            ]

    api_key = os.getenv("AV_API_KEY")
    if not api_key:
        raise HTTPException(status_code=500, detail="Missing AV_API_KEY environment variable.")

    try:
        ts = fetch_time_series_daily(symbol=symbol, api_key=api_key, outputsize=outputsize)
        rows = normalize_daily_ohlcv(ts)
        persist_symbol_rows(STORE_PATHS, symbol=symbol, new_rows=rows, source="alphavantage")
    except ProviderError as e:
        raise HTTPException(status_code=e.status_code, detail=e.message)

    local_rows = read_bars_tail(STORE_PATHS, symbol, limit=limit)
    if local_rows:
        return [
            PriceBar(
                date=r["date"],
                open=float(r["open"]),
                high=float(r["high"]),
                low=float(r["low"]),
                close=float(r["close"]),
                volume=float(r["volume"]),
            )
            for r in local_rows
        ]

    # Fallback: should be rare (e.g., write failure). Return directly-fetched tail.
    return [
        PriceBar(
            date=r["date"],
            open=float(r.get("open") or 0.0),
            high=float(r.get("high") or 0.0),
            low=float(r.get("low") or 0.0),
            close=float(r.get("close") or 0.0),
            volume=float(r.get("volume") or 0.0),
        )
        for r in rows[-limit:]
    ]
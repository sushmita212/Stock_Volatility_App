from fastapi import FastAPI, HTTPException
import os
import requests
from dotenv import load_dotenv
from pydantic import BaseModel

load_dotenv()

app = FastAPI()


class PriceBar(BaseModel):
    date: str
    open: float
    high: float
    low: float
    close: float
    volume: float


@app.get("/")
def read_root():
    return {"Hello": "World"}


@app.get("/prices", response_model=list[PriceBar])
def get_prices(symbol: str, outputsize: str = "compact"):
    api_key = os.getenv("AV_API_KEY")
    if not api_key:
        raise HTTPException(status_code=500, detail="Missing AV_API_KEY environment variable.")

    url = "https://www.alphavantage.co/query"
    params = {
        "function": "TIME_SERIES_DAILY",
        "symbol": symbol,
        "outputsize": outputsize,  # "compact" or "full"
        "datatype": "json",
        "apikey": api_key,
    }

    r = requests.get(url, params=params, timeout=30)
    data = r.json()

    # Alpha Vantage sends errors as JSON with HTTP 200
    if "Note" in data:
        raise HTTPException(status_code=429, detail=f"Alpha Vantage rate limit: {data['Note']}")
    if "Error Message" in data:
        raise HTTPException(status_code=400, detail=f"Alpha Vantage error: {data['Error Message']}")

    ts_key = "Time Series (Daily)"
    if ts_key not in data:
        raise HTTPException(status_code=502, detail=f"Unexpected Alpha Vantage response keys: {list(data.keys())}")

    ts = data[ts_key]  # dict: { "YYYY-MM-DD": { "1. open": "...", ... } }

    # Convert to list[PriceBar], sorted oldest->newest
    out: list[PriceBar] = []
    for d in sorted(ts.keys()):
        row = ts[d]
        out.append(
            PriceBar(
                date=d,
                open=float(row["1. open"]),
                high=float(row["2. high"]),
                low=float(row["3. low"]),
                close=float(row["4. close"]),
                volume=float(row["5. volume"]),
            )
        )

    return out
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import yfinance as yf


app = FastAPI()

class PriceBar(BaseModel):
    date: str
    open: float | None = None
    high: float | None = None
    low: float | None = None
    close: float | None = None
    volume: float | None = None


@app.get("/")
def read_root():
    return {"Hello": "World"}



@app.get("/prices", response_model=list[PriceBar])
def get_prices(symbol: str, period: str = "1y", interval: str = "1d"):
    df = yf.download(
        tickers=symbol,
        period=period,
        interval=interval,
        auto_adjust=True,
        progress=False,
        threads=False,
    )

    if df is None or df.empty:
        raise HTTPException(
            status_code=400,
            detail="No data returned. Check symbol/period/interval combination.",
        )

    # yfinance returns the timestamp as the index
    df = df.reset_index()

    # Normalize column names from yfinance output
    # Typical columns: Date/Datetime, Open, High, Low, Close, Volume
    date_col = "Date" if "Date" in df.columns else ("Datetime" if "Datetime" in df.columns else None)
    if date_col is None:
        raise HTTPException(status_code=500, detail="Unexpected yfinance output: missing Date/Datetime column.")

    df = df.rename(
        columns={
            date_col: "date",
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Volume": "volume",
        }
    )

    # Return last N rows to keep response small (adjust as needed)
    df = df.tail(20)

    # Convert timestamps to ISO strings for JSON
    df["date"] = df["date"].astype("datetime64[ns]").dt.strftime("%Y-%m-%dT%H:%M:%S")

    records = df[["date", "open", "high", "low", "close", "volume"]].to_dict(orient="records")
    return records
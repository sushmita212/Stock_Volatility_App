from fastapi import FastAPI, HTTPException
import os
import json
import csv
from pathlib import Path
from datetime import datetime, timezone
import requests
from dotenv import load_dotenv
from pydantic import BaseModel

load_dotenv()

app = FastAPI()

# Resolve repo root -> data folder (works regardless of where uvicorn is launched)
REPO_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = REPO_ROOT / "data"
STOCK_DATA_DIR = DATA_DIR / "stock_data"
METADATA_PATH = DATA_DIR / "metadata.json"


class PriceBar(BaseModel):
    """Single daily OHLCV price bar for one symbol.

    Notes:
        - `date` is expected to be an ISO date string `YYYY-MM-DD`.
        - All numeric fields are floats for simplicity; `volume` is commonly an int,
          but providers may return it as a string and we cast to float.
    """

    date: str  # YYYY-MM-DD
    open: float
    high: float
    low: float
    close: float
    volume: float


def _utc_now_iso() -> str:
    """Return the current UTC timestamp as an ISO-8601 string.

    Used for metadata timestamps such as `last_refresh_success_at`.
    """
    return datetime.now(timezone.utc).isoformat()

def _local_now_human() -> str:
    """Return the current local timestamp as a human-friendly string.

    Format:
        YYYY-MM-DD HH:MM:SS TZ

    Example:
        2026-02-09 14:04:17 PST

    Notes:
        - Uses the machine's local timezone.
        - Intended for display/debuggability in metadata.json.
    """
    return datetime.now().astimezone().strftime("%Y-%m-%d %H:%M:%S %Z")

def _load_metadata() -> dict:
    """Load the metadata JSON file from disk.

    Returns:
        A dict shaped like:
        {
          "version": 1,
          "tickers": {
              "MSFT": {
                  "last_date": "2026-02-09",
                  "rows": 1234,
                  "last_refresh_success_at": "...",
                  "status": "ok",
                  "source": "alphavantage"
              },
              ...
          }
        }

    Behavior:
        - If `data/metadata.json` does not exist, returns a default structure.
    """
    if not METADATA_PATH.exists():
        return {"version": 1, "tickers": {}}
    with METADATA_PATH.open("r", encoding="utf-8") as f:
        return json.load(f)


def _save_metadata(meta: dict) -> None:
    """Persist metadata JSON to disk atomically.

    Args:
        meta: Metadata dict to write to `data/metadata.json`.

    Notes:
        Uses a write-to-temp-then-rename strategy to reduce the chance of a
        partially-written metadata file if the process is interrupted.
    """
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    tmp = METADATA_PATH.with_suffix(".json.tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(meta, f, indent=2, sort_keys=True)
    tmp.replace(METADATA_PATH)


def _read_existing_rows(csv_path: Path) -> dict[str, dict]:
    """Read an existing OHLCV CSV file into a dict keyed by date.

    Args:
        csv_path: Path to the symbol CSV (e.g., `data/stock_data/MSFT.csv`).

    Returns:
        Dict mapping `date` (YYYY-MM-DD) -> row dict containing canonical fields:
        `date, open, high, low, close, volume`.

    Behavior:
        - If the CSV does not exist, returns an empty dict.
        - If duplicate dates exist in the file, the last one read wins.
    """
    if not csv_path.exists():
        return {}
    with csv_path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        out: dict[str, dict] = {}
        for row in reader:
            # Expect canonical columns
            out[row["date"]] = row
        return out


def _write_rows(csv_path: Path, rows_by_date: dict[str, dict]) -> None:
    """Write canonical OHLCV rows to a CSV file atomically.

    Args:
        csv_path: Destination CSV path for a symbol.
        rows_by_date: Dict keyed by date -> row dict. Each row dict should contain
            the canonical keys: `date, open, high, low, close, volume`.

    Notes:
        - Writes rows sorted by date ascending.
        - Uses a write-to-temp-then-rename strategy to reduce file corruption risk.
    """
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = ["date", "open", "high", "low", "close", "volume"]
    tmp = csv_path.with_suffix(".csv.tmp")
    with tmp.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for d in sorted(rows_by_date.keys()):
            writer.writerow(rows_by_date[d])
    tmp.replace(csv_path)


def _persist_symbol_bars(symbol: str, bars: list[PriceBar]) -> None:
    """Merge fetched bars into the local CSV store and update metadata.

    This is the persistence layer for daily OHLCV data.

    Args:
        symbol: Ticker symbol (e.g., "MSFT"). Stored uppercased.
        bars: List of fetched `PriceBar` objects (typically from Alpha Vantage).

    Behavior:
        - Reads the current `data/stock_data/{SYMBOL}.csv` if it exists.
        - Merges by `date` (new values overwrite old values for the same date).
        - Writes the merged result back to CSV.
        - Updates `data/metadata.json` for the symbol with last_date, row count,
          and refresh timestamp.

    Notes:
        - Currently stores numbers as formatted strings in the CSV for consistency.
        - This function does not implement "lazy refresh" staleness checks; it
          only persists what it is given.
    """
    csv_path = STOCK_DATA_DIR / f"{symbol.upper()}.csv"

    existing = _read_existing_rows(csv_path)

    # Merge: new overwrites old for same date
    for b in bars:
        existing[b.date] = {
            "date": b.date,
            "open": f"{b.open:.6f}",
            "high": f"{b.high:.6f}",
            "low": f"{b.low:.6f}",
            "close": f"{b.close:.6f}",
            "volume": f"{b.volume:.0f}",
        }

    _write_rows(csv_path, existing)

    # Update metadata
    meta = _load_metadata()
    tickers = meta.setdefault("tickers", {})
    sym = symbol.upper()
    last_date = max(existing.keys()) if existing else None
    tickers[sym] = {
        "last_date": last_date,
        "rows": len(existing),
        # Keep ISO UTC for machines and add local time for humans
        "last_refresh_success_at": _utc_now_iso(),
        "last_refresh_success_at_local": _local_now_human(),
        "status": "ok",
        "source": "alphavantage",
    }
    _save_metadata(meta)


@app.get("/")
def read_root():
    """Simple health/root endpoint."""
    return {"Hello": "World"}


@app.get("/prices", response_model=list[PriceBar])
def get_prices(symbol: str, outputsize: str = "compact", limit: int = 200):
    """Fetch daily OHLCV from Alpha Vantage, persist it locally, and return a tail.

    Args:
        symbol: Ticker symbol (e.g., "MSFT").
        outputsize: Alpha Vantage output size ("compact" or "full").
        limit: Maximum number of most-recent bars to return (rows). This does not
            limit what is fetched from Alpha Vantage; it only limits the response.

    Returns:
        A list of `PriceBar` objects sorted oldest -> newest, limited to the most
        recent `limit` bars.

    Raises:
        HTTPException:
            - 400 if `limit` is invalid.
            - 500 if AV_API_KEY is missing.
            - 429 if Alpha Vantage rate limit is hit.
            - 400 for Alpha Vantage API errors (invalid symbol/params).
            - 502 for unexpected upstream response format.
    """
    if limit < 1:
        raise HTTPException(status_code=400, detail="limit must be >= 1")
    if limit > 5000:
        raise HTTPException(status_code=400, detail="limit too large (max 5000)")

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

    ts = data[ts_key]

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

    # Persist full fetched set (compact/full), then return tail(limit)
    _persist_symbol_bars(symbol, out)

    return out[-limit:]
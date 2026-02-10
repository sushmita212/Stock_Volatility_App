from fastapi import FastAPI, HTTPException
import os
import json
import csv
from pathlib import Path
from datetime import datetime, timezone, date, timedelta
from zoneinfo import ZoneInfo
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

    Returns default metadata if the file does not exist or is unreadable.
    """
    if not METADATA_PATH.exists():
        return {"version": 1, "tickers": {}}
    try:
        with METADATA_PATH.open("r", encoding="utf-8") as f:
            meta = json.load(f)
        if not isinstance(meta, dict):
            return {"version": 1, "tickers": {}}
        meta.setdefault("version", 1)
        meta.setdefault("tickers", {})
        if not isinstance(meta["tickers"], dict):
            meta["tickers"] = {}
        return meta
    except (json.JSONDecodeError, OSError):
        return {"version": 1, "tickers": {}}


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


def _csv_path_for_symbol(symbol: str) -> Path:
    """Return the canonical CSV path for a ticker symbol in the local store."""
    return STOCK_DATA_DIR / f"{symbol.upper()}.csv"


def _read_bars_from_csv(symbol: str) -> list[PriceBar]:
    """Read all locally stored OHLCV bars for `symbol` from CSV."""
    csv_path = _csv_path_for_symbol(symbol)
    if not csv_path.exists():
        return []

    bars: list[PriceBar] = []
    with csv_path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            bars.append(
                PriceBar(
                    date=row["date"],
                    open=float(row["open"]),
                    high=float(row["high"]),
                    low=float(row["low"]),
                    close=float(row["close"]),
                    volume=float(row["volume"]),
                )
            )

    bars.sort(key=lambda b: b.date)
    return bars


def _latest_local_date(symbol: str, meta: dict) -> str | None:
    """Get the latest stored date for a symbol from metadata, falling back to CSV."""
    sym = symbol.upper()
    md = meta.get("tickers", {}).get(sym, {})
    last_date = md.get("last_date")
    if isinstance(last_date, str) and last_date:
        return last_date

    # Fallback: compute from CSV without loading everything into Pydantic models
    csv_path = _csv_path_for_symbol(symbol)
    if not csv_path.exists():
        return None

    max_date: str | None = None
    with csv_path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            d = row.get("date")
            if d and (max_date is None or d > max_date):
                max_date = d
    return max_date

def _expected_latest_daily_date_et(now_utc: datetime | None = None, after_close_hour_et: int = 18) -> str:
    """Return the latest *expected* daily bar date for US markets.

    We approximate that the newest fully-available daily bar is:
    - yesterday's date (ET) before `after_close_hour_et` (default 6pm ET)
    - today's date (ET) at/after `after_close_hour_et`

    This avoids repeatedly refreshing during the trading day when providers often
    only have data through the prior close.

    Args:
        now_utc: Current time in UTC (defaults to now).
        after_close_hour_et: Local ET hour after which we expect today's bar.

    Returns:
        ISO date string YYYY-MM-DD in Eastern time.
    """

    if now_utc is None:
        now_utc = datetime.now(timezone.utc)

    now_et = now_utc.astimezone(ZoneInfo("America/New_York"))
    expected = now_et.date()
    if now_et.hour < after_close_hour_et:
        expected = expected - timedelta(days=1)

    return expected.isoformat()


def _parse_utc_iso(ts: str) -> datetime | None:
    """Parse an ISO-8601 timestamp (possibly with Z/offset) into an aware UTC datetime."""
    try:
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def _is_stale_daily(symbol: str, meta: dict, refresh_cooldown_minutes: int = 360) -> bool:
    """Return True if local data for `symbol` should be refreshed.

    Staleness logic (v1 market-aware):
    1) If no local data -> stale
    2) If we refreshed successfully within `refresh_cooldown_minutes` -> not stale
    3) Otherwise compare last stored bar date to the *expected* latest market date
       based on ET and an after-close cutoff.

    Notes:
        - This does not account for weekends/holidays; the cooldown prevents most
          repeated calls when the provider hasn't published a new bar yet.
    """

    sym = symbol.upper()
    last_date = _latest_local_date(symbol, meta)
    if not last_date:
        return True

    md = meta.get("tickers", {}).get(sym, {})
    last_refresh = md.get("last_refresh_success_at")
    if isinstance(last_refresh, str) and last_refresh:
        last_refresh_dt = _parse_utc_iso(last_refresh)
        if last_refresh_dt is not None:
            age = datetime.now(timezone.utc) - last_refresh_dt
            if age < timedelta(minutes=refresh_cooldown_minutes):
                return False

    expected_latest = _expected_latest_daily_date_et()
    return last_date < expected_latest

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

def _fetch_daily_from_alpha_vantage(symbol: str, outputsize: str, api_key: str) -> list[PriceBar]:
    """Fetch daily OHLCV from Alpha Vantage and normalize it into `PriceBar` objects."""
    url = "https://www.alphavantage.co/query"
    params = {
        "function": "TIME_SERIES_DAILY",
        "symbol": symbol,
        "outputsize": outputsize,  # "compact" or "full"
        "datatype": "json",
        "apikey": api_key,
    }
    r = requests.get(url, params=params, timeout=30)
    # If AV returns non-JSON, this will raise; we keep it explicit:
    try:
        data = r.json()
    except ValueError:
        raise HTTPException(status_code=502, detail=f"Alpha Vantage returned non-JSON (status {r.status_code}).")

    # Alpha Vantage sends errors as JSON with HTTP 200
    if "Note" in data:
        raise HTTPException(status_code=429, detail=f"Alpha Vantage rate limit: {data['Note']}")
    if "Information" in data:
        raise HTTPException(status_code=403, detail=f"Alpha Vantage info: {data['Information']}")
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

    return out

@app.get("/")
def read_root():
    """Root endpoint that advertises the API and points to docs."""
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
    """Health check endpoint.

    Returns a small status payload indicating whether the API process is running
    and whether local storage paths are accessible. This endpoint does not call
    external providers (e.g., Alpha Vantage) so it is safe for frequent checks.
    """
    return {
        "status": "ok",
        "time_utc": _utc_now_iso(),
        "repo_root": str(REPO_ROOT),
        "data_dir": str(DATA_DIR),
        "stock_data_dir_exists": STOCK_DATA_DIR.exists(),
        "metadata_exists": METADATA_PATH.exists(),
    }

@app.get("/prices", response_model=list[PriceBar])
def get_prices(symbol: str, outputsize: str = "compact", limit: int = 200):
    """Return daily OHLCV for `symbol` with lazy refresh.

    Strategy:
        - Serve from local CSV if data is fresh.
        - Refresh from Alpha Vantage only when stale/missing.
        - Always return only the most recent `limit` bars.

    Freshness rule (v1):
        Refresh if metadata last_date (or CSV max date) is < today's UTC date.
    """
    if limit < 1:
        raise HTTPException(status_code=400, detail="limit must be >= 1")
    if limit > 5000:
        raise HTTPException(status_code=400, detail="limit too large (max 5000)")
    meta = _load_metadata()

    # If local is fresh, serve from disk without hitting Alpha Vantage
    if not _is_stale_daily(symbol, meta):
        local_bars = _read_bars_from_csv(symbol)
        if local_bars:
            return local_bars[-limit:]
        # If metadata says fresh but file missing/empty, treat as stale
        # (falls through to fetch)

    api_key = os.getenv("AV_API_KEY")
    if not api_key:
        raise HTTPException(status_code=500, detail="Missing AV_API_KEY environment variable.")

    fetched = _fetch_daily_from_alpha_vantage(symbol, outputsize=outputsize, api_key=api_key)

    # Persist fetched set, then serve from local store (ensures consistency with merge/dedupe)
    _persist_symbol_bars(symbol, fetched)

    local_bars = _read_bars_from_csv(symbol)
    if local_bars:
        return local_bars[-limit:]

    # Should not happen, but keep a clear error if persistence failed silently
    return fetched[-limit:]
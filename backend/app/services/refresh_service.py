from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

from backend.app.datalayer.alphavantage import ProviderError, fetch_time_series_daily, normalize_daily_ohlcv
from backend.app.datalayer.storage import StorePaths, load_metadata, persist_symbol_rows, csv_path_for_symbol


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def symbols_from_store(paths: StorePaths) -> list[str]:
    """Return symbols we have ever stored (union of metadata + CSV files)."""
    meta = load_metadata(paths)
    syms = set()
    tickers = meta.get("tickers", {})
    if isinstance(tickers, dict):
        syms.update([str(s).upper() for s in tickers.keys()])

    if paths.stock_data_dir.exists():
        for p in paths.stock_data_dir.glob("*.csv"):
            syms.add(p.stem.upper())

    return sorted(syms)


def should_compact_integrity_refresh(
    symbol: str,
    meta: dict,
    every_days: int = 80,
    now_utc: datetime | None = None,
) -> bool:
    """True if it's been >= `every_days` since last successful refresh for symbol."""
    if now_utc is None:
        now_utc = datetime.now(timezone.utc)

    md = meta.get("tickers", {}).get(symbol.upper(), {})
    last = md.get("last_refresh_success_at")
    if not isinstance(last, str) or not last:
        return True

    try:
        dt = datetime.fromisoformat(last.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        dt = dt.astimezone(timezone.utc)
    except Exception:
        return True

    return (now_utc - dt) >= timedelta(days=every_days)


def refresh_symbol_compact(paths: StorePaths, symbol: str, api_key: str) -> None:
    """Run a compact refresh for existing symbol, full for first-time symbols."""
    outputsize = "compact"
    if not csv_path_for_symbol(paths, symbol).exists():
        outputsize = "full"

    ts = fetch_time_series_daily(symbol=symbol, api_key=api_key, outputsize=outputsize)
    rows = normalize_daily_ohlcv(ts)
    persist_symbol_rows(paths, symbol=symbol, new_rows=rows, source="alphavantage")


def run_integrity_refresh(every_days: int = 80) -> dict:
    """Refresh all known symbols if it's been long enough since last refresh.

    This is intended to be scheduled externally (cron/GitHub Actions/etc).

    Returns a summary dict.
    """

    api_key = os.getenv("AV_API_KEY")
    if not api_key:
        raise RuntimeError("Missing AV_API_KEY environment variable.")

    paths = StorePaths(repo_root=_repo_root())
    meta = load_metadata(paths)

    refreshed: list[str] = []
    skipped: list[str] = []
    errors: dict[str, str] = {}

    for sym in symbols_from_store(paths):
        if not should_compact_integrity_refresh(sym, meta, every_days=every_days):
            skipped.append(sym)
            continue

        try:
            refresh_symbol_compact(paths, sym, api_key=api_key)
            refreshed.append(sym)
        except ProviderError as e:
            errors[sym] = f"ProviderError({e.status_code}): {e.message}"
        except Exception as e:  # pragma: no cover
            errors[sym] = f"{type(e).__name__}: {e}"

    return {
        "time_utc": datetime.now(timezone.utc).isoformat(),
        "every_days": every_days,
        "refreshed": refreshed,
        "skipped": skipped,
        "errors": errors,
    }

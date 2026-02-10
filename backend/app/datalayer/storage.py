from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class StorePaths:
    """Resolve paths for the local on-disk store."""

    repo_root: Path

    @property
    def data_dir(self) -> Path:
        return self.repo_root / "data"

    @property
    def stock_data_dir(self) -> Path:
        return self.data_dir / "stock_data"

    @property
    def metadata_path(self) -> Path:
        return self.data_dir / "metadata.json"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def local_now_human() -> str:
    return datetime.now().astimezone().strftime("%Y-%m-%d %H:%M:%S %Z")


def load_metadata(paths: StorePaths) -> dict[str, Any]:
    """Load metadata JSON, returning a default on missing/invalid."""
    p = paths.metadata_path
    if not p.exists():
        return {"version": 1, "tickers": {}}

    try:
        with p.open("r", encoding="utf-8") as f:
            meta = json.load(f)
    except (json.JSONDecodeError, OSError):
        return {"version": 1, "tickers": {}}

    if not isinstance(meta, dict):
        return {"version": 1, "tickers": {}}

    meta.setdefault("version", 1)
    if not isinstance(meta.get("tickers"), dict):
        meta["tickers"] = {}

    return meta  # type: ignore[return-value]


def save_metadata(paths: StorePaths, meta: dict[str, Any]) -> None:
    """Write metadata JSON atomically."""
    paths.data_dir.mkdir(parents=True, exist_ok=True)
    tmp = paths.metadata_path.with_suffix(".json.tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(meta, f, indent=2, sort_keys=True)
    tmp.replace(paths.metadata_path)


def csv_path_for_symbol(paths: StorePaths, symbol: str) -> Path:
    return paths.stock_data_dir / f"{symbol.upper()}.csv"


def read_rows_by_date(csv_path: Path) -> dict[str, dict[str, str]]:
    """Read canonical OHLCV CSV into a dict keyed by date."""
    if not csv_path.exists():
        return {}

    out: dict[str, dict[str, str]] = {}
    with csv_path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            d = row.get("date")
            if d:
                out[d] = row  # last one wins if duplicates exist
    return out


def write_rows_by_date(csv_path: Path, rows_by_date: dict[str, dict[str, str]]) -> None:
    """Write canonical OHLCV rows to CSV atomically."""
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = ["date", "open", "high", "low", "close", "volume"]
    tmp = csv_path.with_suffix(".csv.tmp")
    with tmp.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for d in sorted(rows_by_date.keys()):
            writer.writerow(rows_by_date[d])
    tmp.replace(csv_path)


def latest_date_in_rows(rows_by_date: dict[str, dict[str, str]]) -> str | None:
    if not rows_by_date:
        return None
    return max(rows_by_date.keys())


def persist_symbol_rows(
    paths: StorePaths,
    symbol: str,
    new_rows: list[dict[str, str]],
    source: str = "alphavantage",
) -> None:
    """Merge new canonical OHLCV rows into the symbol CSV and update metadata."""

    csv_path = csv_path_for_symbol(paths, symbol)
    existing = read_rows_by_date(csv_path)

    for row in new_rows:
        d = row.get("date")
        if not d:
            continue
        existing[d] = {
            "date": d,
            "open": str(row.get("open", "")),
            "high": str(row.get("high", "")),
            "low": str(row.get("low", "")),
            "close": str(row.get("close", "")),
            "volume": str(row.get("volume", "")),
        }

    write_rows_by_date(csv_path, existing)

    meta = load_metadata(paths)
    tickers: dict[str, Any] = meta.setdefault("tickers", {})
    sym = symbol.upper()
    tickers[sym] = {
        "last_date": latest_date_in_rows(existing),
        "rows": len(existing),
        "last_refresh_success_at": utc_now_iso(),
        "last_refresh_success_at_local": local_now_human(),
        "status": "ok",
        "source": source,
    }
    save_metadata(paths, meta)


def read_bars_tail(paths: StorePaths, symbol: str, limit: int) -> list[dict[str, str]]:
    """Read the most recent `limit` rows for a symbol from local CSV."""
    rows = read_rows_by_date(csv_path_for_symbol(paths, symbol))
    if not rows:
        return []
    ordered_dates = sorted(rows.keys())
    tail_dates = ordered_dates[-limit:]
    return [rows[d] for d in tail_dates]

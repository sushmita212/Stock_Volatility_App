from __future__ import annotations

from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from typing import Any


def expected_latest_daily_date_et(
    now_utc: datetime | None = None,
    after_close_hour_et: int = 18,
) -> str:
    """Return the latest *expected* US-market daily bar date in ET (YYYY-MM-DD).

    Approximation:
    - before `after_close_hour_et` in ET => newest complete bar is yesterday
    - at/after `after_close_hour_et` => newest complete bar is today

    This helps avoid unnecessary refresh attempts during the trading day.
    """

    if now_utc is None:
        now_utc = datetime.now(timezone.utc)

    now_et = now_utc.astimezone(ZoneInfo("America/New_York"))
    expected = now_et.date()
    if now_et.hour < after_close_hour_et:
        expected = expected - timedelta(days=1)

    return expected.isoformat()


def parse_utc_iso(ts: str) -> datetime | None:
    """Parse an ISO-8601 timestamp to an aware UTC datetime."""
    try:
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def latest_local_date_from_metadata(symbol: str, meta: dict[str, Any]) -> str | None:
    """Get the latest stored bar date for symbol from metadata (YYYY-MM-DD)."""
    sym = symbol.upper()
    md = meta.get("tickers", {}).get(sym, {})
    last_date = md.get("last_date")
    if isinstance(last_date, str) and last_date:
        return last_date
    return None


def is_stale_daily(
    symbol: str,
    meta: dict[str, Any],
    refresh_cooldown_minutes: int = 360,
    after_close_hour_et: int = 18,
    now_utc: datetime | None = None,
) -> bool:
    """Return True if local daily data for `symbol` should be refreshed.

    Rules:
      1) No last_date => stale
      2) Recent successful refresh within cooldown => not stale
      3) Otherwise, compare last_date to expected latest market date (ET cutoff)

    Note:
      Does not handle market holidays/weekends explicitly.
    """

    if now_utc is None:
        now_utc = datetime.now(timezone.utc)

    sym = symbol.upper()
    last_date = latest_local_date_from_metadata(symbol, meta)
    if not last_date:
        return True

    md = meta.get("tickers", {}).get(sym, {})
    last_refresh = md.get("last_refresh_success_at")
    if isinstance(last_refresh, str) and last_refresh:
        last_refresh_dt = parse_utc_iso(last_refresh)
        if last_refresh_dt is not None:
            if (now_utc - last_refresh_dt) < timedelta(minutes=refresh_cooldown_minutes):
                return False

    expected_latest = expected_latest_daily_date_et(now_utc=now_utc, after_close_hour_et=after_close_hour_et)
    return last_date < expected_latest

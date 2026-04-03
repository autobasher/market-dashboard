from __future__ import annotations

import json
import logging
import sqlite3
from datetime import date
from urllib.request import urlopen, Request
from urllib.error import HTTPError

from market_dashboard.config import get_eodhd_api_key
from market_dashboard.portfolio import queries

logger = logging.getLogger(__name__)

_API_BASE = "https://eodhd.com/api"

# Mirrors the high-water tracking from prices.py
_fetch_high_water: dict[tuple[int, str], str] = {}


def _conn_key(conn: sqlite3.Connection, symbol: str) -> tuple[int, str]:
    return (id(conn), symbol)


def _eodhd_get(endpoint: str, params: dict[str, str]) -> list[dict]:
    """Make a GET request to the EODHD API. Returns parsed JSON."""
    api_key = get_eodhd_api_key()
    if not api_key:
        raise RuntimeError("EODHD_API_KEY not set — check .env or environment")

    params["api_token"] = api_key
    params["fmt"] = "json"
    qs = "&".join(f"{k}={v}" for k, v in params.items())
    url = f"{_API_BASE}/{endpoint}?{qs}"

    req = Request(url, headers={"User-Agent": "market-dashboard/0.1"})
    try:
        with urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read())
    except HTTPError as e:
        if e.code == 429:
            logger.warning("EODHD rate limit hit — daily quota exhausted")
            return []
        if e.code == 401:
            logger.warning("EODHD API key unauthorized (401) — check key validity")
            return []
        if e.code == 404:
            logger.warning("EODHD ticker not found (404) for %s", endpoint)
            return []
        raise

    if isinstance(data, dict) and "error" in data:
        logger.warning("EODHD error for %s: %s", endpoint, data["error"])
        return []

    return data if isinstance(data, list) else []


def fetch_eodhd_prices(
    conn: sqlite3.Connection,
    symbol: str,
    start: date,
    end: date,
) -> int:
    """Fetch missing historical prices from EODHD and cache them.

    Symbol should be in EODHD format, e.g. 'IE00B0HCGS80.EUFUND'.
    Returns the number of new rows inserted.
    """
    cached_min, cached_max = queries.get_cached_price_range(conn, symbol)
    key = _conn_key(conn, symbol)
    high_water = _fetch_high_water.get(key)

    start_str = start.isoformat()
    end_str = end.isoformat()

    # Determine what range (if any) we actually need to fetch
    fetch_start = start_str
    fetch_end = end_str
    need_fetch = False

    if cached_min is None and high_water is None:
        # No data at all — fetch everything
        need_fetch = True
    elif cached_min is not None:
        effective_max = max(cached_max, high_water) if high_water else cached_max
        if start_str < cached_min:
            # Missing early history — fetch the gap before cached data
            need_fetch = True
            fetch_end = cached_min
        if end_str > effective_max:
            # Missing recent data — fetch only the tail
            need_fetch = True
            fetch_start = effective_max
            fetch_end = end_str
    elif high_water is not None and end_str > high_water:
        need_fetch = True
        fetch_start = high_water

    if not need_fetch:
        return 0

    data = _eodhd_get(f"eod/{symbol}", {"from": fetch_start, "to": fetch_end})

    prev = _fetch_high_water.get(key, "")
    _fetch_high_water[key] = max(end_str, prev)

    if not data:
        return 0

    count = 0
    for row in data:
        price_date = date.fromisoformat(row["date"])
        close = float(row["close"])
        adj_close = float(row.get("adjusted_close", close))
        volume = int(row["volume"]) if row.get("volume") else None
        queries.upsert_historical_price(conn, symbol, price_date, close, adj_close, volume)
        count += 1

    conn.commit()
    logger.info("EODHD: fetched %d prices for %s (%s to %s)", count, symbol, start_str, end_str)
    return count

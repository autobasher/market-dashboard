from __future__ import annotations

import logging
import sqlite3
from datetime import date

import pandas as pd
import yfinance as yf

from market_dashboard.portfolio import queries
from market_dashboard.portfolio.models import TxType

logger = logging.getLogger(__name__)

# Tracks (symbol -> requested_end) so we don't re-fetch ranges where
# yfinance returned no data for trailing weekends/holidays.
_fetch_high_water: dict[tuple[int, str], str] = {}


def _conn_key(conn: sqlite3.Connection, symbol: str) -> tuple[int, str]:
    return (id(conn), symbol)


def fetch_historical_prices(
    conn: sqlite3.Connection,
    symbol: str,
    start: date,
    end: date,
) -> int:
    """Fetch missing historical prices from yfinance and cache them.

    Returns the number of new rows inserted.
    """
    cached_min, cached_max = queries.get_cached_price_range(conn, symbol)
    key = _conn_key(conn, symbol)
    high_water = _fetch_high_water.get(key)

    start_str = start.isoformat()
    end_str = end.isoformat()

    # Determine what date ranges are missing
    need_fetch = False
    if cached_min is None and high_water is None:
        need_fetch = True
    elif cached_min is not None:
        effective_max = max(cached_max, high_water) if high_water else cached_max
        if start_str < cached_min or end_str > effective_max:
            need_fetch = True
    elif high_water is not None and end_str > high_water:
        need_fetch = True

    if not need_fetch:
        return 0

    df = yf.download(symbol, start=start_str, end=end_str, progress=False, auto_adjust=False)

    # Record that we've attempted this range
    prev = _fetch_high_water.get(key, "")
    _fetch_high_water[key] = max(end_str, prev)

    if df.empty:
        return 0

    # yfinance 1.1+ always returns MultiIndex columns (Price, Ticker)
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
        # Remove any duplicate columns after flattening
        df = df.loc[:, ~df.columns.duplicated()]

    if "Close" not in df.columns:
        logger.warning("No Close column for %s after download", symbol)
        return 0

    count = 0
    for dt_idx, row in df.iterrows():
        price_date = dt_idx.date()
        close_val = row["Close"]
        if hasattr(close_val, "__len__") and not isinstance(close_val, str):
            close_val = close_val.iloc[0]
        close = float(close_val)
        adj_close = float(row.get("Adj Close", close))
        volume = int(row["Volume"]) if row.get("Volume") is not None else None
        queries.upsert_historical_price(conn, symbol, price_date, close, adj_close, volume)
        count += 1

    conn.commit()
    return count


def ensure_splits_for_portfolio(
    conn: sqlite3.Connection,
    account_id: str,
    symbols: list[str],
    start: date,
    end: date,
) -> int:
    """Fetch splits from yfinance and insert SPLIT transactions.

    Returns the number of splits inserted or updated.
    """
    count = 0
    errors = 0
    for sym in symbols:
        try:
            splits = yf.Ticker(sym).splits
        except Exception:
            logger.warning("Failed to fetch splits for %s", sym)
            errors += 1
            continue
        if splits is None or splits.empty:
            continue
        for dt, ratio in splits.items():
            split_date = dt.date()
            if split_date < start or split_date > end:
                continue
            ratio = float(ratio)
            existing = conn.execute(
                "SELECT tx_id, split_ratio FROM transactions "
                "WHERE account_id = ? AND symbol = ? AND trade_date = ? AND tx_type = ?",
                (account_id, sym, split_date.isoformat(), TxType.SPLIT.value),
            ).fetchone()
            if existing:
                if existing["split_ratio"] is None:
                    conn.execute(
                        "UPDATE transactions SET split_ratio = ? WHERE tx_id = ?",
                        (ratio, existing["tx_id"]),
                    )
                    logger.info("Split: %s on %s ratio=%.6f (updated tx_id=%s)", sym, split_date, ratio, existing["tx_id"])
            else:
                tx_id = queries.insert_transaction(
                    conn,
                    account_id=account_id,
                    trade_date=split_date,
                    tx_type=TxType.SPLIT.value,
                    total_amount=0.0,
                    symbol=sym,
                    split_ratio=ratio,
                    raw_description=f"Stock split {ratio}",
                    source_file="yfinance",
                )
                logger.info("Split: %s on %s ratio=%.6f (new tx_id=%s)", sym, split_date, ratio, tx_id)
            count += 1
    if errors:
        logger.warning("Failed to fetch splits for %d/%d symbols", errors, len(symbols))
    conn.commit()
    return count


def fetch_live_prices(symbols: list[str]) -> dict[str, float]:
    """Fetch current quotes from yfinance for intraday portfolio valuation.

    During market hours, returns the latest traded price.
    After hours, returns the most recent closing price.
    VMFXX is hardcoded to $1.00 (money market).
    """
    if not symbols:
        return {}

    prices: dict[str, float] = {}
    # VMFXX is always $1.00
    non_cash = [s for s in symbols if s != "VMFXX"]
    if "VMFXX" in symbols:
        prices["VMFXX"] = 1.0

    for sym in non_cash:
        try:
            info = yf.Ticker(sym).fast_info
            price = info.get("lastPrice") or info.get("last_price")
            if price and price > 0:
                prices[sym] = float(price)
        except Exception:
            logger.warning("Failed to fetch live price for %s", sym)

    return prices


def ensure_prices_for_portfolio(
    conn: sqlite3.Connection,
    symbols: list[str],
    start: date,
    end: date,
) -> dict[str, int]:
    """Fetch/cache prices for all symbols. Returns {symbol: rows_inserted}."""
    results = {}
    for sym in symbols:
        results[sym] = fetch_historical_prices(conn, sym, start, end)
    return results

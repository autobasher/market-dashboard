from __future__ import annotations

import logging
import sqlite3
from datetime import date

import pandas as pd
import yfinance as yf

from market_dashboard.config import EODHD_TICKERS, MONEY_MARKET_TICKERS
from market_dashboard.portfolio import queries
from market_dashboard.portfolio.eodhd_prices import fetch_eodhd_prices
from market_dashboard.portfolio.models import TxType

logger = logging.getLogger(__name__)

# Tracks (symbol -> requested_end) so we don't re-fetch ranges where
# yfinance returned no data for trailing weekends/holidays.
_fetch_high_water: dict[tuple[int, str], str] = {}


def _conn_key(conn: sqlite3.Connection, symbol: str) -> tuple[int, str]:
    return (id(conn), symbol)


def _isnan(v) -> bool:
    try:
        return v != v  # NaN != NaN
    except (TypeError, ValueError):
        return False


def _plan_fetch(
    conn: sqlite3.Connection,
    symbol: str,
    start: date,
    end: date,
) -> tuple[bool, str, str]:
    """Decide whether (and what range) to fetch for a symbol given the cache.

    Returns (need_fetch, fetch_start, fetch_end) as ISO date strings.
    """
    cached_min, cached_max = queries.get_cached_price_range(conn, symbol)
    high_water = _fetch_high_water.get(_conn_key(conn, symbol))

    start_str = start.isoformat()
    end_str = end.isoformat()
    fetch_start = start_str
    fetch_end = end_str
    need_fetch = False

    if cached_min is None and high_water is None:
        need_fetch = True
    elif cached_min is not None:
        effective_max = max(cached_max, high_water) if high_water else cached_max
        if start_str < cached_min:
            need_fetch = True
            fetch_end = cached_min
        if end_str > effective_max:
            need_fetch = True
            fetch_start = effective_max
            fetch_end = end_str
    elif high_water is not None and end_str > high_water:
        need_fetch = True
        fetch_start = high_water

    return need_fetch, fetch_start, fetch_end


def _extract_series(df: pd.DataFrame, field: str, symbol: str):
    """Pull a single field's Series for `symbol` out of a yf.download frame.

    Handles both the MultiIndex layout (multi-symbol downloads, and single-symbol
    downloads on yfinance 1.1+) and flat columns.
    """
    cols = df.columns
    if isinstance(cols, pd.MultiIndex):
        if (field, symbol) in cols:
            return df[(field, symbol)]
        if (symbol, field) in cols:  # group_by='ticker' layout
            return df[(symbol, field)]
        return None
    return df[field] if field in cols else None


def fetch_historical_prices(
    conn: sqlite3.Connection,
    symbol: str,
    start: date,
    end: date,
) -> int:
    """Fetch missing historical prices from yfinance and cache them.

    Returns the number of new rows inserted.
    """
    key = _conn_key(conn, symbol)
    need_fetch, fetch_start, fetch_end = _plan_fetch(conn, symbol, start, end)
    end_str = end.isoformat()

    if not need_fetch:
        return 0

    df = yf.download(symbol, start=fetch_start, end=fetch_end, progress=False, auto_adjust=False)

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


def fetch_historical_prices_batch(
    conn: sqlite3.Connection,
    symbols: list[str],
    start: date,
    end: date,
) -> dict[str, int]:
    """Batched equivalent of fetch_historical_prices for Yahoo symbols.

    Plans each symbol's needed range independently, then issues ONE yf.download
    over the union range for the symbols that need data — replacing N sequential
    HTTP round-trips with one. Over-fetching a wider range is harmless because
    upserts are idempotent (INSERT OR REPLACE). Falls back to per-symbol fetch
    if the batched download raises.
    """
    results = {sym: 0 for sym in symbols}

    plans = {}
    for sym in symbols:
        need, fs, fe = _plan_fetch(conn, sym, start, end)
        if need:
            plans[sym] = (fs, fe)

    if not plans:
        return results

    needed = list(plans.keys())
    union_start = min(fs for fs, _ in plans.values())
    union_end = max(fe for _, fe in plans.values())
    end_str = end.isoformat()

    try:
        df = yf.download(
            needed, start=union_start, end=union_end,
            progress=False, auto_adjust=False, threads=True,
        )
    except Exception:
        logger.exception("Batch yf.download failed for %s — falling back per-symbol", needed)
        for sym in needed:
            results[sym] = fetch_historical_prices(conn, sym, start, end)
        return results

    # Record attempted range for every symbol, even those with no data returned
    for sym in needed:
        key = _conn_key(conn, sym)
        prev = _fetch_high_water.get(key, "")
        _fetch_high_water[key] = max(end_str, prev)

    if df is None or df.empty:
        return results

    for sym in needed:
        close_s = _extract_series(df, "Close", sym)
        if close_s is None:
            logger.debug("No Close data for %s in batch download", sym)
            continue
        adj_s = _extract_series(df, "Adj Close", sym)
        vol_s = _extract_series(df, "Volume", sym)

        count = 0
        for dt_idx in close_s.index:
            close_val = close_s[dt_idx]
            if close_val is None or _isnan(close_val):
                continue
            close = float(close_val)
            adj_val = adj_s[dt_idx] if adj_s is not None else None
            adj_close = float(adj_val) if adj_val is not None and not _isnan(adj_val) else close
            vol_val = vol_s[dt_idx] if vol_s is not None else None
            volume = int(vol_val) if vol_val is not None and not _isnan(vol_val) else None
            queries.upsert_historical_price(conn, sym, dt_idx.date(), close, adj_close, volume)
            count += 1
        results[sym] = count

    conn.commit()
    return results


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
    # EODHD EUFUND tickers are mutual funds — no splits
    symbols = [s for s in symbols if s not in EODHD_TICKERS]
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


def fetch_live_prices(
    symbols: list[str],
    conn: sqlite3.Connection | None = None,
) -> dict[str, float]:
    """Fetch current quotes for intraday portfolio valuation.

    Yahoo tickers: latest traded price via yfinance.
    EODHD tickers: latest cached price from DB (daily NAV only).
    VMFXX is hardcoded to $1.00 (money market).
    """
    if not symbols:
        return {}

    prices: dict[str, float] = {}
    for sym in symbols:
        if sym in MONEY_MARKET_TICKERS:
            prices[sym] = 1.0

    non_cash = [s for s in symbols if s not in MONEY_MARKET_TICKERS]
    eodhd_syms = [s for s in non_cash if s in EODHD_TICKERS]
    yahoo_syms = [s for s in non_cash if s not in EODHD_TICKERS]

    if eodhd_syms and conn is not None:
        prices.update(queries.get_latest_prices(conn, eodhd_syms))

    if yahoo_syms:
        try:
            tickers_obj = yf.Tickers(" ".join(yahoo_syms))
            for sym in yahoo_syms:
                try:
                    fi = tickers_obj.tickers[sym].fast_info
                    price = fi.get("lastPrice") or fi.get("last_price")
                    if price and price > 0:
                        prices[sym] = float(price)
                except Exception:
                    logger.warning("Failed to fetch live price for %s", sym)
        except Exception:
            logger.warning("Batch live price fetch failed")

    return prices


def ensure_prices_for_portfolio(
    conn: sqlite3.Connection,
    symbols: list[str],
    start: date,
    end: date,
) -> dict[str, int]:
    """Fetch/cache prices for all symbols. Returns {symbol: rows_inserted}."""
    results = {}
    yahoo_syms = []
    for sym in symbols:
        if sym in MONEY_MARKET_TICKERS:
            continue  # money market funds: always $1/share, no price fetch needed
        elif sym in EODHD_TICKERS:
            results[sym] = fetch_eodhd_prices(conn, sym, start, end)
        else:
            yahoo_syms.append(sym)
    if yahoo_syms:
        results.update(fetch_historical_prices_batch(conn, yahoo_syms, start, end))
    return results

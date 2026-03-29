"""What-if (hold) portfolio analysis.

Reconstructs the portfolio as of a start date, then values it daily
using real prices — as if no trades occurred after the start date.
Splits are still applied (mechanical, not a trading decision).
"""

from __future__ import annotations

import sqlite3
from datetime import date, timedelta

import pandas as pd

from market_dashboard.config import MONEY_MARKET_TICKERS
from market_dashboard.portfolio import queries
from market_dashboard.portfolio.models import TxType
from market_dashboard.portfolio.snapshots import (
    _build_split_factors,
    _unadjust_factor,
    _STALE_PRICE_DAYS,
)


def get_positions_as_of(
    conn: sqlite3.Connection,
    portfolio_id: int,
    as_of_date: date,
) -> tuple[dict[str, float], float]:
    """Replay transactions up to as_of_date to reconstruct positions.

    Returns (positions dict {symbol: shares}, vmfxx_balance).
    """
    account_ids = queries.get_effective_account_ids(conn, portfolio_id)
    if not account_ids:
        return {}, 0.0

    all_txs = []
    for acct_id in account_ids:
        all_txs.extend(
            queries.get_transactions(conn, account_id=acct_id, end_date=as_of_date)
        )
    all_txs.sort(key=lambda t: (t["trade_date"], t["tx_id"]))

    positions: dict[str, float] = {}
    vmfxx_balance = 0.0
    seen_splits: set[tuple] = set()

    for tx in all_txs:
        tx_type = tx["tx_type"]
        symbol = tx["symbol"]
        shares = tx["shares"] or 0.0

        if tx_type == TxType.SWEEP_IN.value:
            vmfxx_balance += abs(tx["total_amount"] or 0.0)
        elif tx_type == TxType.SWEEP_OUT.value:
            vmfxx_balance -= abs(tx["total_amount"] or 0.0)
        elif tx_type in (TxType.BUY.value, TxType.DRIP.value):
            if tx_type == TxType.DRIP.value and symbol == "VMFXX":
                vmfxx_balance += abs(tx["total_amount"] or 0.0)
            elif symbol and shares:
                positions[symbol] = positions.get(symbol, 0.0) + shares
        elif tx_type == TxType.SELL.value:
            if symbol:
                positions[symbol] = positions.get(symbol, 0.0) - shares
        elif tx_type == TxType.TRANSFER_IN.value:
            if symbol and shares:
                positions[symbol] = positions.get(symbol, 0.0) + shares
        elif tx_type == TxType.TRANSFER_OUT.value:
            if symbol and shares:
                positions[symbol] = positions.get(symbol, 0.0) - shares
        elif tx_type == TxType.SPLIT.value:
            ratio = tx["split_ratio"] or 1.0
            # Deduplicate splits: same symbol+date+ratio from different accounts
            # is the same corporate action, not two separate splits
            split_key = (symbol, tx["trade_date"], ratio)
            if symbol and symbol in positions and split_key not in seen_splits:
                seen_splits.add(split_key)
                positions[symbol] *= ratio

    # Drop zero/negative positions (threshold handles FP dust)
    positions = {s: sh for s, sh in positions.items() if sh > 1e-9}

    return positions, vmfxx_balance


def build_whatif_series(
    conn: sqlite3.Connection,
    portfolio_id: int,
    start: date,
    end: date,
) -> pd.DataFrame:
    """Build daily hold-portfolio valuations from start to end.

    Returns DataFrame with columns [date, hold_value, hold_return].
    """
    positions, vmfxx_balance = get_positions_as_of(conn, portfolio_id, start)

    if not positions and vmfxx_balance <= 0:
        return pd.DataFrame(columns=["date", "hold_value", "hold_return"])

    held_symbols = [s for s in positions if s not in MONEY_MARKET_TICKERS]

    # Load ALL transactions (need post-start splits for split factors)
    account_ids = queries.get_effective_account_ids(conn, portfolio_id)
    all_txs = []
    for acct_id in account_ids:
        all_txs.extend(queries.get_transactions(conn, account_id=acct_id))
    all_txs.sort(key=lambda t: (t["trade_date"], t["tx_id"]))

    # Deduplicate split transactions before building factors — aggregates
    # have the same corporate action recorded once per member account
    seen = set()
    deduped_txs = []
    for tx in all_txs:
        if tx["tx_type"] == TxType.SPLIT.value and tx["split_ratio"]:
            key = (tx["symbol"], tx["trade_date"], tx["split_ratio"])
            if key in seen:
                continue
            seen.add(key)
        deduped_txs.append(tx)
    split_factors = _build_split_factors(deduped_txs)

    # Collect post-start splits to apply to frozen positions
    # Deduplicate: same (date, symbol, ratio) from different accounts is one corporate action
    post_split_set: set[tuple[date, str, float]] = set()
    post_splits: list[tuple[date, str, float]] = []
    for tx in all_txs:
        if tx["tx_type"] != TxType.SPLIT.value:
            continue
        ratio = tx["split_ratio"] or 1.0
        tx_date = date.fromisoformat(tx["trade_date"])
        key = (tx_date, tx["symbol"], ratio)
        if tx_date > start and tx_date <= end and tx["symbol"] in positions and key not in post_split_set:
            post_split_set.add(key)
            post_splits.append(key)
    post_splits.sort()

    # Load prices for held symbols — start 7 days early so weekends/holidays
    # have a last_price available on the first output day
    price_lookback = start - timedelta(days=7)
    prices_by_sym: dict[str, dict[str, float]] = {}
    for sym in held_symbols:
        rows = queries.get_daily_prices(conn, sym, price_lookback, end)
        prices_by_sym[sym] = {r["price_date"]: r["close"] for r in rows}

    # Day-by-day valuation — run from lookback to seed last_price,
    # but only emit rows from start onward
    current = price_lookback
    start_value: float | None = None
    last_price: dict[str, float] = {}
    last_price_date: dict[str, date] = {}
    split_idx = 0
    rows_out = []

    # Seed money market prices
    for sym in positions:
        if sym in MONEY_MARKET_TICKERS:
            last_price[sym] = 1.0
            last_price_date[sym] = current

    while current <= end:
        date_str = current.isoformat()

        # Apply any splits that fall on this day
        while split_idx < len(post_splits) and post_splits[split_idx][0] <= current:
            _, sym, ratio = post_splits[split_idx]
            if sym in positions:
                positions[sym] *= ratio
            split_idx += 1

        # Update prices for today
        for sym in held_symbols:
            if sym in prices_by_sym and date_str in prices_by_sym[sym]:
                raw = prices_by_sym[sym][date_str]
                factor = _unadjust_factor(split_factors, sym, date_str)
                last_price[sym] = raw * factor
                last_price_date[sym] = current

        # Compute hold value
        equity = 0.0
        for sym, held in positions.items():
            if held <= 0:
                continue
            stale = (current - last_price_date.get(sym, current)).days > _STALE_PRICE_DAYS
            if stale:
                continue
            equity += held * last_price.get(sym, 0.0)

        cash = max(vmfxx_balance, 0.0)
        hold_value = equity + cash

        # Only emit rows from the requested start date onward
        if current >= start:
            if start_value is None:
                start_value = hold_value

            hold_return = (hold_value / start_value - 1) if start_value > 0 else 0.0

            rows_out.append({
                "date": current,
                "hold_value": hold_value,
                "hold_return": hold_return,
            })

        current += timedelta(days=1)

    return pd.DataFrame(rows_out)

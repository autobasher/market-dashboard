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

    # Seed starting cash if configured
    row = conn.execute(
        "SELECT starting_cash FROM portfolios WHERE portfolio_id = ?",
        (portfolio_id,),
    ).fetchone()
    starting_cash = row["starting_cash"] if row and row["starting_cash"] is not None else None

    all_txs = []
    for acct_id in account_ids:
        all_txs.extend(
            queries.get_transactions(conn, account_id=acct_id, end_date=as_of_date)
        )
    all_txs.sort(key=lambda t: (t["trade_date"], t["tx_id"]))

    has_sweeps = any(tx["tx_type"] in (TxType.SWEEP_IN.value, TxType.SWEEP_OUT.value) for tx in all_txs)

    positions: dict[str, float] = {}
    vmfxx_balance = starting_cash or 0.0
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
                if not has_sweeps:
                    vmfxx_balance -= abs(tx["total_amount"] or 0.0)
        elif tx_type == TxType.SELL.value:
            if symbol:
                positions[symbol] = positions.get(symbol, 0.0) - shares
                if not has_sweeps:
                    vmfxx_balance += abs(tx["total_amount"] or 0.0)
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

    Uses adj_close total-return ratios so dividends are credited as if
    reinvested. adj_close is also split-adjusted, so post-start splits
    require no special handling.

        per_share_value(t) = anchor_close × adj_close(t) / adj_close(start)

    where anchor_close is the actual trading price at start (close ×
    _unadjust_factor) so dollar values reflect reality.

    Returns DataFrame with columns [date, hold_value, hold_return].
    """
    positions, vmfxx_balance = get_positions_as_of(conn, portfolio_id, start)

    if not positions and vmfxx_balance <= 0:
        return pd.DataFrame(columns=["date", "hold_value", "hold_return"])

    held_symbols = [s for s in positions if s not in MONEY_MARKET_TICKERS]

    # Build split factors so we can un-split-adjust the anchor close.
    # adj_close handles split adjustment internally (and dividends) so we
    # don't need post_splits or unadjust beyond the anchor.
    account_ids = queries.get_effective_account_ids(conn, portfolio_id)
    all_txs = []
    for acct_id in account_ids:
        all_txs.extend(queries.get_transactions(conn, account_id=acct_id))
    all_txs.sort(key=lambda t: (t["trade_date"], t["tx_id"]))

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

    # Load prices: need close + adj_close. Lookback so weekend/holiday
    # starts still find an anchor.
    price_lookback = start - timedelta(days=7)
    prices_by_sym: dict[str, dict[str, tuple[float, float]]] = {}
    for sym in held_symbols:
        rows = queries.get_daily_prices(conn, sym, price_lookback, end)
        prices_by_sym[sym] = {
            r["price_date"]: (r["close"], r["adj_close"]) for r in rows
        }

    # Anchor each symbol: actual trading price at start and adj_close at start.
    # If a symbol has no price at/before start, drop it (no way to anchor).
    start_str = start.isoformat()
    anchors: dict[str, tuple[float, float]] = {}  # sym -> (anchor_close, adj_at_start)
    for sym in held_symbols:
        sym_prices = prices_by_sym.get(sym, {})
        candidates = [d for d in sym_prices if d <= start_str]
        if not candidates:
            continue
        anchor_date = max(candidates)
        raw_close, adj = sym_prices[anchor_date]
        unadj = _unadjust_factor(split_factors, sym, anchor_date)
        anchors[sym] = (raw_close * unadj, adj)

    # Money market positions held in `positions` (e.g. VMMXX) trade at $1.
    mm_value = sum(
        sh for sym, sh in positions.items() if sym in MONEY_MARKET_TICKERS
    )
    cash = max(vmfxx_balance, 0.0)

    # Track latest adj_close per symbol and the date of its last update for
    # staleness detection. Don't seed money markets — staleness check defaults
    # to "today" via .get(sym, current) so they're always considered fresh.
    last_adj: dict[str, float] = {}
    last_price_date: dict[str, date] = {}
    rows_out = []
    start_value: float | None = None

    current = start
    while current <= end:
        date_str = current.isoformat()

        # Update today's adj_close for any symbol that traded
        for sym in held_symbols:
            sym_prices = prices_by_sym.get(sym, {})
            if date_str in sym_prices:
                _, adj = sym_prices[date_str]
                last_adj[sym] = adj
                last_price_date[sym] = current

        equity = 0.0
        for sym in held_symbols:
            if sym not in anchors:
                continue
            shares = positions.get(sym, 0.0)
            if shares <= 0:
                continue
            stale = (current - last_price_date.get(sym, current)).days > _STALE_PRICE_DAYS
            if stale:
                continue
            anchor_close, adj_at_start = anchors[sym]
            adj_now = last_adj.get(sym, adj_at_start)
            if adj_at_start <= 0:
                continue
            per_share = anchor_close * (adj_now / adj_at_start)
            equity += shares * per_share

        hold_value = equity + mm_value + cash

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

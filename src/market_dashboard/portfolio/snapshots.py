from __future__ import annotations

import sqlite3
from datetime import date, timedelta

import pandas as pd

from market_dashboard.config import MONEY_MARKET_TICKERS
from market_dashboard.portfolio import queries
from market_dashboard.portfolio.models import TxType

# Positions with no price update in this many calendar days are valued at $0
# (handles delisted tickers whose last cached price would otherwise persist forever)
_STALE_PRICE_DAYS = 45


def _build_split_factors(all_txs: list) -> dict[str, list[tuple[str, float]]]:
    """Build per-symbol date intervals for reversing yfinance's split adjustment.

    yfinance's Close column is already split-adjusted. To get actual trading
    prices, we multiply Close by the cumulative product of all split ratios
    that occur AFTER a given date.

    Returns {symbol: [(date_boundary, factor), ...]} sorted ascending.
    For any date, use the factor from the last boundary <= that date.
    """
    splits_by_sym: dict[str, list[tuple[str, float]]] = {}
    for tx in all_txs:
        if tx["tx_type"] == TxType.SPLIT.value and tx["split_ratio"] and tx["symbol"]:
            sym = tx["symbol"]
            splits_by_sym.setdefault(sym, []).append((tx["trade_date"], tx["split_ratio"]))

    factors: dict[str, list[tuple[str, float]]] = {}
    for sym, splits in splits_by_sym.items():
        splits.sort()
        # Walk from last split backward, accumulating the product
        intervals = []
        cum = 1.0
        for split_date, ratio in reversed(splits):
            intervals.append((split_date, cum))
            cum *= ratio
        intervals.append(("0000-00-00", cum))  # before all splits
        intervals.reverse()
        factors[sym] = intervals

    return factors


def _unadjust_factor(factors: dict[str, list[tuple[str, float]]], symbol: str, date_str: str) -> float:
    """Get the multiplier to convert split-adjusted Close to actual trading price."""
    if symbol not in factors:
        return 1.0
    intervals = factors[symbol]
    result = 1.0
    for boundary, factor in intervals:
        if date_str >= boundary:
            result = factor
    return result


def _find_price(sym_prices: dict[str, float], target_date: str) -> float | None:
    """Find the close price on or just before target_date."""
    if target_date in sym_prices:
        return sym_prices[target_date]
    candidates = [d for d in sym_prices if d <= target_date]
    if candidates:
        return sym_prices[max(candidates)]
    return None


def build_daily_snapshots(
    conn: sqlite3.Connection,
    portfolio_id: int,
    end: date | None = None,
) -> pd.DataFrame:
    """Build daily portfolio valuations from transactions and prices.

    Always rebuilds the full history from the first transaction (see the note
    on incremental resume below); only ``end`` (the last day to value) is a
    parameter.

    total_value = equity positions + VMFXX settlement fund balance.
    total_cost  = cumulative net external cash flows (derived from residuals).
    twr         = cumulative time-weighted return.

    External cash flows are derived each day as:
        external_CF = total_value - pre_tx_value - investment_income
    where pre_tx_value = yesterday's positions at today's prices + yesterday's cash,
    and investment_income = dividends + fees (fees are negative).

    Time-weighted return chains daily returns adjusted for external cash flows:
        r_t = V_t / (V_{t-1} + CF_t) - 1
    """
    accounts = queries.get_portfolio_accounts(conn, portfolio_id)
    account_ids = [a["account_id"] for a in accounts]
    if not account_ids:
        return pd.DataFrame(columns=["date", "total_value", "total_cost", "cash_balance", "twr"])

    all_txs = []
    for acct_id in account_ids:
        all_txs.extend(queries.get_transactions(conn, account_id=acct_id))

    if not all_txs:
        return pd.DataFrame(columns=["date", "total_value", "total_cost", "cash_balance", "twr"])

    all_txs.sort(key=lambda t: (t["trade_date"], t["tx_id"]))

    # Deduplicate split transactions: aggregates pull the same corporate action
    # once per member account that holds the symbol. Without this, the multiplier
    # is applied multiple times to the combined position dict.
    seen_splits: set[tuple] = set()
    deduped_txs = []
    for tx in all_txs:
        if tx["tx_type"] == TxType.SPLIT.value and tx["split_ratio"]:
            key = (tx["symbol"], tx["trade_date"], tx["split_ratio"])
            if key in seen_splits:
                continue
            seen_splits.add(key)
        deduped_txs.append(tx)
    all_txs = deduped_txs

    first_date = date.fromisoformat(all_txs[0]["trade_date"])
    snap_end = end or date.today()

    # Pre-load all prices into memory: {symbol: {date_str: close}}
    all_symbols = list({tx["symbol"] for tx in all_txs if tx["symbol"]})
    prices_by_sym: dict[str, dict[str, float]] = {}
    for sym in all_symbols:
        if sym in MONEY_MARKET_TICKERS:
            continue  # money market funds always valued at $1/share
        rows = queries.get_daily_prices(conn, sym, first_date, snap_end)
        prices_by_sym[sym] = {r["price_date"]: r["close"] for r in rows}

    # Build split factors to reverse yfinance's split adjustment
    split_factors = _build_split_factors(all_txs)

    # Check for cost_basis_start and starting_cash overrides
    row = conn.execute(
        "SELECT cost_basis_start, starting_cash FROM portfolios WHERE portfolio_id = ?",
        (portfolio_id,),
    ).fetchone()
    cost_basis_start = row["cost_basis_start"] if row and row["cost_basis_start"] is not None else None
    starting_cash = row["starting_cash"] if row and row["starting_cash"] is not None else None

    # If the portfolio has no sweep transactions, accumulate dividends as cash
    # (Vanguard portfolios sweep dividends into VMFXX; others like AIL do not)
    has_sweeps = any(tx["tx_type"] in (TxType.SWEEP_IN.value, TxType.SWEEP_OUT.value) for tx in all_txs)

    # Always full-rebuild from first_date. Incremental resume was removed
    # because stored snapshots drift from yfinance's re-cached prices, and
    # the resume branch never reconsiders prior days. Full rebuild is cheap
    # enough and guarantees prices match the current cache.
    loop_start = first_date
    positions: dict[str, float] = {}
    vmfxx_balance = starting_cash or 0.0
    net_deposits = starting_cash or 0.0
    is_first_day = True
    last_price: dict[str, float] = {}
    last_price_date: dict[str, date] = {}
    prev_total_value = 0.0
    cumulative_twr = 0.0
    tx_idx = 0

    queries.delete_snapshots_from(conn, portfolio_id, first_date)
    # Seed money market prices. Do NOT set last_price_date — they never
    # receive daily updates, so a stored date would go stale and zero them out.
    for sym in all_symbols:
        if sym in MONEY_MARKET_TICKERS:
            last_price[sym] = 1.0

    result_rows = []
    current = loop_start
    while current <= snap_end:
        date_str = current.isoformat()

        # 1. Pre-transaction value: yesterday's positions at today's prices
        #    Use yesterday's unadjust factor so prices match pre-split positions
        prev_date_str = (current - timedelta(days=1)).isoformat()
        pre_tx_equity = 0.0
        for sym, held in positions.items():
            if held <= 0:
                continue
            stale = (current - last_price_date.get(sym, current)).days > _STALE_PRICE_DAYS
            if stale:
                continue  # delisted / no recent price — value at $0
            if sym in prices_by_sym and date_str in prices_by_sym[sym]:
                raw = prices_by_sym[sym][date_str]
                factor = _unadjust_factor(split_factors, sym, prev_date_str)
                pre_tx_equity += held * raw * factor
            else:
                pre_tx_equity += held * last_price.get(sym, 0.0)
        pre_tx_value = pre_tx_equity + max(vmfxx_balance, 0.0)

        # 2. Update prices for today (with today's factor, for total_value)
        for sym in all_symbols:
            if sym in prices_by_sym and date_str in prices_by_sym[sym]:
                raw = prices_by_sym[sym][date_str]
                factor = _unadjust_factor(split_factors, sym, date_str)
                last_price[sym] = raw * factor
                last_price_date[sym] = current

        # 3. Process all transactions on or before this date
        investment_income = 0.0  # dividends (positive) + fees (negative)
        while tx_idx < len(all_txs) and all_txs[tx_idx]["trade_date"] <= date_str:
            tx = all_txs[tx_idx]
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
                if symbol and symbol in positions:
                    positions[symbol] *= ratio

            # Track investment income (dividends positive, fees negative)
            if tx_type in (TxType.DIVIDEND.value, TxType.FEE.value):
                investment_income += tx["total_amount"] or 0.0
                # Portfolios without sweep accounts: dividends accumulate as cash
                if not has_sweeps and tx_type == TxType.DIVIDEND.value:
                    vmfxx_balance += tx["total_amount"] or 0.0

            tx_idx += 1

        # 4. Compute today's total value
        #    Zero out symbols with no price update in >45 days (likely delisted)
        equity_value = sum(
            held * (last_price.get(sym, 0.0)
                    if (current - last_price_date.get(sym, current)).days <= _STALE_PRICE_DAYS
                    else 0.0)
            for sym, held in positions.items() if held > 0
        )
        cash = max(vmfxx_balance, 0.0)  # clamp FP dust
        total_value = equity_value + cash

        # 5. Derive external cash flow and update net deposits
        external_cf = total_value - pre_tx_value - investment_income
        net_deposits += external_cf

        # Override net deposits on first day if cost_basis_start is set
        if is_first_day and cost_basis_start is not None:
            net_deposits = cost_basis_start
            is_first_day = False
        elif is_first_day:
            is_first_day = False

        # 6. Compute daily TWR
        if prev_total_value > 0:
            adjusted_base = prev_total_value + external_cf
            if adjusted_base > 0:
                daily_return = total_value / adjusted_base - 1
            else:
                daily_return = 0.0
            cumulative_twr = (1 + cumulative_twr) * (1 + daily_return) - 1
        prev_total_value = total_value

        queries.upsert_snapshot(
            conn, portfolio_id, current, total_value, net_deposits, cash, cumulative_twr
        )
        result_rows.append({
            "date": current,
            "total_value": total_value,
            "total_cost": net_deposits,
            "cash_balance": cash,
            "twr": cumulative_twr,
        })

        current += timedelta(days=1)

    conn.commit()
    return pd.DataFrame(result_rows)

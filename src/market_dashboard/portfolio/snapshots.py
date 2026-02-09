from __future__ import annotations

import sqlite3
from datetime import date, timedelta

import pandas as pd

from market_dashboard.portfolio import queries
from market_dashboard.portfolio.models import TxType


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
    start: date | None = None,
    end: date | None = None,
) -> pd.DataFrame:
    """Build daily portfolio valuations from transactions and prices.

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

    first_date = date.fromisoformat(all_txs[0]["trade_date"])
    snap_end = end or date.today()

    # Pre-load all prices into memory: {symbol: {date_str: close}}
    all_symbols = list({tx["symbol"] for tx in all_txs if tx["symbol"]})
    prices_by_sym: dict[str, dict[str, float]] = {}
    for sym in all_symbols:
        rows = queries.get_daily_prices(conn, sym, first_date, snap_end)
        prices_by_sym[sym] = {r["price_date"]: r["close"] for r in rows}

    # Build split factors to reverse yfinance's split adjustment
    split_factors = _build_split_factors(all_txs)

    # Always rebuild from the beginning for correct TWR and net deposits
    queries.delete_snapshots_from(conn, portfolio_id, first_date)

    # Replay positions day by day
    positions: dict[str, float] = {}   # symbol -> shares held
    vmfxx_balance = 0.0                # settlement fund balance from sweeps + VMFXX DRIPs
    net_deposits = 0.0                 # cumulative external cash flows
    last_price: dict[str, float] = {}  # symbol -> last known actual (unadjusted) close
    prev_total_value = 0.0
    cumulative_twr = 0.0
    tx_idx = 0

    result_rows = []
    current = first_date
    while current <= snap_end:
        date_str = current.isoformat()

        # 1. Pre-transaction value: yesterday's positions at today's prices
        #    Use yesterday's unadjust factor so prices match pre-split positions
        prev_date_str = (current - timedelta(days=1)).isoformat()
        pre_tx_equity = 0.0
        for sym, held in positions.items():
            if held <= 0:
                continue
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
                if symbol and symbol in positions:
                    positions[symbol] *= ratio

            # Track investment income (dividends positive, fees negative)
            if tx_type in (TxType.DIVIDEND.value, TxType.FEE.value):
                investment_income += tx["total_amount"] or 0.0

            tx_idx += 1

        # 4. Compute today's total value
        equity_value = sum(
            held * last_price.get(sym, 0.0)
            for sym, held in positions.items() if held > 0
        )
        cash = max(vmfxx_balance, 0.0)  # clamp FP dust
        total_value = equity_value + cash

        # 5. Derive external cash flow and update net deposits
        external_cf = total_value - pre_tx_value - investment_income
        net_deposits += external_cf

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

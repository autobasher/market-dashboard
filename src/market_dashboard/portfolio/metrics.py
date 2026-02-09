from __future__ import annotations

from datetime import date

import pandas as pd
import pyxirr
import empyrical



def portfolio_xirr(
    snapshots: list,
    current_value: float,
    as_of_date: date,
    start_value: float | None = None,
    start_date: date | None = None,
) -> float | None:
    """Compute XIRR from external cash flows derived from snapshot net deposits.

    For sub-periods, pass start_value/start_date to treat the starting portfolio
    value as an initial investment.
    """
    if not snapshots:
        return None

    dates = []
    amounts = []

    # For sub-periods, the starting portfolio value is a negative cash flow
    if start_value is not None and start_date is not None:
        dates.append(start_date)
        amounts.append(-start_value)
        prev_deposits = snapshots[0]["total_cost"] if snapshots else 0.0
    else:
        prev_deposits = 0.0

    for snap in snapshots:
        delta = snap["total_cost"] - prev_deposits
        prev_deposits = snap["total_cost"]
        if abs(delta) > 0.01:
            dates.append(date.fromisoformat(snap["snap_date"]))
            amounts.append(-delta)

    dates.append(as_of_date)
    amounts.append(current_value)

    try:
        return pyxirr.xirr(dates, amounts)
    except Exception:
        return None


def time_weighted_return(daily_values: pd.Series) -> float | None:
    """Compute time-weighted return from a series of daily portfolio values.

    Annualized using geometric linking of daily returns.
    """
    if len(daily_values) < 2:
        return None
    daily_returns = daily_values.pct_change().dropna()
    if daily_returns.empty:
        return None
    # Chain-link: product of (1 + r) - 1
    total = (1 + daily_returns).prod() - 1
    n_days = (daily_values.index[-1] - daily_values.index[0]).days
    if n_days <= 0:
        return float(total)
    # Annualize
    annualized = (1 + total) ** (365.25 / n_days) - 1
    return float(annualized)


def sharpe_ratio(daily_returns: pd.Series, risk_free_rate: float = 0.0) -> float:
    return float(empyrical.sharpe_ratio(daily_returns, risk_free=risk_free_rate))


def max_drawdown(daily_returns: pd.Series) -> float:
    return float(empyrical.max_drawdown(daily_returns))


def annual_volatility(daily_returns: pd.Series) -> float:
    return float(empyrical.annual_volatility(daily_returns))


def total_return(current_value: float, total_cost_basis: float) -> float | None:
    if total_cost_basis == 0:
        return None
    return (current_value - total_cost_basis) / total_cost_basis


def unrealized_gain(
    lots: list,
    current_prices: dict[str, float],
) -> dict[str, float]:
    """Compute unrealized gain per symbol and total.

    lots: rows with symbol, shares_remaining, cost_basis_per_share
    current_prices: {symbol: current_price}

    Returns dict with per-symbol gains and a '_total' key.
    """
    gains: dict[str, float] = {}
    for lot in lots:
        sym = lot["symbol"]
        shares = lot["shares_remaining"]
        if shares <= 0:
            continue
        price = current_prices.get(sym, 0.0)
        gain = shares * (price - lot["cost_basis_per_share"])
        gains[sym] = gains.get(sym, 0.0) + gain

    gains["_total"] = sum(v for k, v in gains.items() if k != "_total")
    return gains


def realized_gain(
    disposals: list,
    symbol: str | None = None,
) -> float:
    """Sum realized gains from disposals, optionally filtered by symbol."""
    total = 0.0
    for d in disposals:
        if symbol and d.get("symbol") != symbol:
            continue
        total += d["realized_gain"]
    return total


def current_allocation(
    lots: list,
    current_prices: dict[str, float],
) -> dict[str, float]:
    """Compute current allocation as percentage of total value per symbol.

    Returns {symbol: fraction} where fractions sum to ~1.0.
    """
    values: dict[str, float] = {}
    for lot in lots:
        sym = lot["symbol"]
        shares = lot["shares_remaining"]
        if shares <= 0:
            continue
        price = current_prices.get(sym, 0.0)
        values[sym] = values.get(sym, 0.0) + shares * price

    total = sum(values.values())
    if total == 0:
        return {}
    return {sym: val / total for sym, val in values.items()}

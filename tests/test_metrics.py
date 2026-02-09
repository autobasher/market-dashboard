from datetime import date

import numpy as np
import pandas as pd
import pytest

from market_dashboard.portfolio.metrics import (
    portfolio_xirr,
    time_weighted_return,
    sharpe_ratio,
    max_drawdown,
    annual_volatility,
    total_return,
    unrealized_gain,
    realized_gain,
    current_allocation,
)


# --- XIRR ---

def test_xirr_simple():
    """Invest $1000 Jan 1, worth $1100 Dec 31 -> ~10% IRR."""
    snapshots = [
        {"snap_date": "2024-01-01", "total_cost": 1000.0},
    ]
    result = portfolio_xirr(snapshots, 1100.0, date(2024, 12, 31))
    assert result is not None
    assert result == pytest.approx(0.10, abs=0.02)


def test_xirr_multiple_flows():
    snapshots = [
        {"snap_date": "2024-01-01", "total_cost": 1000.0},
        {"snap_date": "2024-07-01", "total_cost": 1500.0},
    ]
    result = portfolio_xirr(snapshots, 1600.0, date(2024, 12, 31))
    assert result is not None
    assert result > 0  # should be positive return


def test_xirr_skips_zero_deltas():
    """Snapshots with no change in total_cost produce no extra cash flows."""
    snapshots = [
        {"snap_date": "2024-01-01", "total_cost": 1000.0},
        {"snap_date": "2024-06-01", "total_cost": 1000.0},
    ]
    result = portfolio_xirr(snapshots, 1100.0, date(2024, 12, 31))
    assert result is not None


# --- TWR ---

def test_twr_simple():
    """100 -> 110 over 365 days = ~10% annualized."""
    dates = pd.date_range("2024-01-01", periods=366, freq="D")
    values = pd.Series(
        np.linspace(100, 110, 366),
        index=dates,
    )
    result = time_weighted_return(values)
    assert result is not None
    assert result == pytest.approx(0.10, abs=0.02)


def test_twr_too_short():
    values = pd.Series([100.0], index=pd.date_range("2024-01-01", periods=1))
    assert time_weighted_return(values) is None


# --- Empyrical wrappers ---

def test_sharpe_ratio():
    np.random.seed(42)
    returns = pd.Series(np.random.normal(0.001, 0.02, 252))
    result = sharpe_ratio(returns)
    assert isinstance(result, float)


def test_max_drawdown_known():
    # Series that goes up then drops 20%
    returns = pd.Series([0.1, 0.1, -0.2, -0.05, 0.05])
    dd = max_drawdown(returns)
    assert dd < 0  # empyrical returns negative drawdowns
    assert dd >= -1.0


def test_annual_volatility():
    np.random.seed(42)
    returns = pd.Series(np.random.normal(0.0, 0.01, 252))
    vol = annual_volatility(returns)
    assert vol > 0
    # Daily vol ~0.01, annual ~0.01 * sqrt(252) ~= 0.159
    assert vol == pytest.approx(0.159, abs=0.05)


# --- Simple metrics ---

def test_total_return():
    assert total_return(1100.0, 1000.0) == pytest.approx(0.1)
    assert total_return(900.0, 1000.0) == pytest.approx(-0.1)
    assert total_return(100.0, 0.0) is None


def test_unrealized_gain():
    lots = [
        {"symbol": "VTI", "shares_remaining": 10.0, "cost_basis_per_share": 100.0},
        {"symbol": "VTI", "shares_remaining": 5.0, "cost_basis_per_share": 110.0},
        {"symbol": "VEA", "shares_remaining": 20.0, "cost_basis_per_share": 50.0},
    ]
    prices = {"VTI": 120.0, "VEA": 55.0}
    gains = unrealized_gain(lots, prices)

    # VTI: 10*(120-100) + 5*(120-110) = 200 + 50 = 250
    assert gains["VTI"] == pytest.approx(250.0)
    # VEA: 20*(55-50) = 100
    assert gains["VEA"] == pytest.approx(100.0)
    assert gains["_total"] == pytest.approx(350.0)


def test_realized_gain():
    disposals = [
        {"realized_gain": 100.0, "symbol": "VTI"},
        {"realized_gain": -20.0, "symbol": "VTI"},
        {"realized_gain": 50.0, "symbol": "VEA"},
    ]
    assert realized_gain(disposals) == pytest.approx(130.0)
    assert realized_gain(disposals, symbol="VTI") == pytest.approx(80.0)
    assert realized_gain(disposals, symbol="VEA") == pytest.approx(50.0)


def test_current_allocation():
    lots = [
        {"symbol": "VTI", "shares_remaining": 10.0},
        {"symbol": "VEA", "shares_remaining": 20.0},
    ]
    prices = {"VTI": 100.0, "VEA": 50.0}
    alloc = current_allocation(lots, prices)
    # VTI: 1000, VEA: 1000, total: 2000
    assert alloc["VTI"] == pytest.approx(0.5)
    assert alloc["VEA"] == pytest.approx(0.5)


def test_current_allocation_empty():
    assert current_allocation([], {}) == {}

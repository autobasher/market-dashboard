import sqlite3
from datetime import date
from unittest.mock import patch

import pandas as pd
import pytest

from market_dashboard.database import queries as db_queries
from market_dashboard.portfolio.schema import initialize_portfolio_schema
from market_dashboard.portfolio import queries
from market_dashboard.portfolio import prices
from market_dashboard.portfolio.prices import fetch_historical_prices


@pytest.fixture
def portfolio_db():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    db_queries.initialize(conn)
    initialize_portfolio_schema(conn)
    yield conn
    # Clear the high-water mark between tests
    prices._fetch_high_water.clear()


def _mock_download(symbol, start, end, progress=False, auto_adjust=False):
    """Return a small DataFrame mimicking yfinance output."""
    dates = pd.bdate_range(start=start, end=end)[:5]
    if len(dates) == 0:
        return pd.DataFrame()
    data = {
        "Close": [100.0 + i for i in range(len(dates))],
        "Adj Close": [100.0 + i for i in range(len(dates))],
        "Volume": [1000000] * len(dates),
        "Open": [99.0] * len(dates),
        "High": [101.0] * len(dates),
        "Low": [98.0] * len(dates),
    }
    return pd.DataFrame(data, index=dates)


@patch("market_dashboard.portfolio.prices.yf.download", side_effect=_mock_download)
def test_fetch_inserts_rows(mock_dl, portfolio_db):
    count = fetch_historical_prices(
        portfolio_db, "VTI", date(2024, 1, 1), date(2024, 1, 10)
    )
    assert count > 0
    mock_dl.assert_called_once()

    rows = queries.get_daily_prices(
        portfolio_db, "VTI", date(2024, 1, 1), date(2024, 1, 10)
    )
    assert len(rows) == count


@patch("market_dashboard.portfolio.prices.yf.download", side_effect=_mock_download)
def test_fetch_skips_cached(mock_dl, portfolio_db):
    # First fetch
    fetch_historical_prices(portfolio_db, "VTI", date(2024, 1, 1), date(2024, 1, 10))
    # Second fetch for same range
    count = fetch_historical_prices(portfolio_db, "VTI", date(2024, 1, 1), date(2024, 1, 10))
    assert count == 0
    # yf.download should only have been called once
    assert mock_dl.call_count == 1


@patch("market_dashboard.portfolio.prices.yf.download", return_value=pd.DataFrame())
def test_fetch_handles_empty(mock_dl, portfolio_db):
    count = fetch_historical_prices(
        portfolio_db, "FAKE", date(2024, 1, 1), date(2024, 1, 10)
    )
    assert count == 0

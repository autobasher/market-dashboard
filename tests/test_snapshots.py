import sqlite3
from datetime import date

import pytest

from market_dashboard.database import queries as db_queries
from market_dashboard.portfolio.schema import initialize_portfolio_schema
from market_dashboard.portfolio import queries
from market_dashboard.portfolio.snapshots import build_daily_snapshots


@pytest.fixture
def portfolio_db():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    db_queries.initialize(conn)
    initialize_portfolio_schema(conn)

    # Set up account, portfolio, and link them
    queries.insert_account(conn, "acct-1", "Test", "Vanguard")
    pid = queries.insert_portfolio(conn, "Test Portfolio")
    queries.add_account_to_portfolio(conn, pid, "acct-1")
    conn.commit()
    return conn, pid


def test_empty_portfolio(portfolio_db):
    conn, pid = portfolio_db
    df = build_daily_snapshots(conn, pid, date(2024, 1, 1), date(2024, 1, 5))
    assert df.empty


def test_snapshot_with_transactions(portfolio_db):
    conn, pid = portfolio_db

    # Insert a buy transaction
    queries.insert_transaction(
        conn, account_id="acct-1", trade_date=date(2024, 1, 2),
        tx_type="BUY", total_amount=-1000.0, symbol="VTI",
        shares=10.0, price_per_share=100.0,
    )
    # Insert a price for VTI
    queries.upsert_historical_price(conn, "VTI", date(2024, 1, 2), 100.0, 100.0, 1000000)
    queries.upsert_historical_price(conn, "VTI", date(2024, 1, 3), 105.0, 105.0, 1000000)
    conn.commit()

    df = build_daily_snapshots(conn, pid, date(2024, 1, 2), date(2024, 1, 3))
    assert len(df) == 2

    # Cash model tracks VMFXX settlement fund balance from sweeps only.
    # A bare BUY without corresponding SWEEP_OUT doesn't change cash_balance.
    row0 = df.iloc[0]
    assert row0["cash_balance"] == pytest.approx(0.0)

    # Day 2: equity only (no cash component), value = 10 * 105
    row1 = df.iloc[1]
    assert row1["total_value"] == pytest.approx(10 * 105.0)


def test_snapshots_cached(portfolio_db):
    conn, pid = portfolio_db

    queries.insert_transaction(
        conn, account_id="acct-1", trade_date=date(2024, 1, 2),
        tx_type="BUY", total_amount=-500.0, symbol="VTI",
        shares=5.0, price_per_share=100.0,
    )
    queries.upsert_historical_price(conn, "VTI", date(2024, 1, 2), 100.0, 100.0)
    conn.commit()

    # Build once
    build_daily_snapshots(conn, pid, date(2024, 1, 2), date(2024, 1, 2))
    cached = queries.get_snapshots(conn, pid)
    assert len(cached) == 1

    # Build again — should overwrite (delete + re-insert)
    build_daily_snapshots(conn, pid, date(2024, 1, 2), date(2024, 1, 2))
    cached2 = queries.get_snapshots(conn, pid)
    assert len(cached2) == 1

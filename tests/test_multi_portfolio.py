"""Tests for multi-portfolio support: scoped import, aggregates, cascade rebuild, deletion."""

import sqlite3
from datetime import date

import pytest

from market_dashboard.database import queries as db_queries
from market_dashboard.portfolio.schema import initialize_portfolio_schema
from market_dashboard.portfolio import queries
from market_dashboard.portfolio.snapshots import build_daily_snapshots


@pytest.fixture
def multi_db():
    """In-memory DB with schema, two accounts, two portfolios."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    db_queries.initialize(conn)
    initialize_portfolio_schema(conn)

    # Portfolio 1: acct-1
    queries.insert_account(conn, "acct-1", "Account 1", "Vanguard")
    pid1 = queries.insert_portfolio(conn, "Portfolio1")
    queries.add_account_to_portfolio(conn, pid1, "acct-1")

    # Portfolio 2: acct-2
    queries.insert_account(conn, "acct-2", "Account 2", "Vanguard")
    pid2 = queries.insert_portfolio(conn, "Portfolio2")
    queries.add_account_to_portfolio(conn, pid2, "acct-2")

    conn.commit()
    return conn, pid1, pid2


def test_get_all_portfolios(multi_db):
    conn, pid1, pid2 = multi_db
    all_p = queries.get_all_portfolios(conn)
    assert len(all_p) == 2
    names = [p["name"] for p in all_p]
    assert "Portfolio1" in names
    assert "Portfolio2" in names


def test_get_portfolio_by_name(multi_db):
    conn, pid1, pid2 = multi_db
    p = queries.get_portfolio_by_name(conn, "Portfolio1")
    assert p is not None
    assert p["portfolio_id"] == pid1

    missing = queries.get_portfolio_by_name(conn, "Nonexistent")
    assert missing is None


def test_insert_portfolio_is_aggregate(multi_db):
    conn, pid1, pid2 = multi_db
    agg_id = queries.insert_portfolio(conn, "Combined", is_aggregate=True)
    row = conn.execute(
        "SELECT is_aggregate FROM portfolios WHERE portfolio_id = ?", (agg_id,)
    ).fetchone()
    assert row["is_aggregate"] == 1


def test_aggregate_members(multi_db):
    conn, pid1, pid2 = multi_db
    agg_id = queries.insert_portfolio(conn, "Combined", is_aggregate=True)
    queries.add_aggregate_member(conn, agg_id, pid1)
    queries.add_aggregate_member(conn, agg_id, pid2)
    conn.commit()

    members = queries.get_aggregate_members(conn, agg_id)
    assert len(members) == 2
    member_ids = {m["portfolio_id"] for m in members}
    assert member_ids == {pid1, pid2}


def test_get_aggregates_containing(multi_db):
    conn, pid1, pid2 = multi_db
    agg_id = queries.insert_portfolio(conn, "Combined", is_aggregate=True)
    queries.add_aggregate_member(conn, agg_id, pid1)
    queries.add_aggregate_member(conn, agg_id, pid2)
    conn.commit()

    aggs = queries.get_aggregates_containing(conn, pid1)
    assert len(aggs) == 1
    assert aggs[0]["portfolio_id"] == agg_id

    # pid2 is also in the aggregate
    aggs2 = queries.get_aggregates_containing(conn, pid2)
    assert len(aggs2) == 1


def test_effective_account_ids_individual(multi_db):
    conn, pid1, pid2 = multi_db
    aids = queries.get_effective_account_ids(conn, pid1)
    assert aids == ["acct-1"]


def test_effective_account_ids_aggregate(multi_db):
    conn, pid1, pid2 = multi_db
    agg_id = queries.insert_portfolio(conn, "Combined", is_aggregate=True)
    queries.add_aggregate_member(conn, agg_id, pid1)
    queries.add_aggregate_member(conn, agg_id, pid2)
    conn.commit()

    aids = queries.get_effective_account_ids(conn, agg_id)
    assert set(aids) == {"acct-1", "acct-2"}


def test_get_stored_csv_per_account(multi_db):
    conn, pid1, pid2 = multi_db
    conn.execute(
        "INSERT INTO uploaded_csv (account_id, filename, content, account_name, cash_balance) "
        "VALUES (?, ?, ?, ?, ?)",
        ("acct-1", "file1.csv", b"data1", "Account 1", 100.0),
    )
    conn.execute(
        "INSERT INTO uploaded_csv (account_id, filename, content, account_name, cash_balance) "
        "VALUES (?, ?, ?, ?, ?)",
        ("acct-2", "file2.csv", b"data2", "Account 2", 200.0),
    )
    conn.commit()

    csv1 = queries.get_stored_csv(conn, "acct-1")
    assert csv1 is not None
    assert csv1["cash_balance"] == 100.0

    csv2 = queries.get_stored_csv(conn, "acct-2")
    assert csv2 is not None
    assert csv2["cash_balance"] == 200.0

    missing = queries.get_stored_csv(conn, "acct-999")
    assert missing is None


def test_delete_individual_portfolio(multi_db):
    conn, pid1, pid2 = multi_db
    # Add a transaction so there's data to verify cleanup
    queries.insert_transaction(
        conn, account_id="acct-1", trade_date=date(2024, 1, 2),
        tx_type="BUY", total_amount=-1000.0, symbol="VTI", shares=10.0,
    )
    conn.commit()

    queries.delete_portfolio(conn, pid1)
    conn.commit()

    # Portfolio gone
    assert queries.get_portfolio_by_name(conn, "Portfolio1") is None
    # Portfolio2 still exists
    assert queries.get_portfolio_by_name(conn, "Portfolio2") is not None
    # Snapshots for pid1 gone (none were created, but verify no error)
    assert queries.get_snapshots(conn, pid1) == []


def test_delete_aggregate_preserves_members(multi_db):
    conn, pid1, pid2 = multi_db
    agg_id = queries.insert_portfolio(conn, "Combined", is_aggregate=True)
    queries.add_aggregate_member(conn, agg_id, pid1)
    queries.add_aggregate_member(conn, agg_id, pid2)
    conn.commit()

    queries.delete_portfolio(conn, agg_id)
    conn.commit()

    # Aggregate gone
    assert queries.get_portfolio_by_name(conn, "Combined") is None
    # Members still exist
    assert queries.get_portfolio_by_name(conn, "Portfolio1") is not None
    assert queries.get_portfolio_by_name(conn, "Portfolio2") is not None


def test_scoped_data_isolation(multi_db):
    """Transactions in one account don't appear in another portfolio's queries."""
    conn, pid1, pid2 = multi_db
    queries.insert_transaction(
        conn, account_id="acct-1", trade_date=date(2024, 1, 2),
        tx_type="BUY", total_amount=-1000.0, symbol="VTI", shares=10.0,
    )
    queries.insert_transaction(
        conn, account_id="acct-2", trade_date=date(2024, 1, 3),
        tx_type="BUY", total_amount=-500.0, symbol="VOO", shares=5.0,
    )
    conn.commit()

    acct1_txs = queries.get_transactions(conn, account_id="acct-1")
    acct2_txs = queries.get_transactions(conn, account_id="acct-2")
    assert len(acct1_txs) == 1
    assert len(acct2_txs) == 1
    assert acct1_txs[0]["symbol"] == "VTI"
    assert acct2_txs[0]["symbol"] == "VOO"


def test_aggregate_snapshots(multi_db):
    """Aggregate portfolio snapshots cover both accounts' transactions."""
    conn, pid1, pid2 = multi_db

    # Transactions in both accounts
    queries.insert_transaction(
        conn, account_id="acct-1", trade_date=date(2024, 1, 2),
        tx_type="BUY", total_amount=-1000.0, symbol="VTI", shares=10.0,
    )
    queries.insert_transaction(
        conn, account_id="acct-2", trade_date=date(2024, 1, 2),
        tx_type="BUY", total_amount=-500.0, symbol="VOO", shares=5.0,
    )
    # Prices
    queries.upsert_historical_price(conn, "VTI", date(2024, 1, 2), 100.0, 100.0)
    queries.upsert_historical_price(conn, "VOO", date(2024, 1, 2), 100.0, 100.0)
    conn.commit()

    # Create aggregate
    agg_id = queries.insert_portfolio(conn, "Combined", is_aggregate=True)
    queries.add_aggregate_member(conn, agg_id, pid1)
    queries.add_aggregate_member(conn, agg_id, pid2)
    queries.add_account_to_portfolio(conn, agg_id, "acct-1")
    queries.add_account_to_portfolio(conn, agg_id, "acct-2")
    conn.commit()

    # Build individual snapshots first
    build_daily_snapshots(conn, pid1, date(2024, 1, 2), date(2024, 1, 2))
    build_daily_snapshots(conn, pid2, date(2024, 1, 2), date(2024, 1, 2))

    # Build aggregate
    df = build_daily_snapshots(conn, agg_id, date(2024, 1, 2), date(2024, 1, 2))
    assert len(df) == 1
    # Aggregate value = 10*100 + 5*100 = 1500
    assert df.iloc[0]["total_value"] == pytest.approx(1500.0)


def test_schema_migration_rename():
    """Test that 'My Portfolio' gets renamed to 'Ariel1' during migration."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    db_queries.initialize(conn)

    # Create old-style schema with "My Portfolio"
    initialize_portfolio_schema(conn)
    queries.insert_portfolio(conn, "My Portfolio")
    conn.commit()

    # Re-run initialization (simulates app restart)
    initialize_portfolio_schema(conn)

    assert queries.get_portfolio_by_name(conn, "My Portfolio") is None
    assert queries.get_portfolio_by_name(conn, "Ariel1") is not None

import sqlite3
from datetime import date

from market_dashboard.database import queries as db_queries
from market_dashboard.portfolio.schema import initialize_portfolio_schema
from market_dashboard.portfolio import queries


def _setup_db():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    db_queries.initialize(conn)
    initialize_portfolio_schema(conn)
    queries.insert_account(conn, "acct-1", "Test Account", "Vanguard")
    conn.commit()
    return conn


def test_update_transaction():
    conn = _setup_db()
    tx_id = queries.insert_transaction(
        conn, account_id="acct-1", trade_date=date(2024, 1, 15),
        tx_type="BUY", total_amount=-1000.0, symbol="VTI", shares=10.0,
        price_per_share=100.0,
    )
    queries.update_transaction(conn, tx_id, symbol="VOO", shares=5.0)
    conn.commit()

    row = conn.execute("SELECT * FROM transactions WHERE tx_id = ?", (tx_id,)).fetchone()
    assert row["symbol"] == "VOO"
    assert row["shares"] == 5.0
    assert row["total_amount"] == -1000.0  # unchanged


def test_update_transaction_ignores_unknown_fields():
    conn = _setup_db()
    tx_id = queries.insert_transaction(
        conn, account_id="acct-1", trade_date=date(2024, 1, 15),
        tx_type="BUY", total_amount=-500.0, symbol="VTI", shares=5.0,
    )
    queries.update_transaction(conn, tx_id, bogus_field="nope", symbol="VOO")
    conn.commit()

    row = conn.execute("SELECT * FROM transactions WHERE tx_id = ?", (tx_id,)).fetchone()
    assert row["symbol"] == "VOO"


def test_delete_transaction():
    conn = _setup_db()
    tx_id = queries.insert_transaction(
        conn, account_id="acct-1", trade_date=date(2024, 1, 15),
        tx_type="BUY", total_amount=-1000.0, symbol="VTI", shares=10.0,
    )
    conn.commit()
    assert conn.execute("SELECT COUNT(*) FROM transactions").fetchone()[0] == 1

    queries.delete_transaction(conn, tx_id)
    conn.commit()
    assert conn.execute("SELECT COUNT(*) FROM transactions").fetchone()[0] == 0

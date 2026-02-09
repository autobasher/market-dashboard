import sqlite3

import pytest

from market_dashboard.database import queries as db_queries
from market_dashboard.portfolio.schema import initialize_portfolio_schema
from market_dashboard.portfolio import queries


@pytest.fixture
def in_memory_db():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    db_queries.initialize(conn)
    return conn


@pytest.fixture
def portfolio_db():
    """In-memory DB with both schemas, a test account, portfolio, and link."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    db_queries.initialize(conn)
    initialize_portfolio_schema(conn)
    queries.insert_account(conn, "acct-1", "Test Account", "Vanguard")
    pid = queries.insert_portfolio(conn, "Test Portfolio")
    queries.add_account_to_portfolio(conn, pid, "acct-1")
    conn.commit()
    return conn, pid

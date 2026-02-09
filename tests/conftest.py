import sqlite3

import pytest

from market_dashboard.database import queries
from market_dashboard.portfolio.schema import initialize_portfolio_schema


@pytest.fixture
def in_memory_db():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    queries.initialize(conn)
    return conn


@pytest.fixture
def portfolio_db():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    queries.initialize(conn)
    initialize_portfolio_schema(conn)
    return conn

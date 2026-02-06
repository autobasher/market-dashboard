import sqlite3

import pytest

from market_dashboard.database import queries


@pytest.fixture
def in_memory_db():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    queries.initialize(conn)
    return conn

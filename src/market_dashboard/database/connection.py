from __future__ import annotations

import sqlite3
from pathlib import Path


def get_connection(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path), check_same_thread=False, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


import streamlit as st


@st.cache_resource
def get_app_connection() -> sqlite3.Connection:
    """Get a connection with both dashboard and portfolio schemas initialized.

    Cached per-process: every Streamlit rerun reuses the one connection instead
    of opening a new one. This prevents connections from piling up (each holding
    a WAL write lock) and made the ``id(conn)``-keyed migration guards unreliable.
    """
    from market_dashboard.config import Settings
    from market_dashboard.database.queries import initialize
    from market_dashboard.portfolio.schema import initialize_portfolio_schema

    settings = Settings()
    conn = get_connection(settings.db_path)
    initialize(conn)
    initialize_portfolio_schema(conn)
    return conn

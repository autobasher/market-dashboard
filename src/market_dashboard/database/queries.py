from __future__ import annotations

import sqlite3

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS latest_quotes (
    symbol      TEXT PRIMARY KEY,
    price       REAL,
    change_pct  REAL,
    market_time TEXT,
    fetched_at  TEXT NOT NULL DEFAULT (datetime('now'))
);
"""


def initialize(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA_SQL)


def upsert_quote(
    conn: sqlite3.Connection,
    symbol: str,
    price: float | None,
    change_pct: float | None,
    market_time: str,
) -> None:
    conn.execute(
        """INSERT OR REPLACE INTO latest_quotes
           (symbol, price, change_pct, market_time, fetched_at)
           VALUES (?, ?, ?, ?, datetime('now'))""",
        (symbol, price, change_pct, market_time),
    )


def get_all_quotes(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    return conn.execute(
        "SELECT symbol, price, change_pct, market_time FROM latest_quotes ORDER BY symbol"
    ).fetchall()


def get_reference_closes(
    conn: sqlite3.Connection,
    symbols: list[str],
    target_date: str,
) -> dict[str, float]:
    """Get the latest close on or before target_date for each symbol."""
    result: dict[str, float] = {}
    for sym in symbols:
        row = conn.execute(
            "SELECT close FROM historical_prices "
            "WHERE symbol = ? AND price_date <= ? "
            "ORDER BY price_date DESC LIMIT 1",
            (sym, target_date),
        ).fetchone()
        if row is not None:
            result[sym] = row["close"]
    return result

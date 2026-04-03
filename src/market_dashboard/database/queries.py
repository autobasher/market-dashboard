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


_initialized: set[int] = set()


def initialize(conn: sqlite3.Connection) -> None:
    key = id(conn)
    if key in _initialized:
        return
    conn.executescript(SCHEMA_SQL)
    _initialized.add(key)


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
    if not symbols:
        return {}
    placeholders = ",".join("?" * len(symbols))
    rows = conn.execute(
        f"SELECT hp.symbol, hp.close "
        f"FROM historical_prices hp "
        f"INNER JOIN ("
        f"  SELECT symbol, MAX(price_date) AS best_date "
        f"  FROM historical_prices "
        f"  WHERE symbol IN ({placeholders}) AND price_date <= ? "
        f"  GROUP BY symbol"
        f") best ON hp.symbol = best.symbol AND hp.price_date = best.best_date",
        [*symbols, target_date],
    ).fetchall()
    return {r["symbol"]: r["close"] for r in rows}

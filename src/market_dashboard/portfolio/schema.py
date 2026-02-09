from __future__ import annotations

import sqlite3

PORTFOLIO_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS accounts (
    account_id   TEXT PRIMARY KEY,
    name         TEXT NOT NULL,
    institution  TEXT NOT NULL DEFAULT '',
    account_type TEXT NOT NULL DEFAULT '',
    created_at   TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS portfolios (
    portfolio_id INTEGER PRIMARY KEY AUTOINCREMENT,
    name         TEXT NOT NULL UNIQUE,
    is_aggregate INTEGER NOT NULL DEFAULT 0,
    created_at   TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS aggregate_members (
    aggregate_id INTEGER NOT NULL REFERENCES portfolios(portfolio_id),
    member_id    INTEGER NOT NULL REFERENCES portfolios(portfolio_id),
    PRIMARY KEY (aggregate_id, member_id)
);

CREATE TABLE IF NOT EXISTS portfolio_accounts (
    portfolio_id INTEGER NOT NULL REFERENCES portfolios(portfolio_id),
    account_id   TEXT NOT NULL REFERENCES accounts(account_id),
    PRIMARY KEY (portfolio_id, account_id)
);

CREATE TABLE IF NOT EXISTS transactions (
    tx_id            INTEGER PRIMARY KEY AUTOINCREMENT,
    account_id       TEXT NOT NULL REFERENCES accounts(account_id),
    trade_date       TEXT NOT NULL,
    settlement_date  TEXT,
    tx_type          TEXT NOT NULL,
    symbol           TEXT,
    shares           REAL,
    price_per_share  REAL,
    total_amount     REAL NOT NULL,
    fees             REAL NOT NULL DEFAULT 0.0,
    split_ratio      REAL,
    raw_description  TEXT NOT NULL DEFAULT '',
    source_file      TEXT NOT NULL DEFAULT '',
    created_at       TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS lots (
    lot_id               INTEGER PRIMARY KEY AUTOINCREMENT,
    account_id           TEXT NOT NULL REFERENCES accounts(account_id),
    symbol               TEXT NOT NULL,
    acquired_date        TEXT NOT NULL,
    shares_acquired      REAL NOT NULL,
    shares_remaining     REAL NOT NULL,
    cost_basis_per_share REAL NOT NULL,
    total_cost_basis     REAL NOT NULL,
    source_tx_id         INTEGER REFERENCES transactions(tx_id),
    created_at           TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS lot_disposals (
    disposal_id     INTEGER PRIMARY KEY AUTOINCREMENT,
    sell_tx_id      INTEGER NOT NULL REFERENCES transactions(tx_id),
    lot_id          INTEGER NOT NULL REFERENCES lots(lot_id),
    shares_disposed REAL NOT NULL,
    cost_basis      REAL NOT NULL,
    proceeds        REAL NOT NULL,
    realized_gain   REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS historical_prices (
    symbol     TEXT NOT NULL,
    price_date TEXT NOT NULL,
    close      REAL NOT NULL,
    adj_close  REAL NOT NULL,
    volume     INTEGER,
    PRIMARY KEY (symbol, price_date)
);

CREATE TABLE IF NOT EXISTS portfolio_snapshots (
    portfolio_id INTEGER NOT NULL REFERENCES portfolios(portfolio_id),
    snap_date    TEXT NOT NULL,
    total_value  REAL NOT NULL,
    total_cost   REAL NOT NULL,
    cash_balance REAL NOT NULL DEFAULT 0.0,
    twr          REAL NOT NULL DEFAULT 0.0,
    computed_at  TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (portfolio_id, snap_date)
);

CREATE INDEX IF NOT EXISTS idx_transactions_account_date
    ON transactions(account_id, trade_date);

CREATE INDEX IF NOT EXISTS idx_lots_account_symbol
    ON lots(account_id, symbol);

CREATE INDEX IF NOT EXISTS idx_historical_prices_symbol
    ON historical_prices(symbol, price_date);

CREATE TABLE IF NOT EXISTS uploaded_csv (
    account_id   TEXT PRIMARY KEY,
    filename     TEXT NOT NULL,
    content      BLOB NOT NULL,
    account_name TEXT NOT NULL DEFAULT '',
    cash_balance REAL NOT NULL DEFAULT 0.0,
    uploaded_at  TEXT NOT NULL DEFAULT (datetime('now'))
);
"""


def initialize_portfolio_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(PORTFOLIO_SCHEMA_SQL)
    # Idempotent migrations for existing databases
    for stmt in [
        "ALTER TABLE uploaded_csv ADD COLUMN cash_balance REAL NOT NULL DEFAULT 0.0",
        "ALTER TABLE portfolio_snapshots ADD COLUMN twr REAL NOT NULL DEFAULT 0.0",
        "ALTER TABLE portfolios ADD COLUMN is_aggregate INTEGER NOT NULL DEFAULT 0",
    ]:
        try:
            conn.execute(stmt)
        except sqlite3.OperationalError:
            pass  # column already exists

    # Migrate uploaded_csv from single-row (id=1) to per-account PK
    _migrate_uploaded_csv(conn)

    # Rename "My Portfolio" to "Ariel1" if it exists
    conn.execute(
        "UPDATE portfolios SET name = 'Ariel1' WHERE name = 'My Portfolio'"
    )


def _migrate_uploaded_csv(conn: sqlite3.Connection) -> None:
    """Migrate uploaded_csv from old single-row format to per-account PK."""
    # Check if old format exists (has 'id' column)
    cols = [r[1] for r in conn.execute("PRAGMA table_info(uploaded_csv)").fetchall()]
    if "id" not in cols:
        return  # already new format

    # Read existing row if any
    old_row = conn.execute("SELECT * FROM uploaded_csv WHERE id = 1").fetchone()

    conn.execute("DROP TABLE uploaded_csv")
    conn.execute("""
        CREATE TABLE uploaded_csv (
            account_id   TEXT PRIMARY KEY,
            filename     TEXT NOT NULL,
            content      BLOB NOT NULL,
            account_name TEXT NOT NULL DEFAULT '',
            cash_balance REAL NOT NULL DEFAULT 0.0,
            uploaded_at  TEXT NOT NULL DEFAULT (datetime('now'))
        )
    """)

    if old_row:
        conn.execute(
            "INSERT INTO uploaded_csv (account_id, filename, content, account_name, cash_balance, uploaded_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (old_row["account_id"], old_row["filename"], old_row["content"],
             old_row["account_name"], old_row["cash_balance"], old_row["uploaded_at"]),
        )

from __future__ import annotations

import sqlite3
from datetime import date


# --- Accounts ---

def insert_account(
    conn: sqlite3.Connection,
    account_id: str,
    name: str,
    institution: str = "",
    account_type: str = "",
) -> None:
    conn.execute(
        "INSERT OR IGNORE INTO accounts (account_id, name, institution, account_type) "
        "VALUES (?, ?, ?, ?)",
        (account_id, name, institution, account_type),
    )


def get_account(conn: sqlite3.Connection, account_id: str) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT * FROM accounts WHERE account_id = ?", (account_id,)
    ).fetchone()


# --- Portfolios ---

def insert_portfolio(conn: sqlite3.Connection, name: str) -> int:
    cur = conn.execute("INSERT INTO portfolios (name) VALUES (?)", (name,))
    return cur.lastrowid


def add_account_to_portfolio(
    conn: sqlite3.Connection, portfolio_id: int, account_id: str
) -> None:
    conn.execute(
        "INSERT OR IGNORE INTO portfolio_accounts (portfolio_id, account_id) VALUES (?, ?)",
        (portfolio_id, account_id),
    )


def get_portfolio_accounts(
    conn: sqlite3.Connection, portfolio_id: int
) -> list[sqlite3.Row]:
    return conn.execute(
        "SELECT a.* FROM accounts a "
        "JOIN portfolio_accounts pa ON a.account_id = pa.account_id "
        "WHERE pa.portfolio_id = ?",
        (portfolio_id,),
    ).fetchall()


# --- Transactions ---

def insert_transaction(
    conn: sqlite3.Connection,
    account_id: str,
    trade_date: date,
    tx_type: str,
    total_amount: float,
    settlement_date: date | None = None,
    symbol: str | None = None,
    shares: float | None = None,
    price_per_share: float | None = None,
    fees: float = 0.0,
    split_ratio: float | None = None,
    raw_description: str = "",
    source_file: str = "",
) -> int | None:
    """Insert a transaction. Returns tx_id."""
    cur = conn.execute(
        "INSERT INTO transactions "
        "(account_id, trade_date, settlement_date, tx_type, symbol, shares, "
        "price_per_share, total_amount, fees, split_ratio, raw_description, source_file) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            account_id,
            trade_date.isoformat(),
            settlement_date.isoformat() if settlement_date else None,
            tx_type,
            symbol,
            shares,
            price_per_share,
            total_amount,
            fees,
            split_ratio,
            raw_description,
            source_file,
        ),
    )
    return cur.lastrowid


def get_transactions(
    conn: sqlite3.Connection,
    account_id: str | None = None,
    symbol: str | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
) -> list[sqlite3.Row]:
    clauses = []
    params: list = []
    if account_id:
        clauses.append("account_id = ?")
        params.append(account_id)
    if symbol:
        clauses.append("symbol = ?")
        params.append(symbol)
    if start_date:
        clauses.append("trade_date >= ?")
        params.append(start_date.isoformat())
    if end_date:
        clauses.append("trade_date <= ?")
        params.append(end_date.isoformat())
    where = " AND ".join(clauses)
    sql = "SELECT * FROM transactions"
    if where:
        sql += f" WHERE {where}"
    sql += " ORDER BY trade_date, tx_id"
    return conn.execute(sql, params).fetchall()


def update_transaction(conn: sqlite3.Connection, tx_id: int, **fields) -> None:
    """Update specific fields on a transaction by tx_id."""
    if not fields:
        return
    allowed = {
        "trade_date", "settlement_date", "tx_type", "symbol", "shares",
        "price_per_share", "total_amount", "fees", "split_ratio",
        "raw_description", "source_file",
    }
    to_set = {k: v for k, v in fields.items() if k in allowed}
    if not to_set:
        return
    set_clause = ", ".join(f"{col} = ?" for col in to_set)
    params = list(to_set.values()) + [tx_id]
    conn.execute(f"UPDATE transactions SET {set_clause} WHERE tx_id = ?", params)


def delete_transaction(conn: sqlite3.Connection, tx_id: int) -> None:
    """Delete a single transaction by tx_id."""
    conn.execute("DELETE FROM transactions WHERE tx_id = ?", (tx_id,))


# --- Lots ---

def insert_lot(
    conn: sqlite3.Connection,
    account_id: str,
    symbol: str,
    acquired_date: date,
    shares_acquired: float,
    cost_basis_per_share: float,
    source_tx_id: int | None = None,
) -> int:
    total_cost_basis = shares_acquired * cost_basis_per_share
    cur = conn.execute(
        "INSERT INTO lots "
        "(account_id, symbol, acquired_date, shares_acquired, shares_remaining, "
        "cost_basis_per_share, total_cost_basis, source_tx_id) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        (
            account_id,
            symbol,
            acquired_date.isoformat(),
            shares_acquired,
            shares_acquired,
            cost_basis_per_share,
            total_cost_basis,
            source_tx_id,
        ),
    )
    return cur.lastrowid


def update_lot_shares(
    conn: sqlite3.Connection, lot_id: int, shares_remaining: float
) -> None:
    conn.execute(
        "UPDATE lots SET shares_remaining = ? WHERE lot_id = ?",
        (shares_remaining, lot_id),
    )


def update_lot_split(
    conn: sqlite3.Connection,
    lot_id: int,
    shares_acquired: float,
    shares_remaining: float,
    cost_basis_per_share: float,
) -> None:
    conn.execute(
        "UPDATE lots SET shares_acquired = ?, shares_remaining = ?, "
        "cost_basis_per_share = ? WHERE lot_id = ?",
        (shares_acquired, shares_remaining, cost_basis_per_share, lot_id),
    )


def get_open_lots(
    conn: sqlite3.Connection,
    account_id: str,
    symbol: str,
) -> list[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM lots WHERE account_id = ? AND symbol = ? "
        "AND shares_remaining > 0 ORDER BY acquired_date ASC, lot_id ASC",
        (account_id, symbol),
    ).fetchall()


# --- Lot Disposals ---

def insert_disposal(
    conn: sqlite3.Connection,
    sell_tx_id: int,
    lot_id: int,
    shares_disposed: float,
    cost_basis: float,
    proceeds: float,
) -> int:
    realized_gain = proceeds - cost_basis
    cur = conn.execute(
        "INSERT INTO lot_disposals "
        "(sell_tx_id, lot_id, shares_disposed, cost_basis, proceeds, realized_gain) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (sell_tx_id, lot_id, shares_disposed, cost_basis, proceeds, realized_gain),
    )
    return cur.lastrowid


def get_disposals(
    conn: sqlite3.Connection,
    symbol: str | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
) -> list[sqlite3.Row]:
    sql = (
        "SELECT ld.*, t.symbol, t.trade_date FROM lot_disposals ld "
        "JOIN transactions t ON ld.sell_tx_id = t.tx_id"
    )
    clauses = []
    params: list = []
    if symbol:
        clauses.append("t.symbol = ?")
        params.append(symbol)
    if start_date:
        clauses.append("t.trade_date >= ?")
        params.append(start_date.isoformat())
    if end_date:
        clauses.append("t.trade_date <= ?")
        params.append(end_date.isoformat())
    if clauses:
        sql += " WHERE " + " AND ".join(clauses)
    return conn.execute(sql, params).fetchall()


# --- Historical Prices ---

def upsert_historical_price(
    conn: sqlite3.Connection,
    symbol: str,
    price_date: date,
    close: float,
    adj_close: float,
    volume: int | None = None,
) -> None:
    conn.execute(
        "INSERT OR REPLACE INTO historical_prices "
        "(symbol, price_date, close, adj_close, volume) VALUES (?, ?, ?, ?, ?)",
        (symbol, price_date.isoformat(), close, adj_close, volume),
    )


def get_daily_prices(
    conn: sqlite3.Connection,
    symbol: str,
    start: date,
    end: date,
) -> list[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM historical_prices "
        "WHERE symbol = ? AND price_date >= ? AND price_date <= ? "
        "ORDER BY price_date",
        (symbol, start.isoformat(), end.isoformat()),
    ).fetchall()


def get_cached_price_range(
    conn: sqlite3.Connection, symbol: str
) -> tuple[str | None, str | None]:
    """Return (min_date, max_date) cached for a symbol, or (None, None)."""
    row = conn.execute(
        "SELECT MIN(price_date), MAX(price_date) FROM historical_prices WHERE symbol = ?",
        (symbol,),
    ).fetchone()
    return (row[0], row[1]) if row else (None, None)


# --- Portfolio Snapshots ---

def upsert_snapshot(
    conn: sqlite3.Connection,
    portfolio_id: int,
    snap_date: date,
    total_value: float,
    total_cost: float,
    cash_balance: float = 0.0,
    twr: float = 0.0,
) -> None:
    conn.execute(
        "INSERT OR REPLACE INTO portfolio_snapshots "
        "(portfolio_id, snap_date, total_value, total_cost, cash_balance, twr, computed_at) "
        "VALUES (?, ?, ?, ?, ?, ?, datetime('now'))",
        (portfolio_id, snap_date.isoformat(), total_value, total_cost, cash_balance, twr),
    )


def get_snapshots(
    conn: sqlite3.Connection,
    portfolio_id: int,
    start: date | None = None,
    end: date | None = None,
) -> list[sqlite3.Row]:
    sql = "SELECT * FROM portfolio_snapshots WHERE portfolio_id = ?"
    params: list = [portfolio_id]
    if start:
        sql += " AND snap_date >= ?"
        params.append(start.isoformat())
    if end:
        sql += " AND snap_date <= ?"
        params.append(end.isoformat())
    sql += " ORDER BY snap_date"
    return conn.execute(sql, params).fetchall()


def delete_snapshots_from(
    conn: sqlite3.Connection, portfolio_id: int, from_date: date
) -> None:
    conn.execute(
        "DELETE FROM portfolio_snapshots WHERE portfolio_id = ? AND snap_date >= ?",
        (portfolio_id, from_date.isoformat()),
    )


# --- Convenience queries ---

def get_all_open_lots(
    conn: sqlite3.Connection, account_ids: list[str]
) -> list[sqlite3.Row]:
    all_lots = []
    for acct_id in account_ids:
        rows = conn.execute(
            "SELECT * FROM lots WHERE account_id = ? AND shares_remaining > 0",
            (acct_id,),
        ).fetchall()
        all_lots.extend(rows)
    return all_lots


def get_latest_prices(
    conn: sqlite3.Connection, symbols: list[str]
) -> dict[str, float]:
    prices = {}
    for sym in symbols:
        row = conn.execute(
            "SELECT close FROM historical_prices "
            "WHERE symbol = ? ORDER BY price_date DESC LIMIT 1",
            (sym,),
        ).fetchone()
        if row:
            prices[sym] = row["close"]
    return prices


def get_default_portfolio_id(conn: sqlite3.Connection) -> int | None:
    row = conn.execute("SELECT portfolio_id FROM portfolios LIMIT 1").fetchone()
    return row["portfolio_id"] if row else None


def get_all_account_ids(conn: sqlite3.Connection) -> list[str]:
    rows = conn.execute("SELECT account_id FROM accounts").fetchall()
    return [r["account_id"] for r in rows]


def get_stored_csv(conn: sqlite3.Connection) -> dict | None:
    row = conn.execute("SELECT * FROM uploaded_csv WHERE id = 1").fetchone()
    return dict(row) if row else None


# --- Lot Rebuild ---

def delete_lots_and_disposals(
    conn: sqlite3.Connection,
    account_id: str,
    symbol: str | None = None,
) -> None:
    """Delete all lots and their disposals for an account (optionally filtered by symbol)."""
    if symbol:
        lot_ids = conn.execute(
            "SELECT lot_id FROM lots WHERE account_id = ? AND symbol = ?",
            (account_id, symbol),
        ).fetchall()
    else:
        lot_ids = conn.execute(
            "SELECT lot_id FROM lots WHERE account_id = ?", (account_id,)
        ).fetchall()

    ids = [r["lot_id"] for r in lot_ids]
    if ids:
        placeholders = ",".join("?" * len(ids))
        conn.execute(
            f"DELETE FROM lot_disposals WHERE lot_id IN ({placeholders})", ids
        )
        conn.execute(f"DELETE FROM lots WHERE lot_id IN ({placeholders})", ids)

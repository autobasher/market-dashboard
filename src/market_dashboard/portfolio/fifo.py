from __future__ import annotations

import logging
import sqlite3
from datetime import date

from market_dashboard.portfolio.models import TxType
from market_dashboard.portfolio import queries

logger = logging.getLogger(__name__)


def process_buy(conn: sqlite3.Connection, tx: sqlite3.Row, tx_id: int) -> None:
    shares = tx["shares"]
    cost = abs(tx["total_amount"]) + abs(tx["fees"])
    cost_basis_per_share = cost / shares if shares else 0.0
    queries.insert_lot(
        conn,
        account_id=tx["account_id"],
        symbol=tx["symbol"],
        acquired_date=date.fromisoformat(tx["trade_date"]),
        shares_acquired=shares,
        cost_basis_per_share=cost_basis_per_share,
        source_tx_id=tx_id,
    )


def process_drip(conn: sqlite3.Connection, tx: sqlite3.Row, tx_id: int) -> None:
    # DRIP creates a new lot at the reinvestment price, same as a buy
    process_buy(conn, tx, tx_id)


def process_sell(conn: sqlite3.Connection, tx: sqlite3.Row, tx_id: int) -> None:
    shares_to_sell = abs(tx["shares"])
    proceeds = abs(tx["total_amount"]) - abs(tx["fees"])
    price_per_share = proceeds / shares_to_sell if shares_to_sell else 0.0

    open_lots = queries.get_open_lots(conn, tx["account_id"], tx["symbol"])
    remaining = shares_to_sell

    for lot in open_lots:
        if remaining <= 0:
            break
        available = lot["shares_remaining"]
        disposed = min(available, remaining)
        lot_proceeds = disposed * price_per_share
        lot_cost = disposed * lot["cost_basis_per_share"]

        queries.insert_disposal(
            conn,
            sell_tx_id=tx_id,
            lot_id=lot["lot_id"],
            shares_disposed=disposed,
            cost_basis=lot_cost,
            proceeds=lot_proceeds,
        )
        queries.update_lot_shares(conn, lot["lot_id"], available - disposed)
        remaining -= disposed

    if remaining > 1e-9:
        logger.warning(
            "Insufficient shares for SELL: %s %s, short %.6f shares",
            tx["account_id"], tx["symbol"], remaining,
        )


def process_split(conn: sqlite3.Connection, tx: sqlite3.Row) -> None:
    ratio = tx["split_ratio"]
    if not ratio or ratio <= 0:
        return
    open_lots = queries.get_open_lots(conn, tx["account_id"], tx["symbol"])
    for lot in open_lots:
        new_acquired = lot["shares_acquired"] * ratio
        new_remaining = lot["shares_remaining"] * ratio
        new_cost_per = lot["cost_basis_per_share"] / ratio
        queries.update_lot_split(conn, lot["lot_id"], new_acquired, new_remaining, new_cost_per)


def process_transfer_in(conn: sqlite3.Connection, tx: sqlite3.Row, tx_id: int) -> None:
    shares = abs(tx["shares"]) if tx["shares"] else 0.0
    if tx["price_per_share"]:
        cost_basis_per_share = tx["price_per_share"]
    elif shares and tx["total_amount"]:
        cost_basis_per_share = abs(tx["total_amount"]) / shares
    else:
        cost_basis_per_share = 0.0

    queries.insert_lot(
        conn,
        account_id=tx["account_id"],
        symbol=tx["symbol"],
        acquired_date=date.fromisoformat(tx["trade_date"]),
        shares_acquired=shares,
        cost_basis_per_share=cost_basis_per_share,
        source_tx_id=tx_id,
    )


def process_transfer_out(conn: sqlite3.Connection, tx: sqlite3.Row, tx_id: int) -> None:
    shares_to_remove = abs(tx["shares"]) if tx["shares"] else 0.0
    open_lots = queries.get_open_lots(conn, tx["account_id"], tx["symbol"])
    remaining = shares_to_remove

    for lot in open_lots:
        if remaining <= 0:
            break
        available = lot["shares_remaining"]
        disposed = min(available, remaining)
        queries.update_lot_shares(conn, lot["lot_id"], available - disposed)
        remaining -= disposed

    if remaining > 1e-9:
        logger.warning(
            "Insufficient shares for TRANSFER_OUT: %s %s, short %.6f shares",
            tx["account_id"], tx["symbol"], remaining,
        )


_TX_HANDLERS = {
    TxType.BUY.value: process_buy,
    TxType.DRIP.value: process_drip,
    TxType.SELL.value: process_sell,
    TxType.TRANSFER_IN.value: process_transfer_in,
    TxType.TRANSFER_OUT.value: process_transfer_out,
}


def rebuild_lots(
    conn: sqlite3.Connection,
    account_id: str,
    symbol: str | None = None,
) -> None:
    """Delete all lots/disposals and replay transactions chronologically."""
    queries.delete_lots_and_disposals(conn, account_id, symbol)

    txs = queries.get_transactions(conn, account_id=account_id, symbol=symbol)
    for tx in txs:
        tx_type = tx["tx_type"]

        if tx_type == TxType.SPLIT.value:
            if tx["symbol"] and tx["split_ratio"]:
                process_split(conn, tx)
            continue

        # Skip transactions with no symbol or no shares — can't create lots
        if not tx["symbol"] or not tx["shares"]:
            continue

        # DIVIDEND and FEE have no lot impact
        if tx_type in (TxType.DIVIDEND.value, TxType.FEE.value):
            continue

        handler = _TX_HANDLERS.get(tx_type)
        if handler:
            handler(conn, tx, tx["tx_id"])

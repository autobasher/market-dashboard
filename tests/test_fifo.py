import sqlite3
from datetime import date

import pytest

from market_dashboard.database import queries as db_queries
from market_dashboard.portfolio.schema import initialize_portfolio_schema
from market_dashboard.portfolio import queries
from market_dashboard.portfolio.fifo import (
    process_buy,
    process_drip,
    process_sell,
    process_split,
    process_transfer_in,
    process_transfer_out,
    rebuild_lots,
)


@pytest.fixture
def portfolio_db():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    db_queries.initialize(conn)
    initialize_portfolio_schema(conn)
    queries.insert_account(conn, "acct-1", "Test Account", "Vanguard")
    conn.commit()
    return conn


def _insert_tx(conn, **kwargs):
    """Helper to insert a transaction and return (tx_row, tx_id)."""
    defaults = dict(
        account_id="acct-1",
        trade_date=date(2024, 1, 15),
        tx_type="BUY",
        total_amount=-1000.0,
        symbol="VTI",
        shares=10.0,
        price_per_share=100.0,
        fees=0.0,
    )
    defaults.update(kwargs)
    tx_id = queries.insert_transaction(conn, **defaults)
    conn.commit()
    row = conn.execute("SELECT * FROM transactions WHERE tx_id = ?", (tx_id,)).fetchone()
    return row, tx_id


def test_buy_creates_lot(portfolio_db):
    tx, tx_id = _insert_tx(portfolio_db, total_amount=-1000.0, shares=10.0, fees=0.0)
    process_buy(portfolio_db, tx, tx_id)
    lots = queries.get_open_lots(portfolio_db, "acct-1", "VTI")
    assert len(lots) == 1
    assert lots[0]["shares_remaining"] == 10.0
    assert lots[0]["cost_basis_per_share"] == 100.0
    assert lots[0]["total_cost_basis"] == 1000.0


def test_buy_with_fees(portfolio_db):
    tx, tx_id = _insert_tx(portfolio_db, total_amount=-1000.0, shares=10.0, fees=10.0)
    process_buy(portfolio_db, tx, tx_id)
    lots = queries.get_open_lots(portfolio_db, "acct-1", "VTI")
    # cost = abs(-1000) + abs(10) = 1010 / 10 shares = 101
    assert lots[0]["cost_basis_per_share"] == pytest.approx(101.0)


def test_drip_creates_lot(portfolio_db):
    tx, tx_id = _insert_tx(
        portfolio_db, tx_type="DRIP", total_amount=-15.50, shares=0.07,
        price_per_share=221.43, fees=0.0,
    )
    process_drip(portfolio_db, tx, tx_id)
    lots = queries.get_open_lots(portfolio_db, "acct-1", "VTI")
    assert len(lots) == 1
    assert lots[0]["shares_remaining"] == pytest.approx(0.07)


def test_sell_fifo_order(portfolio_db):
    # Buy lot 1: 10 shares @ $100
    tx1, id1 = _insert_tx(
        portfolio_db, trade_date=date(2024, 1, 1),
        total_amount=-1000.0, shares=10.0, fees=0.0,
    )
    process_buy(portfolio_db, tx1, id1)

    # Buy lot 2: 5 shares @ $120
    tx2, id2 = _insert_tx(
        portfolio_db, trade_date=date(2024, 2, 1),
        total_amount=-600.0, shares=5.0, fees=0.0,
    )
    process_buy(portfolio_db, tx2, id2)

    # Sell 12 shares @ $130
    tx3, id3 = _insert_tx(
        portfolio_db, trade_date=date(2024, 3, 1), tx_type="SELL",
        total_amount=1560.0, shares=12.0, fees=0.0,
    )
    process_sell(portfolio_db, tx3, id3)

    lots = queries.get_open_lots(portfolio_db, "acct-1", "VTI")
    # Lot 1 fully depleted, lot 2 has 3 remaining
    assert len(lots) == 1
    assert lots[0]["shares_remaining"] == pytest.approx(3.0)

    disposals = portfolio_db.execute("SELECT * FROM lot_disposals ORDER BY disposal_id").fetchall()
    assert len(disposals) == 2
    # First disposal: 10 shares from lot 1, cost basis = 10*100 = 1000
    assert disposals[0]["shares_disposed"] == pytest.approx(10.0)
    assert disposals[0]["cost_basis"] == pytest.approx(1000.0)
    # Second disposal: 2 shares from lot 2, cost basis = 2*120 = 240
    assert disposals[1]["shares_disposed"] == pytest.approx(2.0)
    assert disposals[1]["cost_basis"] == pytest.approx(240.0)


def test_sell_partial(portfolio_db):
    tx1, id1 = _insert_tx(portfolio_db, total_amount=-1000.0, shares=10.0, fees=0.0)
    process_buy(portfolio_db, tx1, id1)

    tx2, id2 = _insert_tx(
        portfolio_db, trade_date=date(2024, 2, 1), tx_type="SELL",
        total_amount=300.0, shares=3.0, fees=0.0,
    )
    process_sell(portfolio_db, tx2, id2)

    lots = queries.get_open_lots(portfolio_db, "acct-1", "VTI")
    assert lots[0]["shares_remaining"] == pytest.approx(7.0)


def test_split_adjusts_lots(portfolio_db):
    tx1, id1 = _insert_tx(portfolio_db, total_amount=-1000.0, shares=10.0, fees=0.0)
    process_buy(portfolio_db, tx1, id1)

    # 2:1 split
    split_tx, _ = _insert_tx(
        portfolio_db, trade_date=date(2024, 2, 1), tx_type="SPLIT",
        total_amount=0.0, shares=None, split_ratio=2.0, fees=0.0,
    )
    process_split(portfolio_db, split_tx)

    lots = queries.get_open_lots(portfolio_db, "acct-1", "VTI")
    assert lots[0]["shares_remaining"] == pytest.approx(20.0)
    assert lots[0]["shares_acquired"] == pytest.approx(20.0)
    assert lots[0]["cost_basis_per_share"] == pytest.approx(50.0)
    # Total cost basis unchanged (stored at insert, cost_per_share * original_shares)
    assert lots[0]["total_cost_basis"] == pytest.approx(1000.0)


def test_transfer_in_creates_lot(portfolio_db):
    tx, tx_id = _insert_tx(
        portfolio_db, tx_type="TRANSFER_IN",
        total_amount=0.0, shares=5.0, price_per_share=200.0, fees=0.0,
    )
    process_transfer_in(portfolio_db, tx, tx_id)
    lots = queries.get_open_lots(portfolio_db, "acct-1", "VTI")
    assert len(lots) == 1
    assert lots[0]["cost_basis_per_share"] == pytest.approx(200.0)


def test_transfer_out_depletes_fifo(portfolio_db):
    tx1, id1 = _insert_tx(portfolio_db, total_amount=-1000.0, shares=10.0, fees=0.0)
    process_buy(portfolio_db, tx1, id1)

    tx2, id2 = _insert_tx(
        portfolio_db, trade_date=date(2024, 2, 1), tx_type="TRANSFER_OUT",
        total_amount=0.0, shares=3.0, fees=0.0,
    )
    process_transfer_out(portfolio_db, tx2, id2)

    lots = queries.get_open_lots(portfolio_db, "acct-1", "VTI")
    assert lots[0]["shares_remaining"] == pytest.approx(7.0)
    # No disposals created for transfers
    disposals = portfolio_db.execute("SELECT * FROM lot_disposals").fetchall()
    assert len(disposals) == 0


def test_rebuild_lots_idempotent(portfolio_db):
    # Insert buy and sell
    tx1, id1 = _insert_tx(
        portfolio_db, trade_date=date(2024, 1, 1),
        total_amount=-1000.0, shares=10.0, fees=0.0,
    )
    process_buy(portfolio_db, tx1, id1)

    tx2, id2 = _insert_tx(
        portfolio_db, trade_date=date(2024, 2, 1), tx_type="SELL",
        total_amount=600.0, shares=5.0, fees=0.0,
    )
    process_sell(portfolio_db, tx2, id2)

    # Snapshot state
    lots_before = queries.get_open_lots(portfolio_db, "acct-1", "VTI")
    disposals_before = portfolio_db.execute("SELECT * FROM lot_disposals").fetchall()

    # Rebuild and verify identical
    rebuild_lots(portfolio_db, "acct-1")
    lots_after = queries.get_open_lots(portfolio_db, "acct-1", "VTI")
    disposals_after = portfolio_db.execute("SELECT * FROM lot_disposals").fetchall()

    assert len(lots_before) == len(lots_after)
    assert lots_before[0]["shares_remaining"] == pytest.approx(lots_after[0]["shares_remaining"])
    assert len(disposals_before) == len(disposals_after)


def test_sell_insufficient_shares_warns(portfolio_db, caplog):
    tx1, id1 = _insert_tx(portfolio_db, total_amount=-500.0, shares=5.0, fees=0.0)
    process_buy(portfolio_db, tx1, id1)

    # Try to sell 10 shares when only 5 exist
    tx2, id2 = _insert_tx(
        portfolio_db, trade_date=date(2024, 2, 1), tx_type="SELL",
        total_amount=1000.0, shares=10.0, fees=0.0,
    )
    import logging
    with caplog.at_level(logging.WARNING):
        process_sell(portfolio_db, tx2, id2)

    assert "Insufficient shares" in caplog.text

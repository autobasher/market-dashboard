"""Full pipeline integration test: parse -> insert -> rebuild lots -> prices -> snapshots -> metrics."""

import io
from datetime import date

import pytest

from market_dashboard.portfolio import queries
from market_dashboard.portfolio.parsers import parse_vanguard_csv
from market_dashboard.portfolio.fifo import rebuild_lots
from market_dashboard.portfolio.snapshots import build_daily_snapshots
from market_dashboard.portfolio import metrics

INTEGRATION_CSV = """\
Trade Date,Settlement Date,Transaction Type,Transaction Description,Symbol,Shares,Share Price,Commission,Fees,Net Amount
01/02/2024,01/04/2024,Buy,Buy,VTI,10.0,$100.00,$0.00,$0.00,-$1000.00
02/01/2024,02/03/2024,Buy,Buy,VTI,5.0,$105.00,$0.00,$0.00,-$525.00
03/01/2024,,Dividend,Dividend,VTI,,,$0.00,$0.00,$12.00
04/01/2024,04/03/2024,Sell,Sell,VTI,-3.0,$110.00,$0.00,$0.00,$330.00
"""


def test_full_pipeline(portfolio_db):
    conn, pid = portfolio_db

    # 1. Parse CSV
    txs = parse_vanguard_csv(io.StringIO(INTEGRATION_CSV), "acct-1")
    assert len(txs) == 4

    # 2. Insert transactions
    for tx in txs:
        queries.insert_transaction(
            conn,
            account_id=tx.account_id,
            trade_date=tx.trade_date,
            tx_type=tx.tx_type.value,
            total_amount=tx.total_amount,
            settlement_date=tx.settlement_date,
            symbol=tx.symbol,
            shares=tx.shares,
            price_per_share=tx.price_per_share,
            fees=tx.fees,
            raw_description=tx.raw_description,
            source_file=tx.source_file,
        )
    conn.commit()

    # 3. Rebuild lots
    rebuild_lots(conn, "acct-1")
    conn.commit()

    open_lots = queries.get_open_lots(conn, "acct-1", "VTI")
    # Started with 10+5=15, sold 3 FIFO -> lot1 has 7, lot2 has 5
    total_remaining = sum(lot["shares_remaining"] for lot in open_lots)
    assert total_remaining == pytest.approx(12.0)

    # 4. Check disposals
    disposals = queries.get_disposals(conn)
    assert len(disposals) == 1  # single lot touched (FIFO from lot 1)
    assert disposals[0]["shares_disposed"] == pytest.approx(3.0)
    # Cost basis: 3 * 100 = 300, proceeds: 330, gain: 30
    assert disposals[0]["realized_gain"] == pytest.approx(30.0)

    # 5. Insert mock prices and build snapshots
    for d in range(1, 6):
        queries.upsert_historical_price(
            conn, "VTI", date(2024, 4, d), 110.0 + d, 110.0 + d
        )
    conn.commit()

    df = build_daily_snapshots(conn, pid, date(2024, 4, 1), date(2024, 4, 5))
    # Snapshots rebuild from first transaction date (Jan 2) through Apr 5
    assert len(df) >= 5
    # Verify the April rows with prices have positive values
    apr_df = df[df["date"] >= date(2024, 4, 1)]
    assert len(apr_df) == 5
    assert all(apr_df["total_value"] > 0)

    # 6. Compute metrics
    current_value = df.iloc[-1]["total_value"]

    snap_rows = queries.get_snapshots(conn, pid)
    xirr = metrics.portfolio_xirr(snap_rows, current_value, date(2024, 4, 5))
    # XIRR may return None if solver can't converge on sparse test data
    assert xirr is None or isinstance(xirr, float)

    tr = metrics.total_return(current_value, df.iloc[-1]["total_cost"])
    assert tr is not None

    prices = {"VTI": 115.0}
    ug = metrics.unrealized_gain(open_lots, prices)
    assert "_total" in ug

    rg = metrics.realized_gain(disposals)
    assert rg == pytest.approx(30.0)

    alloc = metrics.current_allocation(open_lots, prices)
    assert "VTI" in alloc
    assert alloc["VTI"] == pytest.approx(1.0)


def test_reimport_wipe_and_reinsert(portfolio_db):
    """Re-importing via wipe+reinsert produces the same transaction count."""
    conn, pid = portfolio_db

    txs = parse_vanguard_csv(io.StringIO(INTEGRATION_CSV), "acct-1")
    for tx in txs:
        queries.insert_transaction(
            conn,
            account_id=tx.account_id,
            trade_date=tx.trade_date,
            tx_type=tx.tx_type.value,
            total_amount=tx.total_amount,
            symbol=tx.symbol,
            shares=tx.shares,
            price_per_share=tx.price_per_share,
            fees=tx.fees,
        )
    conn.commit()

    count_before = conn.execute("SELECT COUNT(*) FROM transactions").fetchone()[0]

    # Wipe and re-import (mirrors the real import path)
    conn.execute("DELETE FROM transactions")
    conn.commit()

    txs2 = parse_vanguard_csv(io.StringIO(INTEGRATION_CSV), "acct-1")
    for tx in txs2:
        queries.insert_transaction(
            conn,
            account_id=tx.account_id,
            trade_date=tx.trade_date,
            tx_type=tx.tx_type.value,
            total_amount=tx.total_amount,
            symbol=tx.symbol,
            shares=tx.shares,
            price_per_share=tx.price_per_share,
            fees=tx.fees,
        )
    conn.commit()

    count_after = conn.execute("SELECT COUNT(*) FROM transactions").fetchone()[0]
    assert count_before == count_after

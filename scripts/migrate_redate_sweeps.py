"""One-shot migration: re-date settlement-paired SWEEP_OUT/SWEEP_IN rows
to the trade date of the BUY/SELL they offset.

Background: Vanguard records BUY/SELL on trade date but the offsetting
VMFXX sweep settles T+1. snapshots.py walks by trade date, so positions
go up on day T while VMFXX hasn't been debited yet — producing 1-day
spikes in total_value that immediately revert. This migration runs the
same redate logic the parser now applies to fresh imports, but on
existing DB rows.

Idempotent: re-running is a no-op (sweeps already on the trade date
won't move).
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from datetime import date

from market_dashboard.config import Settings
from market_dashboard.database.connection import get_connection
from market_dashboard.portfolio.models import Transaction, TxType
from market_dashboard.portfolio.parsers import redate_settlement_sweeps


def main() -> None:
    settings = Settings()
    conn = get_connection(settings.db_path)

    rows = conn.execute(
        "SELECT tx_id, account_id, trade_date, settlement_date, tx_type, "
        "symbol, shares, price_per_share, total_amount, fees, split_ratio, "
        "raw_description, source_file FROM transactions"
    ).fetchall()

    txs: list[Transaction] = []
    for r in rows:
        txs.append(Transaction(
            tx_id=r["tx_id"],
            account_id=r["account_id"],
            trade_date=date.fromisoformat(r["trade_date"]),
            settlement_date=date.fromisoformat(r["settlement_date"]) if r["settlement_date"] else None,
            tx_type=TxType(r["tx_type"]),
            symbol=r["symbol"],
            shares=r["shares"],
            price_per_share=r["price_per_share"],
            total_amount=r["total_amount"] or 0.0,
            fees=r["fees"] or 0.0,
            split_ratio=r["split_ratio"],
            raw_description=r["raw_description"] or "",
            source_file=r["source_file"] or "",
        ))

    redated = redate_settlement_sweeps(txs)

    changes = [
        (new.trade_date.isoformat(), new.tx_id)
        for old, new in zip(txs, redated)
        if old.trade_date != new.trade_date
    ]

    print(f"Total transactions: {len(txs)}")
    print(f"Sweeps to re-date: {len(changes)}")

    if not changes:
        print("Nothing to do.")
        return

    # Show first 20 for sanity
    by_id = {t.tx_id: t for t in txs}
    for new_date, tx_id in changes[:20]:
        old = by_id[tx_id]
        print(f"  tx_id={tx_id}  {old.account_id}  {old.tx_type.value}  "
              f"${old.total_amount:>+12,.2f}  {old.trade_date} -> {new_date}")
    if len(changes) > 20:
        print(f"  ... and {len(changes) - 20} more")

    conn.executemany(
        "UPDATE transactions SET trade_date = ? WHERE tx_id = ?", changes
    )
    conn.commit()
    print(f"Updated {len(changes)} rows.")


if __name__ == "__main__":
    main()

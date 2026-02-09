"""Migration: insert sweep transactions from the uploaded CSV, fix VMMXX->VMRXX conversion
shares (tx 529/530), backfill VMMXX $1.00 prices, and rebuild portfolio snapshots.
Created ~Feb 7, 2026 when cash/settlement-fund model was overhauled.
ONE-TIME: safe to delete after migration is verified (idempotent but no longer needed).
"""

import csv
import io
import sqlite3
from datetime import date, timedelta
from pathlib import Path

# Add src to path for imports
import sys
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from market_dashboard.portfolio.parsers import parse_vanguard_csv
from market_dashboard.portfolio.snapshots import build_daily_snapshots

DB = Path(__file__).resolve().parent.parent / "data" / "market_dashboard.db"

conn = sqlite3.connect(DB)
conn.row_factory = sqlite3.Row
conn.execute("PRAGMA journal_mode=WAL")

ACCOUNT_ID = "Ariel1"
PORTFOLIO_ID = 4

# --- Step 1: Insert sweep transactions from CSV ---

# Check how many sweeps already exist
existing = conn.execute(
    "SELECT COUNT(*) FROM transactions WHERE tx_type IN ('SWEEP_IN','SWEEP_OUT')"
).fetchone()[0]
print(f"Existing sweep transactions: {existing}")

if existing == 0:
    raw = conn.execute("SELECT content FROM uploaded_csv WHERE id=1").fetchone()[0]
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8")

    # Use the updated parser which now includes sweeps
    txns = parse_vanguard_csv(io.StringIO(raw), ACCOUNT_ID)
    sweep_txns = [t for t in txns if t.tx_type.value in ("SWEEP_IN", "SWEEP_OUT")]

    for t in sweep_txns:
        conn.execute(
            "INSERT INTO transactions "
            "(account_id, trade_date, settlement_date, tx_type, symbol, shares, "
            "price_per_share, total_amount, fees, split_ratio, raw_description, source_file) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                t.account_id,
                t.trade_date.isoformat(),
                t.settlement_date.isoformat() if t.settlement_date else None,
                t.tx_type.value,
                t.symbol,
                t.shares,
                t.price_per_share,
                t.total_amount,
                t.fees,
                t.split_ratio,
                t.raw_description,
                t.source_file,
            ),
        )
    conn.commit()
    print(f"Inserted {len(sweep_txns)} sweep transactions")
else:
    print("Sweeps already exist, skipping insert")

# --- Step 2: Fix VMMXX→VMRXX conversion (shares=NULL → 28000) ---

r529 = conn.execute("SELECT shares FROM transactions WHERE tx_id=529").fetchone()
if r529 and r529["shares"] is None:
    conn.execute("UPDATE transactions SET shares=28000.0 WHERE tx_id=529")
    conn.execute("UPDATE transactions SET shares=28000.0 WHERE tx_id=530")
    conn.commit()
    print("Fixed VMMXX->VMRXX conversion: set shares=28000 on tx_id 529 and 530")
else:
    print(f"VMMXX conversion already fixed (shares={r529['shares'] if r529 else 'missing'})")

# --- Step 3: Add VMMXX $1 prices ---

vmmxx_prices = conn.execute(
    "SELECT COUNT(*) FROM historical_prices WHERE symbol='VMMXX'"
).fetchone()[0]

if vmmxx_prices == 0:
    # Get the date range for VMMXX holding
    start = date(2020, 1, 24)  # first VMMXX transaction
    end = date(2020, 8, 31)    # conversion date
    current = start
    count = 0
    while current <= end:
        conn.execute(
            "INSERT OR IGNORE INTO historical_prices (symbol, price_date, close, adj_close) "
            "VALUES ('VMMXX', ?, 1.0, 1.0)",
            (current.isoformat(),),
        )
        current += timedelta(days=1)
        count += 1
    conn.commit()
    print(f"Added {count} VMMXX price records at $1.00")
else:
    print(f"VMMXX already has {vmmxx_prices} price records")

# --- Step 4: Rebuild snapshots ---

print("\nRebuilding daily snapshots...")
df = build_daily_snapshots(conn, PORTFOLIO_ID)
print(f"Built {len(df)} daily snapshots")

# --- Step 5: Validate ---

latest = conn.execute(
    "SELECT snap_date, total_value, total_cost, cash_balance FROM portfolio_snapshots "
    "WHERE portfolio_id=? ORDER BY snap_date DESC LIMIT 1",
    (PORTFOLIO_ID,),
).fetchone()

target_cash = conn.execute(
    "SELECT cash_balance FROM uploaded_csv WHERE id=1"
).fetchone()[0]

print(f"\nLatest snapshot: {latest['snap_date']}")
print(f"  total_value:  ${latest['total_value']:,.2f}")
print(f"  total_cost:   ${latest['total_cost']:,.2f}")
print(f"  cash_balance: ${latest['cash_balance']:,.2f}")
print(f"  target cash:  ${target_cash:,.2f}")
print(f"  cash diff:    ${latest['cash_balance'] - target_cash:,.2f}")

# Check earliest snapshot too
earliest = conn.execute(
    "SELECT snap_date, total_value, total_cost, cash_balance FROM portfolio_snapshots "
    "WHERE portfolio_id=? ORDER BY snap_date ASC LIMIT 1",
    (PORTFOLIO_ID,),
).fetchone()
print(f"\nEarliest snapshot: {earliest['snap_date']}")
print(f"  total_value:  ${earliest['total_value']:,.2f}")
print(f"  cash_balance: ${earliest['cash_balance']:,.2f}")

conn.close()

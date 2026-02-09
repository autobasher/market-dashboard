"""Rebuild all portfolio snapshots from scratch and spot-check key date ranges:
OUST split day (Apr 2023), July 2022 deposit, and the final snapshot values.
Created ~Feb 8, 2026 during snapshot rebuild iteration.
REUSABLE: run after any change to snapshot logic to verify nothing regressed.
"""
from datetime import date
from market_dashboard.config import Settings
from market_dashboard.database.connection import get_connection
from market_dashboard.database import queries as dq
from market_dashboard.portfolio.schema import initialize_portfolio_schema
from market_dashboard.portfolio.snapshots import build_daily_snapshots

settings = Settings()
conn = get_connection(settings.db_path)
dq.initialize(conn)
initialize_portfolio_schema(conn)

portfolio_id = conn.execute("SELECT portfolio_id FROM portfolios LIMIT 1").fetchone()[0]
print(f"Rebuilding snapshots for portfolio {portfolio_id}...")

txs = conn.execute("SELECT MIN(trade_date) FROM transactions").fetchone()[0]
earliest = date.fromisoformat(txs)
today = date.today()

build_daily_snapshots(conn, portfolio_id, earliest, today)

# Check key dates
print("\n=== OUST split day (Apr 20-22, 2023) ===")
rows = conn.execute(
    "SELECT snap_date, total_value, total_cost, cash_balance, twr "
    "FROM portfolio_snapshots WHERE portfolio_id = ? "
    "AND snap_date BETWEEN '2023-04-19' AND '2023-04-25' ORDER BY snap_date",
    (portfolio_id,),
).fetchall()
prev_dep = None
for r in rows:
    dep_chg = r["total_cost"] - prev_dep if prev_dep is not None else 0
    prev_dep = r["total_cost"]
    print(f"  {r['snap_date']}  val={r['total_value']:>11,.0f}  dep={r['total_cost']:>11,.0f}  chg={dep_chg:>10,.0f}  cash={r['cash_balance']:>8,.0f}  twr={r['twr']*100:>7.1f}%")

# Check final values
last = conn.execute(
    "SELECT snap_date, total_value, total_cost, cash_balance, twr "
    "FROM portfolio_snapshots WHERE portfolio_id = ? ORDER BY snap_date DESC LIMIT 1",
    (portfolio_id,),
).fetchone()
print(f"\n=== Final snapshot ===")
print(f"  Date: {last['snap_date']}")
print(f"  Value: ${last['total_value']:,.2f}")
print(f"  Net deposits: ${last['total_cost']:,.0f}")
print(f"  Cash: ${last['cash_balance']:,.2f}")
print(f"  TWR: {last['twr']*100:.1f}%")

# Check July 2022 (the other big deposit)
print("\n=== July 2022 deposit ===")
rows = conn.execute(
    "SELECT snap_date, total_value, total_cost, cash_balance, twr "
    "FROM portfolio_snapshots WHERE portfolio_id = ? "
    "AND snap_date BETWEEN '2022-07-27' AND '2022-07-31' ORDER BY snap_date",
    (portfolio_id,),
).fetchall()
for r in rows:
    print(f"  {r['snap_date']}  val={r['total_value']:>11,.0f}  dep={r['total_cost']:>11,.0f}  cash={r['cash_balance']:>8,.0f}  twr={r['twr']*100:>7.1f}%")

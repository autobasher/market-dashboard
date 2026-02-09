"""Check TWR against Ariel's Excel reference by listing large daily deposit changes and all
stock splits. Validates that final TWR and net deposits are in the right ballpark.
Created ~Feb 8, 2026 during TWR validation.
ONE-TIME: safe to delete after TWR is confirmed matching Excel.
"""
from market_dashboard.config import Settings
from market_dashboard.database.connection import get_connection
from market_dashboard.database import queries as dq
from market_dashboard.portfolio.schema import initialize_portfolio_schema

settings = Settings()
conn = get_connection(settings.db_path)
dq.initialize(conn)
initialize_portfolio_schema(conn)

# Large daily external_cf events (net_deposits changes > $10K in a day)
rows = conn.execute("""
    SELECT s1.snap_date, s1.total_value, s1.total_cost,
           s1.total_cost - s2.total_cost as dep_change,
           s1.twr, s1.cash_balance
    FROM portfolio_snapshots s1
    JOIN portfolio_snapshots s2
      ON s1.portfolio_id = s2.portfolio_id
      AND date(s1.snap_date) = date(s2.snap_date, '+1 day')
    WHERE s1.portfolio_id = 4
      AND ABS(s1.total_cost - s2.total_cost) > 10000
    ORDER BY s1.snap_date
""").fetchall()

print(f"{'Date':<12} {'Value':>12} {'NetDep':>12} {'DepChg':>12} {'Cash':>10} {'TWR%':>8}")
print("-" * 70)
for r in rows:
    print(f"{r['snap_date']:<12} {r['total_value']:>12,.0f} {r['total_cost']:>12,.0f} {r['dep_change']:>12,.0f} {r['cash_balance']:>10,.0f} {r['twr']*100:>7.1f}%")

# Check: do any other symbols have splits?
print("\n=== ALL SPLITS ===")
splits = conn.execute("""
    SELECT tx_id, trade_date, symbol, shares, split_ratio
    FROM transactions WHERE tx_type = 'SPLIT'
    ORDER BY trade_date, tx_id
""").fetchall()
for s in splits:
    print(f"  {s['tx_id']:>5} {s['trade_date']} {s['symbol'] or 'NULL':>8} ratio={s['split_ratio']}  shares={s['shares']}")

# Total net deposits at end
last = conn.execute(
    "SELECT total_cost, twr FROM portfolio_snapshots WHERE portfolio_id = 4 ORDER BY snap_date DESC LIMIT 1"
).fetchone()
print(f"\nFinal net_deposits: ${last['total_cost']:,.0f}")
print(f"Final TWR: {last['twr']*100:.1f}%")
print(f"Excel reference cumulative returns: ~$1,027,300 on ~$1.69M deposits")
print(f"Excel implied return: {1027300/1690000*100:.0f}% (very rough)")

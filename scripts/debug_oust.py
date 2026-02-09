"""Trace OUST position and value through the April 2023 10:1 reverse split.
Validates share counts, unadjust factors, and portfolio value continuity across the split date.
Created ~Feb 7-8, 2026 during portfolio snapshot debugging.
ONE-TIME: safe to delete after OUST split logic is verified correct.
"""
from market_dashboard.config import Settings
from market_dashboard.database.connection import get_connection
from market_dashboard.database import queries as dq
from market_dashboard.portfolio.schema import initialize_portfolio_schema

settings = Settings()
conn = get_connection(settings.db_path)
dq.initialize(conn)
initialize_portfolio_schema(conn)

# Sum all OUST shares before the split (pre April 21 2023)
buys = conn.execute(
    "SELECT SUM(shares) FROM transactions "
    "WHERE symbol = 'OUST' AND tx_type = 'BUY' AND trade_date < '2023-04-21'"
).fetchone()[0] or 0

sells = conn.execute(
    "SELECT SUM(shares) FROM transactions "
    "WHERE symbol = 'OUST' AND tx_type = 'SELL' AND trade_date < '2023-04-21'"
).fetchone()[0] or 0

transfer = conn.execute(
    "SELECT SUM(shares) FROM transactions "
    "WHERE symbol = 'OUST' AND tx_type = 'TRANSFER_IN' AND trade_date < '2023-04-21'"
).fetchone()[0] or 0

pre_split_shares = transfer + buys - sells
print(f"OUST pre-split position: transfer={transfer:.0f} + buys={buys:.0f} - sells={sells:.0f} = {pre_split_shares:.0f} shares")

# Only tx 1730 has ratio=0.1 (10:1 reverse split), the rest have None -> treated as 1.0
post_split_shares = pre_split_shares * 0.1
print(f"After 10:1 reverse split: {pre_split_shares:.0f} * 0.1 = {post_split_shares:.0f} shares")

# OUST prices around the split
for d in ['2023-04-19', '2023-04-20', '2023-04-21', '2023-04-24']:
    row = conn.execute(
        "SELECT close FROM historical_prices WHERE symbol = ? AND price_date = ?",
        ('OUST', d)
    ).fetchone()
    price = row["close"] if row else "NO DATA"
    print(f"OUST price {d}: {price}")

# Now check the _unadjust_factor logic
# yfinance gives split-adjusted prices. For OUST the split is 0.1 (reverse split).
# The _build_split_factors logic: walks from last split backward
# For a 0.1 split on 2023-04-21:
#   cum starts at 1.0
#   At split_date='2023-04-21', ratio=0.1: intervals.append(('2023-04-21', 1.0)), cum = 1.0 * 0.1 = 0.1
#   intervals.append(('0000-00-00', 0.1))
#   After reverse: [('0000-00-00', 0.1), ('2023-04-21', 1.0)]
# So: before split date -> factor=0.1, on/after split date -> factor=1.0
# Meaning: pre-split prices get multiplied by 0.1, post-split by 1.0

# If yfinance gives split-adjusted prices, pre-split close should be ~$3.50 (adjusted for 10:1 reverse)
# And we multiply by 0.1 to get actual ~$0.35
# Post-split close should be ~$3.50 (no adjustment needed, factor=1.0)

# Check what snapshots.py actually computes for OUST value
# On Apr 20: pre_split_shares * (yf_close * 0.1)
# On Apr 21: post_split_shares * (yf_close * 1.0)

# Let's get the raw yfinance prices
for d in ['2023-04-19', '2023-04-20', '2023-04-21', '2023-04-24']:
    row = conn.execute(
        "SELECT close, adj_close FROM historical_prices WHERE symbol = ? AND price_date = ?",
        ('OUST', d)
    ).fetchone()
    if row:
        print(f"OUST {d}: close={row['close']:.4f}  adj_close={row['adj_close']:.4f}")

# Value check
print()
print("=== VALUE CHECK ===")
# Before split (Apr 20): shares * close * unadjust_factor
# unadjust_factor for dates < 2023-04-21 = 0.1
# After split (Apr 21): shares * close * unadjust_factor
# unadjust_factor for dates >= 2023-04-21 = 1.0

row_pre = conn.execute(
    "SELECT close FROM historical_prices WHERE symbol = 'OUST' AND price_date = '2023-04-20'"
).fetchone()
row_post = conn.execute(
    "SELECT close FROM historical_prices WHERE symbol = 'OUST' AND price_date = '2023-04-24'"
).fetchone()

if row_pre and row_post:
    val_pre = pre_split_shares * row_pre["close"] * 0.1
    val_post = post_split_shares * row_post["close"] * 1.0
    print(f"Apr 20: {pre_split_shares:.0f} sh * {row_pre['close']:.4f} * 0.1 = ${val_pre:,.0f}")
    print(f"Apr 24: {post_split_shares:.0f} sh * {row_post['close']:.4f} * 1.0 = ${val_post:,.0f}")
    print(f"Value change: ${val_post - val_pre:,.0f}")

# Also check: what does the snapshot say about the TOTAL portfolio?
print()
print("=== SNAPSHOT VALUE CHANGE Apr 20 -> Apr 21 ===")
for d in ['2023-04-20', '2023-04-21']:
    row = conn.execute(
        "SELECT total_value, total_cost, cash_balance FROM portfolio_snapshots "
        "WHERE portfolio_id = 4 AND snap_date = ?", (d,)
    ).fetchone()
    if row:
        print(f"{d}: val={row['total_value']:,.0f}  dep={row['total_cost']:,.0f}  cash={row['cash_balance']:,.0f}")

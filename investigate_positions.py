import sqlite3
from datetime import date, timedelta

conn = sqlite3.connect(r"C:\Users\ariel\Desktop\Claude Projects\market-dashboard\data\market_dashboard.db")
conn.row_factory = sqlite3.Row

# 1. Show ALL split transactions with their ratios
cur = conn.execute("SELECT * FROM transactions WHERE tx_type = 'SPLIT' ORDER BY trade_date, tx_id")
splits = [dict(r) for r in cur.fetchall()]
print("=== ALL SPLIT transactions ===")
for s in splits:
    desc = s["raw_description"][:100] if s["raw_description"] else "N/A"
    sym = s["symbol"] or "NONE"
    print("  tx_id={} {} {:10} ratio={}  desc: {}".format(s["tx_id"], s["trade_date"], sym, s["split_ratio"], desc))

# 2. Show all OUST transactions
cur = conn.execute("SELECT * FROM transactions WHERE symbol = 'OUST' ORDER BY trade_date, tx_id")
oust_txs = [dict(r) for r in cur.fetchall()]
print("\n=== ALL OUST transactions ({}) ===".format(len(oust_txs)))
for t in oust_txs:
    desc = (t["raw_description"] or "")[:100]
    print("  {} {:15} shares={}  pps={}  total={}  split_ratio={}  desc: {}".format(
        t["trade_date"], t["tx_type"], t["shares"], t["price_per_share"],
        t["total_amount"], t["split_ratio"], desc))

# 3. Replay ALL positions through today and show final state
all_txs_cur = conn.execute("SELECT * FROM transactions ORDER BY trade_date, tx_id")
all_txs = [dict(r) for r in all_txs_cur.fetchall()]

positions = {}
cash = 0.0
for tx in all_txs:
    sym = tx["symbol"]
    shares = tx["shares"] or 0.0
    tt = tx["tx_type"]
    cash += tx["total_amount"] or 0.0
    
    if tt in ("BUY", "DRIP"):
        if sym and shares:
            positions[sym] = positions.get(sym, 0.0) + shares
    elif tt == "SELL":
        if sym:
            positions[sym] = positions.get(sym, 0.0) - shares
    elif tt == "TRANSFER_IN":
        if sym and shares:
            positions[sym] = positions.get(sym, 0.0) + shares
    elif tt == "TRANSFER_OUT":
        if sym and shares:
            positions[sym] = positions.get(sym, 0.0) - shares
    elif tt == "SPLIT":
        ratio = tx["split_ratio"] or 1.0
        if sym and sym in positions:
            positions[sym] *= ratio

print("\n=== Final positions (non-zero) ===")
print("Cash: {:,.2f}".format(cash))
for sym, held in sorted(positions.items(), key=lambda x: -abs(x[1])):
    if abs(held) > 0.001:
        cur = conn.execute("SELECT close FROM historical_prices WHERE symbol = ? ORDER BY price_date DESC LIMIT 1", (sym,))
        row = cur.fetchone()
        price = row["close"] if row else 0
        value = held * price
        print("  {:10}: {:>12,.4f} shares  x  {:>10,.4f}  =  {:>14,.2f}".format(sym, held, price, value))

# 4. Look at symbols with None split ratio that might need fixing
print("\n=== Splits with None ratio ===")
for s in splits:
    if s["split_ratio"] is None:
        sym = s["symbol"] or "NONE"
        print("  tx_id={} {} {:10}  desc: {}".format(s["tx_id"], s["trade_date"], sym, s["raw_description"]))

# 5. Show OUST price history to understand the split
print("\n=== OUST prices around split dates ===")
cur = conn.execute("""
    SELECT price_date, close FROM historical_prices 
    WHERE symbol = 'OUST' 
    AND (price_date BETWEEN '2023-03-15' AND '2023-05-01'
         OR price_date BETWEEN '2021-03-01' AND '2021-03-31')
    ORDER BY price_date
""")
for r in cur.fetchall():
    print("  {}: {:.4f}".format(r["price_date"], r["close"]))

conn.close()

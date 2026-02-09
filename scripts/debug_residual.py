"""Replay all transactions up to Apr 21, 2023 to trace how the residual external cash flow
is computed on the OUST split day. Diagnoses phantom deposits caused by split-day value jumps.
Created ~Feb 8, 2026 during TWR/net-deposits debugging.
ONE-TIME: safe to delete after residual cash flow logic is verified correct.
"""
from market_dashboard.config import Settings
from market_dashboard.database.connection import get_connection
from market_dashboard.database import queries as dq
from market_dashboard.portfolio.schema import initialize_portfolio_schema
from market_dashboard.portfolio import queries
from market_dashboard.portfolio.models import TxType
from market_dashboard.portfolio.snapshots import _build_split_factors, _unadjust_factor
from datetime import date

settings = Settings()
conn = get_connection(settings.db_path)
dq.initialize(conn)
initialize_portfolio_schema(conn)

# Get all transactions and replay up to April 20, then show what happens on April 21
all_txs = queries.get_transactions(conn, account_id="Ariel1")
all_txs = [dict(r) for r in all_txs]
all_txs.sort(key=lambda t: (t["trade_date"], t["tx_id"]))

first_date = date.fromisoformat(all_txs[0]["trade_date"])
all_symbols = list({tx["symbol"] for tx in all_txs if tx["symbol"]})

# Load prices
prices_by_sym = {}
for sym in all_symbols:
    rows = queries.get_daily_prices(conn, sym, first_date, date(2023, 5, 1))
    prices_by_sym[sym] = {r["price_date"]: r["close"] for r in rows}

split_factors = _build_split_factors(all_txs)

# Show OUST split factors
print("OUST split factors:", split_factors.get("OUST", "NONE"))
print()

# Replay to get state at end of April 20
positions = {}
vmfxx_balance = 0.0
last_price = {}
tx_idx = 0

target = "2023-04-20"
from datetime import timedelta
current = first_date
while current.isoformat() <= target:
    date_str = current.isoformat()
    # Update prices
    for sym in all_symbols:
        if sym in prices_by_sym and date_str in prices_by_sym[sym]:
            raw = prices_by_sym[sym][date_str]
            factor = _unadjust_factor(split_factors, sym, date_str)
            last_price[sym] = raw * factor

    # Process transactions
    while tx_idx < len(all_txs) and all_txs[tx_idx]["trade_date"] <= date_str:
        tx = all_txs[tx_idx]
        tx_type = tx["tx_type"]
        symbol = tx["symbol"]
        shares = tx["shares"] or 0.0

        if tx_type == TxType.SWEEP_IN.value:
            vmfxx_balance += abs(tx["total_amount"] or 0.0)
        elif tx_type == TxType.SWEEP_OUT.value:
            vmfxx_balance -= abs(tx["total_amount"] or 0.0)
        elif tx_type in (TxType.BUY.value, TxType.DRIP.value):
            if tx_type == TxType.DRIP.value and symbol == "VMFXX":
                vmfxx_balance += abs(tx["total_amount"] or 0.0)
            elif symbol and shares:
                positions[symbol] = positions.get(symbol, 0.0) + shares
        elif tx_type == TxType.SELL.value:
            if symbol:
                positions[symbol] = positions.get(symbol, 0.0) - shares
        elif tx_type == TxType.TRANSFER_IN.value:
            if symbol and shares:
                positions[symbol] = positions.get(symbol, 0.0) + shares
        elif tx_type == TxType.TRANSFER_OUT.value:
            if symbol and shares:
                positions[symbol] = positions.get(symbol, 0.0) - shares
        elif tx_type == TxType.SPLIT.value:
            ratio = tx["split_ratio"] or 1.0
            if symbol and symbol in positions:
                positions[symbol] *= ratio

        tx_idx += 1
    current += timedelta(days=1)

print(f"=== STATE AT END OF {target} ===")
print(f"OUST position: {positions.get('OUST', 0):.0f} shares")
print(f"OUST last_price: {last_price.get('OUST', 0):.4f}")
print(f"OUST value: {positions.get('OUST', 0) * last_price.get('OUST', 0):,.0f}")
print(f"VMFXX balance: {vmfxx_balance:,.2f}")

total_equity_apr20 = sum(
    held * last_price.get(sym, 0.0) for sym, held in positions.items() if held > 0
)
total_val_apr20 = total_equity_apr20 + max(vmfxx_balance, 0.0)
print(f"Total equity: {total_equity_apr20:,.0f}")
print(f"Total value (end Apr 20): {total_val_apr20:,.0f}")

# Now simulate April 21
print(f"\n=== APRIL 21 SIMULATION ===")
date_str = "2023-04-21"

# Step 1: Update prices
for sym in all_symbols:
    if sym in prices_by_sym and date_str in prices_by_sym[sym]:
        raw = prices_by_sym[sym][date_str]
        factor = _unadjust_factor(split_factors, sym, date_str)
        old = last_price.get(sym, 0)
        last_price[sym] = raw * factor
        if sym == "OUST":
            print(f"  OUST price update: raw={raw:.4f} * factor={factor:.4f} = {raw*factor:.4f} (was {old:.4f})")

# Step 2: Pre-transaction value (yesterday's positions at today's prices)
pre_tx_equity = sum(
    held * last_price.get(sym, 0.0) for sym, held in positions.items() if held > 0
)
pre_tx_value = pre_tx_equity + max(vmfxx_balance, 0.0)
print(f"OUST in pre_tx: {positions.get('OUST', 0):.0f} sh * {last_price.get('OUST', 0):.4f} = {positions.get('OUST', 0) * last_price.get('OUST', 0):,.0f}")
print(f"Pre-tx value: {pre_tx_value:,.0f}")

# Step 3: Process April 21 transactions
investment_income = 0.0
apr21_txs = []
while tx_idx < len(all_txs) and all_txs[tx_idx]["trade_date"] <= date_str:
    tx = all_txs[tx_idx]
    apr21_txs.append(tx)
    tx_type = tx["tx_type"]
    symbol = tx["symbol"]
    shares = tx["shares"] or 0.0

    print(f"  TX: {tx['tx_id']} {tx_type} {symbol} sh={shares} amt={tx['total_amount'] or 0} ratio={tx.get('split_ratio')}")

    if tx_type == TxType.SWEEP_IN.value:
        vmfxx_balance += abs(tx["total_amount"] or 0.0)
    elif tx_type == TxType.SWEEP_OUT.value:
        vmfxx_balance -= abs(tx["total_amount"] or 0.0)
    elif tx_type in (TxType.BUY.value, TxType.DRIP.value):
        if tx_type == TxType.DRIP.value and symbol == "VMFXX":
            vmfxx_balance += abs(tx["total_amount"] or 0.0)
        elif symbol and shares:
            positions[symbol] = positions.get(symbol, 0.0) + shares
    elif tx_type == TxType.SELL.value:
        if symbol:
            positions[symbol] = positions.get(symbol, 0.0) - shares
    elif tx_type == TxType.TRANSFER_IN.value:
        if symbol and shares:
            positions[symbol] = positions.get(symbol, 0.0) + shares
    elif tx_type == TxType.TRANSFER_OUT.value:
        if symbol and shares:
            positions[symbol] = positions.get(symbol, 0.0) - shares
    elif tx_type == TxType.SPLIT.value:
        ratio = tx["split_ratio"] or 1.0
        if symbol and symbol in positions:
            old_pos = positions[symbol]
            positions[symbol] *= ratio
            print(f"    SPLIT: {symbol} {old_pos:.0f} * {ratio} = {positions[symbol]:.0f}")

    if tx_type in (TxType.DIVIDEND.value, TxType.FEE.value):
        investment_income += tx["total_amount"] or 0.0

    tx_idx += 1

# Step 4: Today's value
equity_value = sum(
    held * last_price.get(sym, 0.0) for sym, held in positions.items() if held > 0
)
cash = max(vmfxx_balance, 0.0)
total_value = equity_value + cash

print(f"\nOUST position after txs: {positions.get('OUST', 0):.0f} shares")
print(f"OUST value: {positions.get('OUST', 0) * last_price.get('OUST', 0):,.0f}")
print(f"Total equity: {equity_value:,.0f}")
print(f"Cash: {cash:,.2f}")
print(f"Total value: {total_value:,.0f}")
print(f"Investment income: {investment_income:,.2f}")

# Step 5: Residual
external_cf = total_value - pre_tx_value - investment_income
print(f"\nRESIDUAL: {total_value:,.0f} - {pre_tx_value:,.0f} - {investment_income:,.2f} = {external_cf:,.0f}")
print(f"This {external_cf:,.0f} gets added to net_deposits")

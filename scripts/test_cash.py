"""Try seven approaches for deriving the VMFXX settlement fund balance from raw CSV
transactions, targeting the known endpoint of ~$848.63 from uploaded_csv.cash_balance.
Created ~Feb 7, 2026 while designing the cash/sweep model.
ONE-TIME: safe to delete -- the winning approach is now in the production snapshot code.
"""
import csv
import io
import sqlite3
from pathlib import Path

DB = Path(__file__).resolve().parent.parent / "data" / "market_dashboard.db"

conn = sqlite3.connect(DB)
conn.row_factory = sqlite3.Row

# Extract CSV content
raw = conn.execute("SELECT content FROM uploaded_csv WHERE id=1").fetchone()[0]
if isinstance(raw, bytes):
    raw = raw.decode("utf-8")

reader = csv.DictReader(io.StringIO(raw))
rows = list(reader)

# Separate sweeps from non-sweeps
sweeps = []
non_sweeps = []
for r in rows:
    tx_type = r.get("Transaction Type", "").strip()
    if tx_type.lower() in ("sweep in", "sweep out"):
        sweeps.append(r)
    else:
        non_sweeps.append(r)

print(f"Total rows: {len(rows)}")
print(f"Sweeps: {len(sweeps)}")
print(f"Non-sweeps: {len(non_sweeps)}")

# --- Approach 1: VMFXX from sweeps only (VMFXX_change = -net_amount) ---
vmfxx_a = 0.0
for s in sweeps:
    amt = float(s.get("Net Amount", "0").replace("$", "").replace(",", "") or "0")
    vmfxx_a -= amt  # VMFXX_change = -net_amount

print(f"\nApproach 1 (VMFXX = -sum(sweep net_amount)):")
print(f"  VMFXX ending balance: ${vmfxx_a:,.2f}")

# --- Approach 2: VMFXX_change = +net_amount ---
vmfxx_b = 0.0
for s in sweeps:
    amt = float(s.get("Net Amount", "0").replace("$", "").replace(",", "") or "0")
    vmfxx_b += amt

print(f"\nApproach 2 (VMFXX = +sum(sweep net_amount)):")
print(f"  VMFXX ending balance: ${vmfxx_b:,.2f}")

# --- Approach 3: cash = sum of ALL total_amounts (non-sweep) ---
cash_c = 0.0
for r in non_sweeps:
    amt = float(r.get("Net Amount", "0").replace("$", "").replace(",", "") or "0")
    cash_c += amt

print(f"\nApproach 3 (cash = sum of non-sweep net amounts):")
print(f"  Cash ending balance: ${cash_c:,.2f}")

# --- Approach 4: cash = sum of ALL total_amounts (including sweeps) ---
cash_d = 0.0
for r in rows:
    amt = float(r.get("Net Amount", "0").replace("$", "").replace(",", "") or "0")
    cash_d += amt

print(f"\nApproach 4 (cash = sum of ALL net amounts including sweeps):")
print(f"  Cash ending balance: ${cash_d:,.2f}")

# --- Approach 5: Sweep-in adds to VMFXX, Sweep-out removes ---
# Using absolute values based on direction label
vmfxx_e = 0.0
for s in sweeps:
    tx_type = s.get("Transaction Type", "").strip().lower()
    amt = float(s.get("Net Amount", "0").replace("$", "").replace(",", "") or "0")
    if tx_type == "sweep in":
        vmfxx_e += abs(amt)  # sweep in = money entering VMFXX
    elif tx_type == "sweep out":
        vmfxx_e -= abs(amt)  # sweep out = money leaving VMFXX

print(f"\nApproach 5 (sweep_in adds |amt|, sweep_out removes |amt|):")
print(f"  VMFXX ending balance: ${vmfxx_e:,.2f}")

# --- Approach 6: sum(non-sweep) + VMFXX_interest ---
# This treats all non-sweep total_amounts as cash changes,
# plus adds VMFXX dividends (skipping DRIPs)
cash_f = 0.0
for r in non_sweeps:
    tx_type = r.get("Transaction Type", "").strip()
    symbol = r.get("Symbol", "").strip()
    amt = float(r.get("Net Amount", "0").replace("$", "").replace(",", "") or "0")

    # Skip VMFXX/VYFXX DRIPs (reinvestment within settlement fund)
    if tx_type == "Reinvestment" and symbol in ("VMFXX", "VYFXX"):
        continue

    cash_f += amt

print(f"\nApproach 6 (non-sweep amounts, skip settlement DRIPs):")
print(f"  Cash ending balance: ${cash_f:,.2f}")

print(f"\n--- Target: ~$848.63 ---")

# --- Approach 7: What initial balance makes approach 3 work? ---
print(f"\nApproach 7: Required initial balance to reach $848.63:")
print(f"  Using non-sweep sum: ${848.63 - cash_c:,.2f}")
print(f"  Using approach 6: ${848.63 - cash_f:,.2f}")

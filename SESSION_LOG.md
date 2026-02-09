# Session Log

## 2026-02-06 — Initial build
- Created market-dashboard as successor to factor-dashboard
- 18 dashboard lines across 4 sections (Equity, Alternatives, Fixed Income, Themes)
- 32 unique tickers, weighted averages where applicable
- Same tech stack: yfinance poller, SQLite, Streamlit with auto-refresh fragment
- Simpler than factor-dashboard: no historical ingestion, no data quality pages, no CLI

## 2026-02-07 — Net Deposits / Cash Tracking Fix (COMPLETED)

Root cause: dashboard wasn't tracking cash (sweeps were skipped by parser, in-kind
transfers had $0 total_amount). Solution: track VMFXX settlement fund balance from
sweep transactions + VMFXX DRIPs. See `scripts/migrate_cash.py` for one-time
migration that inserted 750 sweeps, fixed VMMXX→VMRXX conversion, added VMMXX prices.

Final validation: total_value $2,713,846.26 (Excel: $2,713,845.65 — $0.61 off).
Cash balance $848.63 (exact match).

## 2026-02-08 — Investment Returns (TWR)

### Problem
"Investment Returns" chart showed `gain = total_value - net_deposits`. Net deposits
only changed at 5 points in history (missed ~$891K external deposit that arrived as
SWEEP_IN, not TRANSFER_IN). Result: fake $1.24M overnight "gain" on 2022-07-29.

### Solution: Time-Weighted Return via residual cash flow derivation
Instead of classifying which transactions are external deposits, derive external
cash flows mathematically each day:
```
pre_tx_value = yesterday's positions × today's prices + yesterday's cash
external_CF  = total_value - pre_tx_value - investment_income
```
Then chain daily TWR: `r_t = V_t / (V_{t-1} + CF_t) - 1`

### Changes
1. **schema.py**: Added `twr` column to portfolio_snapshots + migration
2. **queries.py**: `upsert_snapshot` accepts `twr` parameter
3. **snapshots.py**: Major rewrite — computes pre_tx_value before processing txns,
   derives external_cf from residual, accumulates cumulative TWR. Always rebuilds
   from first_date for correctness.
4. **page.py**: Investment Returns chart shows cumulative TWR %. Risk metrics
   (Sharpe, drawdown, volatility) now use TWR-adjusted daily returns.

### Validation
- Total value: $2,713,846.26 (unchanged, within $0.61 of Excel)
- Cash: $848.63 (exact match)
- TWR: 105.27% cumulative (Nov 2019–Feb 2026)
- July 2022 artifact eliminated — smooth transition through the $891K deposit
- Net deposits correctly captures all external flows ($1,311K on Jul 29 vs old $810K)

### Known limitations
- ~$29K undercount in net_deposits (two early cash deposits with no matching sweeps)
- CDs/bonds (~$46K face value) invisible to model (no symbol, $0 total_amount)

## 2026-02-09 Session started -- Portfolio pipeline improvements

Three changes preparing for a new PDF import:

### 1. PDF Upload Support
- `page.py`: File uploader now accepts CSV or PDF
- PDF files are converted via `pdf_to_csv_rows()` to CSV in memory
- Shows preview of converted transactions before import
- Stores resulting CSV (pipeline canonical format stays CSV)

### 2. Transactions Page
- New `tx_page.py` with full transaction CRUD (view/edit/add/delete)
- New `pages/2_Transactions.py` entry point in Streamlit sidebar
- Manual "Rebuild Portfolio" button runs `rebuild_lots()` + `build_daily_snapshots()`

### 3. New query functions
- `update_transaction(conn, tx_id, **fields)` in `queries.py`
- `delete_transaction(conn, tx_id)` in `queries.py`
- Tests in `tests/test_tx_queries.py`

## 2026-02-09 Session started — Codebase cleanup (8 phases)

Goal: Dead code removal, extract shared helpers, move config/SQL to proper layers, fix 10 test failures, add PDF parser tests, deduplicate fixtures, enable foreign keys.

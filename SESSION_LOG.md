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
- ~~TWR: 105.27% cumulative (Nov 2019–Feb 2026)~~ **Incorrect — see correction below**
- July 2022 artifact eliminated — smooth transition through the $891K deposit
- Net deposits correctly captures all external flows ($1,311K on Jul 29 vs old $810K)

> **Correction (2026-02-09):** The 105.27% figure was never the actual stored TWR.
> The dashboard showed ~59.7% both before and after data refreshes. Best theories
> for where 105% came from: (1) the previous Claude session stated it
> conversationally without verifying against the DB — a "vibe-y" answer rather
> than a checked output; (2) possibly confused with a different metric or
> intermediate calculation during iterative development of the TWR feature.
> Diagnostic confirmed 59.7% is internally consistent: daily returns are
> reasonable (max ±6%), trajectory tracks market events correctly, no anomalies
> on deposit or split days.

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

### Completed (all 8 phases)

1. **Dead code removal**: Deleted 6 scripts, removed `pdf_to_csv_file()` from pdf_parser.py, removed `PARSERS` dict from parsers.py.
2. **Extract shared `_get_conn()`**: Added `get_app_connection()` to connection.py, replaced duplicates in page.py and tx_page.py.
3. **Move config constants**: DISPLAY_GROUPS, SYMBOL_LABELS, CLASS_ORDER, CLASS_BASE_COLORS, BENCHMARK_DEFS, GROUPED_SYMS moved from page.py to config.py.
4. **Move inline SQL**: 5 query functions extracted from page.py to queries.py; 3 inline queries in tx_page.py replaced.
5. **Fix 10 test failures**: Updated test_parsers (4), test_metrics (3), test_portfolio_integration (2), test_snapshots (1) to match current code behavior. 42→52 passing.
6. **Add PDF parser tests**: 22 new tests in test_pdf_parser.py for internal helpers. 52→74 passing.
7. **Deduplicate test fixtures**: Shared `portfolio_db` fixture in conftest.py; removed local duplicates from 3 test files.
8. **Enable foreign keys**: Added `PRAGMA foreign_keys = ON` to `get_connection()`.

Final state: 74 tests passing, 0 failing. 8 commits.

## 2026-02-09 Session started — Multi-portfolio support

Goal: Named persistent portfolios, scoped imports, aggregate portfolios, management page. 8 phases planned.

## 2026-02-13 Session started — Split cloud/local deployments

Goal: Remove portfolio pages from cloud deployment, keep them local-only.

### Completed
1. Created `local/local_app.py` — local entry point that Streamlit uses to find `local/pages/`
2. Created `local/launch.bat` — double-click launcher, opens browser at localhost:4006
3. Moved `pages/*.py` to `local/pages/*.py`, fixed sys.path (one extra `.parent`)
4. Updated `ports.json` start command for port 4006
5. Cloud entry point (`streamlit_app.py`) unchanged — no `pages/` directory at root means no sidebar

### Decision
- See DECISIONS.md: "Split into cloud and local deployments"

## 2026-02-10 Session started — AIL portfolio import

### Completed
1. **EODHD coverage confirmed**: All 3 previously-missing ISINs (IE00B3V7VL84, IE00BG85LS38, LU1662505954) now on EODHD EUFUND. All 16 AIL ISINs have price sources.
2. **ISIN_MAP + TickerSource** in config.py: Maps 16 ISINs → (ticker, source). `EODHD_TICKERS` frozenset for routing.
3. **EODHD price fetcher** (eodhd_prices.py): stdlib urllib client, mirrors yfinance pattern with high-water caching.
4. **AIL xlsx parser** (parsers.py): `parse_ail_xlsx()` reads xlsx, resolves ISINs, computes 1% fees.
5. **Source routing** (prices.py): `ensure_prices_for_portfolio` routes EODHD tickers to EODHD API. `fetch_live_prices` uses cached DB prices for EODHD (no real-time). Splits skip EODHD tickers.
6. **Page updated** (page.py): File uploader accepts xlsx. Detects AIL format, previews, imports with broker="AIL".
7. **End-to-end verified**: 32 transactions parsed, 30 open lots, prices fetched for all 16 symbols.

### Blocker: EODHD 1-year history limit
EODHD free tier only provides prices from Feb 2025 onward. AIL transactions go back to May 2024. The 6 EUFUND holdings are valued at $0 for the first 9 months, creating an 85% TWR spike when real prices appear. Attempted carry-backward fix was rejected (fabricated data). Ariel considering options (upgrade to paid tier at $19.99/mo, or alternative data source).

### Decisions
- See DECISIONS.md for EODHD integration decision.

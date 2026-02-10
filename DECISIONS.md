# Decision Log

Structured record of architectural and strategic decisions. Entries written at the moment of decision.

Format: Date, context, alternatives considered, choice, rationale, impact.

---

<!-- Note: Decisions prior to 2026-02-09 are reconstructed from session logs and code archaeology. -->
<!-- Entries after this date are written contemporaneously. -->

## [2026-02-06] Create market-dashboard as separate project from factor-dashboard
- **Context**: factor-dashboard was built around Fama-French academic data, then pivoted to ETF proxies. The new goal — a live market dashboard with weighted ETF groups plus portfolio analytics — diverged enough to warrant a fresh project.
- **Alternatives**: (a) Continue extending factor-dashboard, (b) Start fresh
- **Choice**: (b) — new project
- **Rationale**: Different ticker universe, different purpose (market monitoring + portfolio accounting vs. factor tracking), cleaner separation
- **Impact**: New repo `autobasher/market-dashboard`, port 4003

## [2026-02-07] Derive external cash flows mathematically instead of classifying transaction types
- **Context**: Net deposits were wrong — a $1.24M phantom overnight "gain" appeared because some external flows (sweeps, in-kind transfers) weren't classified as deposits.
- **Alternatives**: (a) Enumerate and classify every tx type as internal/external, (b) Derive external_cf as the residual: total_value - pre_tx_value - investment_income
- **Choice**: (b) — residual derivation
- **Rationale**: Mathematically captures all external flows without needing to anticipate every tx type. Robust against future parser changes or new tx types.
- **Impact**: Rewrote snapshots.py, eliminated phantom gain, TWR validated against Excel ($0.61 off on $2.7M)

## [2026-02-07] Track VMFXX settlement fund balance from sweep transactions
- **Context**: Cash balance was wrong because sweeps (money moving between settlement fund and investment positions) were being ignored.
- **Alternatives**: (a) Ignore sweeps and estimate cash, (b) Parse sweep transactions and track VMFXX balance explicitly
- **Choice**: (b) — explicit sweep tracking
- **Rationale**: Cash balance is a first-class number in portfolio accounting. Estimation would compound errors.
- **Impact**: Migration script inserted 750 sweep transactions, fixed VMMXX→VMRXX conversion. Cash validated at $848.63 (exact match to Excel).

## [2026-02-10] EODHD as supplementary price source for UCITS mutual funds
- **Context**: AIL portfolio has 16 ISINs. 10 are available on Yahoo Finance (US/Canadian stocks + UCITS ETFs). 6 are UCITS mutual funds not on Yahoo — institutional/direct-only funds from Dimensional and AQR.
- **Alternatives**: (a) Manual NAV entry, (b) EODHD EUFUND virtual exchange, (c) Scrape from fund provider websites
- **Choice**: (b) — EODHD EUFUND
- **Rationale**: EODHD provides daily NAV data via a clean REST API. Free tier (20 calls/day) covers 6 funds with headroom. stdlib urllib client, no new dependencies.
- **Impact**: New eodhd_prices.py client, ISIN_MAP in config.py, source routing in prices.py. 6 EODHD calls/day for daily updates.
- **Open issue**: Free tier limited to 1 year of history. Transactions go back to May 2024. Snapshots before Feb 2025 have $0 valuations for EODHD holdings. Paid tier ($19.99/mo) removes the limit. Ariel evaluating options.

## [2026-02-09] Multi-portfolio support with aggregates
- **Context**: Dashboard assumed a single portfolio. Each CSV import wiped all data globally. Need named persistent portfolios that survive re-imports, plus aggregate portfolios for combined views.
- **Alternatives**: (a) Keep single portfolio with multi-account support only, (b) Full multi-portfolio with individual + aggregate types
- **Choice**: (b) — full multi-portfolio with `is_aggregate` flag and `aggregate_members` junction table
- **Rationale**: Ariel has multiple Vanguard accounts. Aggregates let him view combined performance without reimporting. `aggregate_members` links portfolios (not accounts directly) so membership is stable across re-imports.
- **Impact**: Schema changes (new column, new table, uploaded_csv migration), ~10 new query functions, scoped import replaces global wipe, portfolio selector on all pages, new Portfolios management page.

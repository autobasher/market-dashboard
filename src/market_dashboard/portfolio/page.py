from __future__ import annotations

import csv as csv_mod
import io
import tempfile
from datetime import date, timedelta

from dateutil.relativedelta import relativedelta

import altair as alt
import pandas as pd
import streamlit as st

from market_dashboard.config import (
    CLASS_BASE_COLORS, CLASS_ORDER, DISPLAY_GROUPS,
    GROUPED_SYMS, SYMBOL_LABELS,
)
from market_dashboard.database.connection import get_app_connection
from market_dashboard.portfolio import queries
from market_dashboard.portfolio.parsers import parse_vanguard_csv
from market_dashboard.portfolio.pdf_parser import pdf_to_csv_rows
from market_dashboard.portfolio.fifo import rebuild_lots
from market_dashboard.portfolio.prices import ensure_prices_for_portfolio, ensure_splits_for_portfolio, fetch_historical_prices, fetch_live_prices
from market_dashboard.portfolio.snapshots import build_daily_snapshots
from market_dashboard.portfolio import metrics




def _symbols_from_lots(lots) -> list[str]:
    return list({lot["symbol"] for lot in lots})


def _wipe_account_data(conn, account_id: str, portfolio_id: int):
    """Delete data for a single account/portfolio, leaving others intact."""
    conn.execute("DELETE FROM portfolio_snapshots WHERE portfolio_id = ?", (portfolio_id,))
    # Delete lot disposals for lots in this account
    conn.execute(
        "DELETE FROM lot_disposals WHERE lot_id IN "
        "(SELECT lot_id FROM lots WHERE account_id = ?)", (account_id,)
    )
    conn.execute("DELETE FROM lots WHERE account_id = ?", (account_id,))
    conn.execute("DELETE FROM transactions WHERE account_id = ?", (account_id,))


def _store_csv(conn, filename: str, content: bytes, account_id: str, account_name: str, cash_balance: float = 0.0):
    conn.execute(
        "INSERT OR REPLACE INTO uploaded_csv (account_id, filename, content, account_name, cash_balance) "
        "VALUES (?, ?, ?, ?, ?)",
        (account_id, filename, content, account_name, cash_balance),
    )


def _do_import(conn, csv_text: str, portfolio_name: str):
    """Parse CSV, wipe old data for this portfolio, and rebuild."""
    account_id = portfolio_name.lower().replace(" ", "-")

    txs = parse_vanguard_csv(io.StringIO(csv_text), account_id)
    if not txs:
        st.warning("No transactions found in CSV.")
        return

    # Find or create portfolio + account
    existing = queries.get_portfolio_by_name(conn, portfolio_name)
    if existing:
        portfolio_id = existing["portfolio_id"]
        _wipe_account_data(conn, account_id, portfolio_id)
    else:
        queries.insert_account(conn, account_id, portfolio_name, "Vanguard")
        portfolio_id = queries.insert_portfolio(conn, portfolio_name)
        queries.add_account_to_portfolio(conn, portfolio_id, account_id)

    for tx in txs:
        queries.insert_transaction(
            conn,
            account_id=tx.account_id,
            trade_date=tx.trade_date,
            tx_type=tx.tx_type.value,
            total_amount=tx.total_amount,
            settlement_date=tx.settlement_date,
            symbol=tx.symbol,
            shares=tx.shares,
            price_per_share=tx.price_per_share,
            fees=tx.fees,
            split_ratio=tx.split_ratio,
            raw_description=tx.raw_description,
            source_file=tx.source_file,
        )

    all_symbols = list({tx.symbol for tx in txs if tx.symbol})
    earliest = min(tx.trade_date for tx in txs)
    today = date.today()

    if all_symbols:
        with st.spinner("Checking for stock splits..."):
            n_splits = ensure_splits_for_portfolio(conn, account_id, all_symbols, earliest, today)
        if n_splits:
            st.info(f"Applied {n_splits} stock split(s).")

    rebuild_lots(conn, account_id)

    if all_symbols:
        with st.spinner("Fetching prices..."):
            ensure_prices_for_portfolio(conn, all_symbols, earliest, today)

        with st.spinner("Building snapshots..."):
            build_daily_snapshots(conn, portfolio_id, earliest, today)

    # Rebuild snapshots for any aggregates containing this portfolio
    aggregates = queries.get_aggregates_containing(conn, portfolio_id)
    for agg in aggregates:
        _rebuild_aggregate_snapshots(conn, agg["portfolio_id"])

    conn.commit()
    st.success(f"Imported {len(txs)} transactions into '{portfolio_name}'.")
    st.rerun()


def _rebuild_aggregate_snapshots(conn, aggregate_id: int):
    """Rebuild aggregate snapshots by summing member portfolio snapshots.

    For each date, sums total_value/total_cost/cash_balance across members,
    then chains TWR from the combined daily returns.
    """
    members = queries.get_aggregate_members(conn, aggregate_id)
    if not members:
        return

    # Sync portfolio_accounts for the aggregate
    conn.execute("DELETE FROM portfolio_accounts WHERE portfolio_id = ?", (aggregate_id,))
    for m in members:
        for acct in queries.get_portfolio_accounts(conn, m["portfolio_id"]):
            queries.add_account_to_portfolio(conn, aggregate_id, acct["account_id"])

    # Collect member snapshots into DataFrames keyed by date
    member_dfs = []
    for m in members:
        snaps = queries.get_snapshots(conn, m["portfolio_id"])
        if snaps:
            df = pd.DataFrame([dict(s) for s in snaps])
            df["snap_date"] = pd.to_datetime(df["snap_date"])
            df = df.set_index("snap_date")[["total_value", "total_cost", "cash_balance"]]
            member_dfs.append(df)

    if not member_dfs:
        return

    # Sum across members, filling missing dates with 0 (portfolio didn't exist yet)
    combined = member_dfs[0]
    for df in member_dfs[1:]:
        combined = combined.add(df, fill_value=0)
    combined = combined.sort_index()

    # Chain TWR from combined daily values and derived external cash flows
    conn.execute(
        "DELETE FROM portfolio_snapshots WHERE portfolio_id = ?", (aggregate_id,)
    )
    prev_value = 0.0
    cumulative_twr = 0.0
    for snap_date, row in combined.iterrows():
        total_value = row["total_value"]
        total_cost = row["total_cost"]
        cash_balance = row["cash_balance"]

        if prev_value > 0:
            # external_cf for the day = change in net deposits
            prev_cost = prev_row["total_cost"]
            external_cf = total_cost - prev_cost
            denominator = prev_value + external_cf
            if denominator > 0:
                daily_return = total_value / denominator - 1
            else:
                daily_return = 0.0
            cumulative_twr = (1 + cumulative_twr) * (1 + daily_return) - 1

        queries.upsert_snapshot(
            conn, aggregate_id, snap_date.date(),
            total_value, total_cost, cash_balance, cumulative_twr,
        )
        prev_value = total_value
        prev_row = row

    conn.commit()


def _pdf_to_csv_text(pdf_bytes: bytes) -> str:
    """Convert PDF bytes to Vanguard-format CSV text via pdf_to_csv_rows()."""
    with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
        tmp.write(pdf_bytes)
        tmp_path = tmp.name
    try:
        rows = pdf_to_csv_rows(tmp_path)
    finally:
        import os
        os.unlink(tmp_path)
    if not rows:
        return ""
    fieldnames = [
        "Trade Date", "Settlement Date", "Transaction Type",
        "Transaction Description", "Symbol", "Shares",
        "Share Price", "Commission", "Fees", "Net Amount",
    ]
    buf = io.StringIO()
    writer = csv_mod.DictWriter(buf, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(rows)
    return buf.getvalue()


def _import_section(conn):
    all_portfolios = queries.get_all_portfolios(conn)
    individual_portfolios = [p for p in all_portfolios if not p["is_aggregate"]]
    has_data = len(individual_portfolios) > 0

    with st.expander("Import Transactions", expanded=not has_data):
        # Portfolio selector
        portfolio_options = [p["name"] for p in individual_portfolios] + ["+ New portfolio..."]
        selected = st.selectbox("Portfolio", portfolio_options, key="import_portfolio_select")

        if selected == "+ New portfolio...":
            portfolio_name = st.text_input("New portfolio name", placeholder="e.g. Ariel2")
        else:
            portfolio_name = selected

        # Show current file info if existing portfolio
        account_id = portfolio_name.lower().replace(" ", "-") if portfolio_name else ""
        stored = queries.get_stored_csv(conn, account_id) if account_id else None
        if stored:
            col_info, col_dl = st.columns([3, 1])
            col_info.caption(f"Current file: **{stored['filename']}** (uploaded {stored['uploaded_at']})")
            col_dl.download_button(
                "Download CSV",
                data=stored["content"],
                file_name=stored["filename"],
                mime="text/csv",
            )

        default_cash = stored["cash_balance"] if stored else 0.0
        cash_balance = st.number_input("Cash Balance (VMFXX)", value=float(default_cash), min_value=0.0, step=0.01, format="%.2f")

        uploaded = st.file_uploader("Upload Vanguard CSV or PDF", type=["csv", "pdf"])

        # PDF preview: convert and show before import
        csv_text = None
        if uploaded:
            raw_bytes = uploaded.getvalue()
            if uploaded.name.lower().endswith(".pdf"):
                with st.spinner("Converting PDF..."):
                    csv_text = _pdf_to_csv_text(raw_bytes)
                if csv_text:
                    preview_df = pd.read_csv(io.StringIO(csv_text), dtype=str)
                    st.caption(f"Converted {len(preview_df)} transactions from PDF:")
                    st.dataframe(preview_df, use_container_width=True, hide_index=True)
                else:
                    st.warning("No transactions found in PDF.")
            else:
                csv_text = raw_bytes.decode("utf-8")

        can_import = uploaded and portfolio_name and csv_text
        if st.button("Import", disabled=not can_import):
            if not csv_text or not portfolio_name:
                return
            try:
                csv_bytes = csv_text.encode("utf-8")
                csv_filename = uploaded.name.rsplit(".", 1)[0] + ".csv" if uploaded.name.lower().endswith(".pdf") else uploaded.name

                _store_csv(conn, csv_filename, csv_bytes, account_id, portfolio_name, cash_balance)
                conn.commit()

                _do_import(conn, csv_text, portfolio_name)

            except Exception as e:
                st.error(f"Import failed: {e}")


def _find_or_create_aggregate(conn, selected_portfolios: list) -> int:
    """Find an existing aggregate with exactly these members, or create one."""
    member_ids = sorted(p["portfolio_id"] for p in selected_portfolios)
    target = set(member_ids)

    # Check existing aggregates that contain the first member
    candidates = queries.get_aggregates_containing(conn, member_ids[0])
    for agg in candidates:
        members = queries.get_aggregate_members(conn, agg["portfolio_id"])
        if {m["portfolio_id"] for m in members} == target:
            return agg["portfolio_id"]

    # None found — create one
    name = " + ".join(p["name"] for p in selected_portfolios)
    agg_id = queries.insert_portfolio(conn, name, is_aggregate=True)
    for mid in member_ids:
        queries.add_aggregate_member(conn, agg_id, mid)
    conn.commit()
    return agg_id


def main():
    st.set_page_config(page_title="Performance", page_icon=":material/account_balance:", layout="wide")
    st.markdown("""<style>
        [data-testid="stMetricLabel"] {
            font-size: 0.75rem;
            line-height: 1.3;
            min-height: 2.4em;
            display: flex;
            align-items: flex-end;
        }
        [data-testid="stMetricValue"] { font-size: 1.4rem; }
    </style>""", unsafe_allow_html=True)

    conn = get_app_connection()

    _import_section(conn)

    # Portfolio selector — checkboxes + Build button
    all_portfolios = queries.get_all_portfolios(conn)
    individual_portfolios = [p for p in all_portfolios if not p["is_aggregate"]]
    if not individual_portfolios:
        st.info("Upload a Vanguard CSV to get started.")
        return

    sel_cols = st.columns(len(individual_portfolios) + 1)
    checked = {}
    for i, p in enumerate(individual_portfolios):
        checked[p["name"]] = sel_cols[i].checkbox(p["name"], key=f"pf_check_{p['portfolio_id']}")
    build_clicked = sel_cols[-1].button("Build", key="pf_build")

    selected_names = sorted(name for name, on in checked.items() if on)
    if not selected_names:
        st.info("Select one or more portfolios and click Build.")
        return

    # Only proceed when Build is clicked; store selection in session state
    if build_clicked:
        st.session_state["built_portfolios"] = selected_names
    built = st.session_state.get("built_portfolios")

    # If nothing built yet, or checkboxes changed since last Build, wait
    if not built or sorted(built) != selected_names:
        st.info("Click **Build** to load selected portfolios.")
        return

    selected = [p for p in individual_portfolios if p["name"] in built]
    if not selected:
        return
    is_aggregate = len(selected) > 1

    # Resolve portfolio_id fresh every time (no caching — aggregate lookup is cheap)
    if not is_aggregate:
        portfolio_id = selected[0]["portfolio_id"]
    else:
        portfolio_id = _find_or_create_aggregate(conn, selected)

    account_ids = queries.get_effective_account_ids(conn, portfolio_id)
    if not account_ids:
        st.info("No accounts linked to this portfolio.")
        return

    # Cash balance: sum across all accounts' stored CSVs
    cash_balance = sum(
        (queries.get_stored_csv(conn, aid) or {}).get("cash_balance", 0.0)
        for aid in account_ids
    )

    open_lots = queries.get_all_open_lots(conn, account_ids)
    symbols = _symbols_from_lots(open_lots)
    current_prices = queries.get_latest_prices(conn, symbols)

    # Ensure prices and snapshots are up to today
    today = date.today()
    if symbols:
        tx_rows = []
        for acct_id in account_ids:
            tx_rows.extend(queries.get_transactions(conn, account_id=acct_id))
        if tx_rows:
            earliest = min(date.fromisoformat(r["trade_date"]) for r in tx_rows)
            with st.spinner("Checking prices..."):
                ensure_prices_for_portfolio(conn, symbols, earliest, today)
            current_prices = queries.get_latest_prices(conn, symbols)

            # Rebuild snapshots if stale or missing
            latest_snap = conn.execute(
                "SELECT MAX(snap_date) as max_date FROM portfolio_snapshots WHERE portfolio_id = ?",
                (portfolio_id,),
            ).fetchone()
            if not latest_snap or not latest_snap["max_date"] or latest_snap["max_date"] < today.isoformat():
                with st.spinner("Updating snapshots..."):
                    if is_aggregate:
                        # Update each member's snapshots incrementally, then rebuild aggregate
                        for sel in selected:
                            member_snap = conn.execute(
                                "SELECT MAX(snap_date) as max_date FROM portfolio_snapshots WHERE portfolio_id = ?",
                                (sel["portfolio_id"],),
                            ).fetchone()
                            member_start = earliest
                            if member_snap and member_snap["max_date"]:
                                member_start = date.fromisoformat(member_snap["max_date"]) + timedelta(days=1)
                            if member_start <= today:
                                build_daily_snapshots(conn, sel["portfolio_id"], member_start, today)
                        _rebuild_aggregate_snapshots(conn, portfolio_id)
                    else:
                        snap_start = earliest
                        if latest_snap and latest_snap["max_date"]:
                            snap_start = date.fromisoformat(latest_snap["max_date"]) + timedelta(days=1)
                        build_daily_snapshots(conn, portfolio_id, snap_start, today)

    snapshots = queries.get_snapshots(conn, portfolio_id)

    # Gather all transactions + disposals for metrics
    all_txs = []
    for acct_id in account_ids:
        all_txs.extend(queries.get_transactions(conn, account_id=acct_id))
    disposals = queries.get_disposals(conn)

    # Build full snapshot DataFrame
    if snapshots:
        snap_df = pd.DataFrame([dict(r) for r in snapshots])
        snap_df["date"] = pd.to_datetime(snap_df["snap_date"])
        snap_df = snap_df.set_index("date").sort_index()
        snap_df["twr_growth"] = 1 + snap_df["twr"]
    else:
        snap_df = pd.DataFrame()

    # --- Live portfolio value (always current, regardless of period) ---
    live_prices = fetch_live_prices(symbols)
    portfolio_value = cash_balance + sum(
        lot["shares_remaining"] * live_prices.get(lot["symbol"], current_prices.get(lot["symbol"], 0.0))
        for lot in open_lots
        if lot["shares_remaining"] > 0
    )
    prev_close_value = float(snap_df["total_value"].iloc[-1]) if not snap_df.empty else None
    st.metric("Portfolio Value", f"${portfolio_value:,.2f}")

    # --- Period selector ---
    PERIODS = {
        "1D": relativedelta(days=1),
        "1W": relativedelta(weeks=1),
        "1M": relativedelta(months=1),
        "3M": relativedelta(months=3),
        "6M": relativedelta(months=6),
        "YTD": None,  # special case
        "1Y": relativedelta(years=1),
        "3Y": relativedelta(years=3),
        "5Y": relativedelta(years=5),
        "10Y": relativedelta(years=10),
        "All": None,
    }
    period = st.pills("Period", list(PERIODS.keys()), default="All")

    # --- 1D intraday mode ---
    is_intraday = period == "1D"
    if is_intraday:
        if prev_close_value and prev_close_value > 0:
            day_return = portfolio_value / prev_close_value - 1
            day_change = portfolio_value - prev_close_value
        else:
            day_return = day_change = None
        m1, m2 = st.columns(2)
        m1.metric("Day Return", f"{day_return:.2%}" if day_return is not None else "—")
        m2.metric("Day Change", f"${day_change:+,.2f}" if day_change is not None else "—")
    else:

        # Compute period start date
        if period == "All" or period is None:
            period_start = None
        elif period == "YTD":
            period_start = pd.Timestamp(date(today.year, 1, 1))
        else:
            period_start = pd.Timestamp(today - PERIODS[period])

        # Filter snapshots to period
        if not snap_df.empty and period_start is not None:
            view_df = snap_df[snap_df.index >= period_start]
        else:
            view_df = snap_df

        # Compute portfolio metrics for the selected period
        if not view_df.empty and len(view_df) > 1:
            twr_end = float(view_df["twr"].iloc[-1])
            if period_start is not None and not snap_df.empty:
                before = snap_df[snap_df.index < period_start]
                twr_base = float(before["twr"].iloc[-1]) if not before.empty else 0.0
            else:
                twr_base = 0.0
            total_ret = (1 + twr_end) / (1 + twr_base) - 1

            n_days = (view_df.index[-1] - view_df.index[0]).days
            cagr = (1 + total_ret) ** (365.25 / n_days) - 1 if n_days > 365 else None

            period_snaps = [s for s in snapshots if s["snap_date"] >= (period_start or pd.Timestamp.min).strftime("%Y-%m-%d")]
            if period_start is not None and not before.empty:
                start_value = float(before["total_value"].iloc[-1])
                start_date = before.index[-1].date()
                xirr_val = metrics.portfolio_xirr(period_snaps, portfolio_value, today, start_value, start_date)
            else:
                xirr_val = metrics.portfolio_xirr(period_snaps, portfolio_value, today)

            daily_returns = view_df["twr_growth"].pct_change().dropna()
            sharpe = metrics.sharpe_ratio(daily_returns) if len(daily_returns) > 1 else None
            mdd = metrics.max_drawdown(daily_returns) if len(daily_returns) > 1 else None
            vol = metrics.annual_volatility(daily_returns) if len(daily_returns) > 1 else None
        elif not view_df.empty and len(view_df) == 1:
            total_ret = cagr = xirr_val = sharpe = mdd = vol = n_days = None
        else:
            total_ret = xirr_val = sharpe = mdd = vol = cagr = n_days = None

        # --- Benchmark selector + computation ---
        eq_pct_options = [None] + list(range(10, 101, 10))
        eq_pct = st.selectbox(
            "Equity benchmark",
            eq_pct_options,
            format_func=lambda v: "None" if v is None else f"{v}% equity / {100 - v}% cash",
            index=0,
            key=f"eq_benchmark_{portfolio_id}",
        )

        benchmarks = []  # list of {name, cum_ret_vals, dates, daily_returns, color}
        if eq_pct is not None and not view_df.empty and len(view_df) > 1:
            eq_w = eq_pct / 100
            bond_w = 1 - eq_w
            bm_start = view_df.index[0].date()
            bm_end = view_df.index[-1].date()
            for sym in ("VOO", "ACWI", "VGSH"):
                fetch_historical_prices(conn, sym, bm_start, bm_end)

            bond_rows = queries.get_daily_prices(conn, "VGSH", bm_start, bm_end)
            bond_s = pd.Series(
                {pd.Timestamp(r["price_date"]): r["adj_close"] for r in bond_rows}
            ).sort_index() if bond_rows else pd.Series(dtype=float)

            for eq_sym, eq_label, color in (("VOO", "S&P 500", "#a05a5a"), ("ACWI", "ACWI", "#5a8a5a")):
                bm_name = f"{eq_pct}% {eq_label} / {100 - eq_pct}% Cash"
                eq_rows = queries.get_daily_prices(conn, eq_sym, bm_start, bm_end)
                if not eq_rows or bond_s.empty:
                    continue
                eq_s = pd.Series(
                    {pd.Timestamp(r["price_date"]): r["adj_close"] for r in eq_rows}
                ).sort_index()
                common = eq_s.index.intersection(bond_s.index)
                if len(common) < 2:
                    continue
                eq_ret = eq_s[common].pct_change().fillna(0)
                bond_ret = bond_s[common].pct_change().fillna(0)

                # Simulate portfolio with quarterly rebalancing
                eq_portion = eq_w
                bond_portion = bond_w
                cum_ret_vals = []
                prev_quarter = None
                for dt, er, br in zip(common, eq_ret, bond_ret):
                    quarter = (dt.year, (dt.month - 1) // 3)
                    if prev_quarter is not None and quarter != prev_quarter:
                        total_bm = eq_portion + bond_portion
                        eq_portion = total_bm * eq_w
                        bond_portion = total_bm * bond_w
                    eq_portion *= (1 + er)
                    bond_portion *= (1 + br)
                    cum_ret_vals.append(eq_portion + bond_portion - 1)
                    prev_quarter = quarter

                # Compute benchmark metrics
                growth = pd.Series(
                    [1.0] + [1 + v for v in cum_ret_vals], index=[common[0]] + list(common),
                )
                bm_daily_ret = growth.pct_change().dropna()
                bm_total_ret = cum_ret_vals[-1] if cum_ret_vals else None
                bm_n_days = (common[-1] - common[0]).days
                bm_cagr = (1 + bm_total_ret) ** (365.25 / bm_n_days) - 1 if bm_total_ret is not None and bm_n_days > 365 else None
                bm_sharpe = metrics.sharpe_ratio(bm_daily_ret) if len(bm_daily_ret) > 1 else None
                bm_mdd = metrics.max_drawdown(bm_daily_ret) if len(bm_daily_ret) > 1 else None
                bm_vol = metrics.annual_volatility(bm_daily_ret) if len(bm_daily_ret) > 1 else None

                benchmarks.append({
                    "name": bm_name, "dates": common, "cum_ret_vals": cum_ret_vals,
                    "color": color, "total_ret": bm_total_ret, "cagr": bm_cagr,
                    "sharpe": bm_sharpe, "mdd": bm_mdd, "vol": bm_vol,
                })

        # --- Metrics display ---
        def _fmt_pct(v): return f"{v:.1%}" if v is not None else "—"
        def _fmt_sharpe(v): return f"{v:.2f}" if v is not None else "—"

        m1, m2, m3, m4, m5, m6 = st.columns([2, 2.5, 3, 3, 1.5, 1.5])
        m1.metric("Total return (TWR)", _fmt_pct(total_ret))
        m2.metric("Return, per year (CAGR)", _fmt_pct(cagr))
        m3.metric("Money return, per year (XIRR)", _fmt_pct(xirr_val))
        m4.metric("Risk-to-reward (Sharpe)", _fmt_sharpe(sharpe))
        m5.metric("Max drawdown", _fmt_pct(mdd))
        m6.metric("Volatility", _fmt_pct(vol))

        for bm in benchmarks:
            m1, m2, m3, m4, m5, m6 = st.columns([2, 2.5, 3, 3, 1.5, 1.5])
            m1.caption(f"**{bm['name']}**")
            m2.metric("TWR", _fmt_pct(bm["total_ret"]))
            m3.metric("CAGR", _fmt_pct(bm["cagr"]))
            m4.metric("Sharpe", _fmt_sharpe(bm["sharpe"]))
            m5.metric("Max DD", _fmt_pct(bm["mdd"]))
            m6.metric("Vol", _fmt_pct(bm["vol"]))

    # --- Charts ---
    if not is_intraday and not view_df.empty:
        dollar_fmt = "$,.0f"
        span_days = (view_df.index[-1] - view_df.index[0]).days
        x_fmt = "%b %Y" if span_days > 180 else "%b %d"
        x_axis = alt.Axis(format=x_fmt)

        st.subheader("Investment Returns")
        twr_base = float(view_df["twr"].iloc[0])
        ret_src = view_df[["twr"]].copy()
        ret_src["Return"] = (1 + ret_src["twr"]) / (1 + twr_base) - 1
        ret_src = ret_src.reset_index()

        ret_long = ret_src.rename(columns={"Return": "value"})[["date", "value"]].copy()
        ret_long["Series"] = "Portfolio"

        _RET_COLORS = {"Portfolio": "#4a90d9"}
        for bm in benchmarks:
            bm_df = pd.DataFrame({"date": bm["dates"], "value": bm["cum_ret_vals"], "Series": bm["name"]})
            ret_long = pd.concat([ret_long, bm_df], ignore_index=True)
            _RET_COLORS[bm["name"]] = bm["color"]

        _series_order = [s for s in _RET_COLORS if s in ret_long["Series"].unique()]
        ret_chart = (
            alt.Chart(ret_long).mark_line().encode(
                x=alt.X("date:T", title=None, axis=x_axis),
                y=alt.Y("value:Q", axis=alt.Axis(format=".0%"), title="Return"),
                color=alt.Color(
                    "Series:N",
                    title=None,
                    scale=alt.Scale(
                        domain=_series_order,
                        range=[_RET_COLORS[s] for s in _series_order],
                    ),
                ),
                tooltip=[
                    alt.Tooltip("date:T", title="Date"),
                    alt.Tooltip("Series:N", title="Series"),
                    alt.Tooltip("value:Q", title="Return", format=".1%"),
                ],
            ).interactive()
        )
        st.altair_chart(ret_chart, use_container_width=True)

        st.subheader("Balances")
        bal_src = view_df[["total_value", "total_cost"]].rename(
            columns={"total_value": "Market Value", "total_cost": "Net Deposits"}
        ).reset_index()
        bal_long = bal_src.melt("date", var_name="Series", value_name="Amount")
        bal_chart = (
            alt.Chart(bal_long).mark_line().encode(
                x=alt.X("date:T", title=None, axis=x_axis),
                y=alt.Y("Amount:Q", axis=alt.Axis(format=dollar_fmt), title=None),
                color=alt.Color("Series:N", title=None),
                tooltip=[
                    alt.Tooltip("date:T", title="Date"),
                    alt.Tooltip("Series:N", title="Series"),
                    alt.Tooltip("Amount:Q", title="Value", format=dollar_fmt),
                ],
            ).interactive()
        )
        st.altair_chart(bal_chart, use_container_width=True)

    # --- Allocation donut chart ---

    st.subheader("Allocation")
    alloc = metrics.current_allocation(open_lots, current_prices)
    if cash_balance > 0:
        total_with_cash = sum(
            lot["shares_remaining"] * current_prices.get(lot["symbol"], 0.0)
            for lot in open_lots if lot["shares_remaining"] > 0
        ) + cash_balance
        if total_with_cash > 0:
            vmfxx_pct = cash_balance / total_with_cash
            alloc = {sym: val * (1 - vmfxx_pct) / sum(alloc.values()) for sym, val in alloc.items()} if alloc else {}
            alloc["VMFXX"] = vmfxx_pct

    if alloc:
        # Aggregate into display groups
        alloc_rows = []
        for grp, info in DISPLAY_GROUPS.items():
            grp_total = sum(alloc.get(sym, 0.0) for sym in info["symbols"])
            if grp_total > 0.001:
                alloc_rows.append({"Label": grp, "Allocation": grp_total, "Class": info["class"]})
        for sym, pct in alloc.items():
            if sym not in GROUPED_SYMS and pct > 0.001:
                label = SYMBOL_LABELS.get(sym, sym)
                alloc_rows.append({"Label": label, "Allocation": pct, "Class": "Equity"})

        alloc_df = pd.DataFrame(alloc_rows)
        alloc_df["Class"] = pd.Categorical(alloc_df["Class"], categories=CLASS_ORDER, ordered=True)
        alloc_df = alloc_df.sort_values(["Class", "Allocation"], ascending=[True, False]).reset_index(drop=True)

        # Text label with percentage
        alloc_df["text_label"] = alloc_df.apply(
            lambda r: f"{r['Label']} {r['Allocation']:.1%}", axis=1,
        )

        # Compute midpoint fraction for text alignment (right half → left-align, left half → right-align)
        total_alloc = alloc_df["Allocation"].sum()
        alloc_df["cum_end"] = alloc_df["Allocation"].cumsum() / total_alloc
        alloc_df["mid_frac"] = alloc_df["cum_end"] - alloc_df["Allocation"] / total_alloc / 2

        # Assign shaded colors within each asset class
        color_map = {}
        for cls in CLASS_ORDER:
            cls_labels = alloc_df[alloc_df["Class"] == cls]["Label"].tolist()
            base_r, base_g, base_b = CLASS_BASE_COLORS[cls]
            for i, lbl in enumerate(cls_labels):
                t = i / max(len(cls_labels), 1) * 0.6
                r = int(base_r + (255 - base_r) * t)
                g = int(base_g + (255 - base_g) * t)
                b = int(base_b + (255 - base_b) * t)
                color_map[lbl] = f"rgb({r},{g},{b})"

        label_order = alloc_df["Label"].tolist()
        # Conditional text columns so stacking stays consistent across layers
        alloc_df["text_right"] = alloc_df.apply(
            lambda r: r["text_label"] if r["mid_frac"] < 0.5 else "", axis=1,
        )
        alloc_df["text_left"] = alloc_df.apply(
            lambda r: r["text_label"] if r["mid_frac"] >= 0.5 else "", axis=1,
        )

        base = alt.Chart(alloc_df)
        pie = base.mark_arc(outerRadius=140).encode(
            theta=alt.Theta("Allocation:Q", stack=True),
            color=alt.Color(
                "Label:N",
                scale=alt.Scale(domain=label_order, range=[color_map[s] for s in label_order]),
                legend=None,
            ),
            order=alt.Order("index:O"),
            tooltip=[
                alt.Tooltip("Label:N", title="Holding"),
                alt.Tooltip("Class:N", title="Asset Class"),
                alt.Tooltip("Allocation:Q", title="Weight", format=".1%"),
            ],
        )
        lbl_enc = dict(
            theta=alt.Theta("Allocation:Q", stack=True),
            order=alt.Order("index:O"),
        )
        text_r = base.mark_text(radius=155, size=11, align="left", color="#d0d0d0").encode(
            text="text_right:N", **lbl_enc,
        )
        text_l = base.mark_text(radius=155, size=11, align="right", color="#d0d0d0").encode(
            text="text_left:N", **lbl_enc,
        )
        chart = (pie + text_r + text_l).properties(height=400)
        st.altair_chart(chart, use_container_width=True)
    else:
        st.info("No allocation data.")

    # --- Positions table ---
    st.subheader("Positions")
    if open_lots:
        pos_data = {}
        for lot in open_lots:
            sym = lot["symbol"]
            if sym not in pos_data:
                pos_data[sym] = {"Symbol": sym, "Shares": 0.0, "Cost Basis": 0.0}
            pos_data[sym]["Shares"] += lot["shares_remaining"]
            pos_data[sym]["Cost Basis"] += lot["shares_remaining"] * lot["cost_basis_per_share"]

        if cash_balance > 0:
            pos_data["VMFXX"] = {"Symbol": "VMFXX", "Shares": cash_balance, "Cost Basis": cash_balance}
            current_prices["VMFXX"] = 1.0

        rows = []
        for sym, d in pos_data.items():
            price = current_prices.get(sym, 0.0)
            mkt_val = d["Shares"] * price
            if mkt_val < 0.01:
                continue
            gain = mkt_val - d["Cost Basis"]
            gain_pct = gain / d["Cost Basis"] if d["Cost Basis"] else 0.0
            rows.append({
                "Symbol": sym,
                "Shares": round(d["Shares"], 4),
                "Price": f"${price:,.2f}",
                "Market Value": mkt_val,
                "Cost Basis": d["Cost Basis"],
                "Gain/Loss": gain,
                "Gain %": f"{gain_pct:.1%}",
            })
        pos_df = pd.DataFrame(rows).sort_values("Market Value", ascending=False)
        for col in ["Market Value", "Cost Basis", "Gain/Loss"]:
            pos_df[col] = pos_df[col].apply(lambda x: f"${x:,.2f}")
        st.dataframe(pos_df, use_container_width=True, hide_index=True)
    else:
        st.info("No open positions.")



if __name__ == "__main__":
    main()

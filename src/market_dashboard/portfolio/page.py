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
    BENCHMARK_DEFS, CLASS_BASE_COLORS, CLASS_ORDER, DISPLAY_GROUPS,
    GROUPED_SYMS, SYMBOL_LABELS,
)
from market_dashboard.database.connection import get_app_connection
from market_dashboard.portfolio import queries
from market_dashboard.portfolio.parsers import parse_vanguard_csv
from market_dashboard.portfolio.pdf_parser import pdf_to_csv_rows
from market_dashboard.portfolio.fifo import rebuild_lots
from market_dashboard.portfolio.prices import ensure_prices_for_portfolio, ensure_splits_for_portfolio, fetch_historical_prices
from market_dashboard.portfolio.snapshots import build_daily_snapshots
from market_dashboard.portfolio import metrics




def _get_all_open_lots(conn, account_ids: list[str]):
    all_lots = []
    for acct_id in account_ids:
        rows = conn.execute(
            "SELECT * FROM lots WHERE account_id = ? AND shares_remaining > 0",
            (acct_id,),
        ).fetchall()
        all_lots.extend(rows)
    return all_lots


def _get_current_prices(conn, symbols: list[str]) -> dict[str, float]:
    prices = {}
    for sym in symbols:
        row = conn.execute(
            "SELECT close FROM historical_prices "
            "WHERE symbol = ? ORDER BY price_date DESC LIMIT 1",
            (sym,),
        ).fetchone()
        if row:
            prices[sym] = row["close"]
    return prices


def _get_portfolio_id(conn) -> int | None:
    row = conn.execute("SELECT portfolio_id FROM portfolios LIMIT 1").fetchone()
    return row["portfolio_id"] if row else None


def _get_account_ids(conn) -> list[str]:
    rows = conn.execute("SELECT account_id FROM accounts").fetchall()
    return [r["account_id"] for r in rows]


def _symbols_from_lots(lots) -> list[str]:
    return list({lot["symbol"] for lot in lots})


def _get_stored_csv(conn) -> dict | None:
    row = conn.execute("SELECT * FROM uploaded_csv WHERE id = 1").fetchone()
    return dict(row) if row else None


def _wipe_portfolio_data(conn):
    """Delete all portfolio-related data for a clean re-import."""
    conn.execute("DELETE FROM portfolio_snapshots")
    conn.execute("DELETE FROM lot_disposals")
    conn.execute("DELETE FROM lots")
    conn.execute("DELETE FROM transactions")
    conn.execute("DELETE FROM portfolio_accounts")
    conn.execute("DELETE FROM portfolios")
    conn.execute("DELETE FROM accounts")


def _store_csv(conn, filename: str, content: bytes, account_id: str, account_name: str, cash_balance: float = 0.0):
    conn.execute(
        "INSERT OR REPLACE INTO uploaded_csv (id, filename, content, account_id, account_name, cash_balance) "
        "VALUES (1, ?, ?, ?, ?, ?)",
        (filename, content, account_id, account_name, cash_balance),
    )


def _do_import(conn, csv_text: str, account_id: str, account_name: str):
    """Parse CSV, wipe old data, and rebuild everything from scratch."""
    txs = parse_vanguard_csv(io.StringIO(csv_text), account_id)
    if not txs:
        st.warning("No transactions found in CSV.")
        return

    _wipe_portfolio_data(conn)

    queries.insert_account(conn, account_id, account_name or account_id, "Vanguard")

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

    portfolio_id = queries.insert_portfolio(conn, "My Portfolio")
    queries.add_account_to_portfolio(conn, portfolio_id, account_id)

    if all_symbols:
        with st.spinner("Fetching prices..."):
            ensure_prices_for_portfolio(conn, all_symbols, earliest, today)

        with st.spinner("Building snapshots..."):
            build_daily_snapshots(conn, portfolio_id, earliest, today)

    conn.commit()
    st.success(f"Imported {len(txs)} transactions.")
    st.rerun()


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
    stored = _get_stored_csv(conn)

    with st.expander("Import Transactions", expanded=stored is None):
        if stored:
            col_info, col_dl = st.columns([3, 1])
            col_info.caption(f"Current file: **{stored['filename']}** (uploaded {stored['uploaded_at']})")
            col_dl.download_button(
                "Download CSV",
                data=stored["content"],
                file_name=stored["filename"],
                mime="text/csv",
            )
            st.divider()
            st.caption("Upload a new file to replace the current data.")

        uploaded = st.file_uploader("Upload Vanguard CSV or PDF", type=["csv", "pdf"])
        col1, col2, col3 = st.columns(3)
        default_acct = stored["account_id"] if stored else ""
        default_name = stored["account_name"] if stored else ""
        default_cash = stored["cash_balance"] if stored else 0.0
        account_id = col1.text_input("Account ID", value=default_acct, placeholder="e.g. 12345678")
        account_name = col2.text_input("Account Name", value=default_name, placeholder="e.g. Vanguard Brokerage")
        cash_balance = col3.number_input("Cash Balance (VMFXX)", value=float(default_cash), min_value=0.0, step=0.01, format="%.2f")

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

        if st.button("Import", disabled=not uploaded or not account_id or not csv_text):
            if not csv_text or not account_id:
                return
            try:
                csv_bytes = csv_text.encode("utf-8")
                csv_filename = uploaded.name.rsplit(".", 1)[0] + ".csv" if uploaded.name.lower().endswith(".pdf") else uploaded.name

                _store_csv(conn, csv_filename, csv_bytes, account_id, account_name or account_id, cash_balance)
                conn.commit()

                _do_import(conn, csv_text, account_id, account_name)

            except Exception as e:
                st.error(f"Import failed: {e}")


def main():
    st.set_page_config(page_title="Portfolio", page_icon=":material/account_balance:", layout="wide")
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

    # Check if any accounts exist
    account_ids = _get_account_ids(conn)
    if not account_ids:
        st.info("Upload a Vanguard CSV to get started.")
        return

    portfolio_id = _get_portfolio_id(conn)
    if portfolio_id is None:
        st.info("No portfolio found. Import transactions first.")
        return

    # Gather data
    stored = _get_stored_csv(conn)
    cash_balance = stored["cash_balance"] if stored else 0.0

    open_lots = _get_all_open_lots(conn, account_ids)
    symbols = _symbols_from_lots(open_lots)
    current_prices = _get_current_prices(conn, symbols)

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
            current_prices = _get_current_prices(conn, symbols)

            # Rebuild snapshots if stale or missing
            latest_snap = conn.execute(
                "SELECT MAX(snap_date) as max_date FROM portfolio_snapshots WHERE portfolio_id = ?",
                (portfolio_id,),
            ).fetchone()
            if not latest_snap or not latest_snap["max_date"] or latest_snap["max_date"] < today.isoformat():
                snap_start = earliest
                if latest_snap and latest_snap["max_date"]:
                    snap_start = date.fromisoformat(latest_snap["max_date"]) + timedelta(days=1)
                with st.spinner("Updating snapshots..."):
                    build_daily_snapshots(conn, portfolio_id, snap_start, today)

    snapshots = queries.get_snapshots(conn, portfolio_id)

    # Gather all transactions + disposals for metrics
    all_txs = []
    for acct_id in account_ids:
        all_txs.extend(queries.get_transactions(conn, account_id=acct_id))
    disposals = queries.get_disposals(conn)

    # Compute total market value from positions + cash
    portfolio_value = cash_balance + sum(
        lot["shares_remaining"] * current_prices.get(lot["symbol"], 0.0)
        for lot in open_lots
        if lot["shares_remaining"] > 0
    )

    # Build full snapshot DataFrame
    if snapshots:
        snap_df = pd.DataFrame([dict(r) for r in snapshots])
        snap_df["date"] = pd.to_datetime(snap_df["snap_date"])
        snap_df = snap_df.set_index("date").sort_index()
        snap_df["twr_growth"] = 1 + snap_df["twr"]
    else:
        snap_df = pd.DataFrame()

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
    st.metric("Portfolio Value", f"${portfolio_value:,.2f}")
    period = st.pills("Period", list(PERIODS.keys()), default="All")

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

    # Compute metrics for the selected period
    if not view_df.empty and len(view_df) > 1:
        # TWR for sub-period: (1 + twr_end) / (1 + twr_start) - 1
        twr_end = float(view_df["twr"].iloc[-1])
        # Use the snapshot just before the period for the base
        if period_start is not None and not snap_df.empty:
            before = snap_df[snap_df.index < period_start]
            twr_base = float(before["twr"].iloc[-1]) if not before.empty else 0.0
        else:
            twr_base = 0.0
        total_ret = (1 + twr_end) / (1 + twr_base) - 1

        # CAGR: annualized TWR for the period
        n_days = (view_df.index[-1] - view_df.index[0]).days
        if n_days > 365:
            cagr = (1 + total_ret) ** (365.25 / n_days) - 1
        else:
            cagr = None  # annualizing < 1 year is misleading

        # XIRR for sub-period: starting value as initial investment + flows + ending value
        period_snaps = [s for s in snapshots if s["snap_date"] >= (period_start or pd.Timestamp.min).strftime("%Y-%m-%d")]
        if period_start is not None and not before.empty:
            start_value = float(before["total_value"].iloc[-1])
            start_date = before.index[-1].date()
            xirr_val = metrics.portfolio_xirr(period_snaps, portfolio_value, today, start_value, start_date)
        else:
            xirr_val = metrics.portfolio_xirr(period_snaps, portfolio_value, today)

        # Daily returns for risk metrics
        daily_returns = view_df["twr_growth"].pct_change().dropna()
        sharpe = metrics.sharpe_ratio(daily_returns) if len(daily_returns) > 1 else None
        mdd = metrics.max_drawdown(daily_returns) if len(daily_returns) > 1 else None
        vol = metrics.annual_volatility(daily_returns) if len(daily_returns) > 1 else None
    elif not view_df.empty and len(view_df) == 1:
        total_ret = cagr = xirr_val = sharpe = mdd = vol = None
    else:
        total_ret = xirr_val = sharpe = mdd = vol = cagr = None

    # --- Metrics ---
    m1, m2, m3, m4, m5, m6 = st.columns([2, 2.5, 3, 3, 1.5, 1.5])
    m1.metric("Total return (TWR)", f"{total_ret:.1%}" if total_ret is not None else "—")
    m2.metric("Return, per year (CAGR)", f"{cagr:.1%}" if cagr is not None else "—")
    m3.metric("Money return, per year (XIRR)", f"{xirr_val:.1%}" if xirr_val is not None else "—")
    m4.metric("Risk-to-reward (Sharpe ratio)", f"{sharpe:.2f}" if sharpe is not None else "—")
    m5.metric("Max drawdown", f"{mdd:.1%}" if mdd is not None else "—")
    m6.metric("Volatility", f"{vol:.1%}" if vol is not None else "—")

    # --- Charts ---
    if not view_df.empty:
        dollar_fmt = "$,.0f"
        span_days = (view_df.index[-1] - view_df.index[0]).days
        x_fmt = "%b %Y" if span_days > 180 else "%b %d"
        x_axis = alt.Axis(format=x_fmt)

        st.subheader("Investment Returns")
        twr_base = float(view_df["twr"].iloc[0])
        ret_src = view_df[["twr"]].copy()
        ret_src["Return"] = (1 + ret_src["twr"]) / (1 + twr_base) - 1
        ret_src = ret_src.reset_index()

        # Build benchmark return series (80/20 equity/cash)
        bm_start = view_df.index[0].date()
        bm_end = view_df.index[-1].date()
        bm_syms = list({s for d in BENCHMARK_DEFS.values() for s in (d["equity"], d["bond"])})
        for sym in bm_syms:
            fetch_historical_prices(conn, sym, bm_start, bm_end)

        ret_long = ret_src.rename(columns={"Return": "value"})[["date", "value"]].copy()
        ret_long["Series"] = "Portfolio"

        for bm_name, bm_info in BENCHMARK_DEFS.items():
            eq_rows = queries.get_daily_prices(conn, bm_info["equity"], bm_start, bm_end)
            bond_rows = queries.get_daily_prices(conn, bm_info["bond"], bm_start, bm_end)
            if not eq_rows or not bond_rows:
                continue
            eq_s = pd.Series(
                {pd.Timestamp(r["price_date"]): r["close"] for r in eq_rows}
            ).sort_index()
            bond_s = pd.Series(
                {pd.Timestamp(r["price_date"]): r["close"] for r in bond_rows}
            ).sort_index()
            common = eq_s.index.intersection(bond_s.index)
            if len(common) < 2:
                continue
            eq_ret = eq_s[common].pct_change().fillna(0)
            bond_ret = bond_s[common].pct_change().fillna(0)
            blended = 0.8 * eq_ret + 0.2 * bond_ret
            cum_ret = (1 + blended).cumprod() - 1
            bm_df = pd.DataFrame({
                "date": common,
                "value": cum_ret.values,
                "Series": bm_name,
            })
            ret_long = pd.concat([ret_long, bm_df], ignore_index=True)

        _RET_COLORS = {
            "Portfolio": "#4a90d9",
            "80% S&P 500 / 20% Cash": "#a05a5a",
            "80% ACWI / 20% Cash": "#5a8a5a",
        }
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

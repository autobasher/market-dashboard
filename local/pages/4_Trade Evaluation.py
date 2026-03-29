"""Trade Evaluation page — "What if I'd held?" analysis."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from datetime import date

import altair as alt
import pandas as pd
import streamlit as st

from market_dashboard.database.connection import get_app_connection
from market_dashboard.portfolio import queries
from market_dashboard.portfolio.prices import ensure_prices_for_portfolio
from market_dashboard.portfolio.what_if import build_whatif_series

st.set_page_config(page_title="Trade Evaluation", layout="wide")
st.header("Trade Evaluation")

conn = get_app_connection()

# --- Portfolio selector (same pattern as Performance page) ---
all_portfolios = queries.get_all_portfolios(conn)
individual_portfolios = [p for p in all_portfolios if not p["is_aggregate"]]
if not individual_portfolios:
    st.info("Upload a Vanguard CSV on the Performance page to get started.")
    st.stop()

sel_cols = st.columns(len(individual_portfolios) + 1)
checked = {}
for i, p in enumerate(individual_portfolios):
    checked[p["name"]] = sel_cols[i].checkbox(
        p["name"], key=f"te_check_{p['portfolio_id']}"
    )
build_clicked = sel_cols[-1].button("Build", key="te_build")

selected_names = sorted(name for name, on in checked.items() if on)
if not selected_names:
    st.info("Select one or more portfolios and click Build.")
    st.stop()

if build_clicked:
    st.session_state["te_built_portfolios"] = selected_names
built = st.session_state.get("te_built_portfolios")

if not built or sorted(built) != selected_names:
    st.info("Click **Build** to load selected portfolios.")
    st.stop()

selected = [p for p in individual_portfolios if p["name"] in built]
if not selected:
    st.stop()

# For now, use first selected portfolio (single-portfolio analysis)
portfolio = selected[0]
portfolio_id = portfolio["portfolio_id"]

# --- Date pickers ---
col1, col2, col3 = st.columns([1, 1, 2])
start_date = col1.date_input("Start date", value=date(2026, 2, 28), key="te_start")
end_date = col2.date_input("End date", value=date.today(), key="te_end")

if start_date >= end_date:
    st.warning("Start date must be before end date.")
    st.stop()

evaluate_clicked = col3.button("Evaluate", key="te_evaluate", type="primary")

if not evaluate_clicked and "te_results" not in st.session_state:
    st.info("Set dates and click **Evaluate**.")
    st.stop()

if evaluate_clicked:
    with st.spinner("Fetching prices and building what-if series..."):
        # Ensure prices are up to date for held symbols
        account_ids = queries.get_effective_account_ids(conn, portfolio_id)
        all_txs = []
        for acct_id in account_ids:
            all_txs.extend(queries.get_transactions(conn, account_id=acct_id))
        symbols = list({tx["symbol"] for tx in all_txs if tx["symbol"]})
        ensure_prices_for_portfolio(conn, symbols, start_date, end_date)

        # Build what-if (hold) series
        whatif_df = build_whatif_series(conn, portfolio_id, start_date, end_date)

        # Load actual snapshots for the period
        snap_rows = queries.get_snapshots(conn, portfolio_id, start_date, end_date)
        snap_df = pd.DataFrame([dict(r) for r in snap_rows])

    if whatif_df.empty:
        st.warning("No positions found as of the start date.")
        st.stop()

    if snap_df.empty:
        st.warning("No snapshots found for this period. Rebuild snapshots on the Performance page first.")
        st.stop()

    # Compute actual period return from TWR column
    snap_df["snap_date"] = pd.to_datetime(snap_df["snap_date"])
    twr_start = snap_df["twr"].iloc[0]
    snap_df["actual_return"] = (1 + snap_df["twr"]) / (1 + twr_start) - 1

    st.session_state["te_results"] = {
        "whatif_df": whatif_df,
        "snap_df": snap_df,
        "start_date": start_date,
        "end_date": end_date,
        "portfolio_name": portfolio["name"],
    }

# --- Display results ---
results = st.session_state.get("te_results")
if not results:
    st.stop()

whatif_df = results["whatif_df"]
snap_df = results["snap_df"]

# Metrics
actual_return = snap_df["actual_return"].iloc[-1]
hold_return = whatif_df["hold_return"].iloc[-1]
alpha = actual_return - hold_return

m1, m2, m3 = st.columns(3)
m1.metric("Actual TWR", f"{actual_return:+.2%}")
m2.metric("Hold Return", f"{hold_return:+.2%}")
m3.metric("Alpha (Actual − Hold)", f"{alpha:+.2%}", delta=f"{alpha:+.2%}")

# --- Chart ---
# Build long-form DataFrame for Altair
actual_long = snap_df[["snap_date", "actual_return"]].rename(
    columns={"snap_date": "date", "actual_return": "value"}
)
actual_long["Series"] = "Actual"

hold_long = whatif_df[["date", "hold_return"]].copy()
hold_long["date"] = pd.to_datetime(hold_long["date"])
hold_long = hold_long.rename(columns={"hold_return": "value"})
hold_long["Series"] = "Hold"

chart_df = pd.concat([actual_long, hold_long], ignore_index=True)

colors = {"Actual": "#4a90d9", "Hold": "#e8833a"}
series_order = [s for s in colors if s in chart_df["Series"].unique()]

chart = (
    alt.Chart(chart_df)
    .mark_line()
    .encode(
        x=alt.X("date:T", title=None),
        y=alt.Y("value:Q", axis=alt.Axis(format=".0%"), title="Return"),
        color=alt.Color(
            "Series:N",
            title=None,
            scale=alt.Scale(
                domain=series_order,
                range=[colors[s] for s in series_order],
            ),
        ),
        tooltip=[
            alt.Tooltip("date:T", title="Date"),
            alt.Tooltip("Series:N", title="Series"),
            alt.Tooltip("value:Q", title="Return", format=".1%"),
        ],
    )
    .interactive()
)
st.altair_chart(chart, use_container_width=True)

# Value comparison table
st.subheader("Period Summary")
summary = pd.DataFrame({
    "": ["Start Value", "End Value", "Return"],
    "Actual": [
        f"${snap_df['total_value'].iloc[0]:,.0f}",
        f"${snap_df['total_value'].iloc[-1]:,.0f}",
        f"{actual_return:+.2%}",
    ],
    "Hold": [
        f"${whatif_df['hold_value'].iloc[0]:,.0f}",
        f"${whatif_df['hold_value'].iloc[-1]:,.0f}",
        f"{hold_return:+.2%}",
    ],
})
st.dataframe(summary, hide_index=True, use_container_width=False)

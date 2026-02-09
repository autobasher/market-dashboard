from __future__ import annotations

from datetime import date

import pandas as pd
import streamlit as st

from market_dashboard.config import Settings
from market_dashboard.database.connection import get_connection
from market_dashboard.database import queries as dashboard_queries
from market_dashboard.portfolio import queries
from market_dashboard.portfolio.schema import initialize_portfolio_schema
from market_dashboard.portfolio.fifo import rebuild_lots
from market_dashboard.portfolio.snapshots import build_daily_snapshots
from market_dashboard.portfolio.models import TxType


def _get_conn():
    settings = Settings()
    conn = get_connection(settings.db_path)
    dashboard_queries.initialize(conn)
    initialize_portfolio_schema(conn)
    return conn


_TX_TYPE_OPTIONS = [t.value for t in TxType]


def _render_transaction_table(conn):
    """Render the full transaction table with filters."""
    account_ids = [r["account_id"] for r in conn.execute("SELECT account_id FROM accounts").fetchall()]
    if not account_ids:
        st.info("No transactions. Import data on the Portfolio page first.")
        return

    all_txs = []
    for acct_id in account_ids:
        all_txs.extend(queries.get_transactions(conn, account_id=acct_id))

    if not all_txs:
        st.info("No transactions found.")
        return

    rows = [dict(r) for r in all_txs]
    df = pd.DataFrame(rows)

    # Filters
    col_sym, col_type = st.columns(2)
    symbols = sorted(df["symbol"].dropna().unique())
    types = sorted(df["tx_type"].unique())

    selected_symbols = col_sym.multiselect("Filter by Symbol", symbols)
    selected_types = col_type.multiselect("Filter by Type", types)

    if selected_symbols:
        df = df[df["symbol"].isin(selected_symbols)]
    if selected_types:
        df = df[df["tx_type"].isin(selected_types)]

    df = df.sort_values("trade_date", ascending=False).reset_index(drop=True)

    display_cols = [
        "tx_id", "trade_date", "tx_type", "symbol", "shares",
        "price_per_share", "total_amount", "fees", "raw_description",
    ]
    display_df = df[display_cols].copy()
    display_df.columns = [
        "ID", "Date", "Type", "Symbol", "Shares",
        "Price", "Amount", "Fees", "Description",
    ]

    st.dataframe(display_df, use_container_width=True, hide_index=True)
    return df


def _edit_section(conn, df):
    """Edit an existing transaction."""
    st.subheader("Edit Transaction")
    tx_ids = df["tx_id"].tolist()
    selected_id = st.selectbox("Select Transaction ID", tx_ids)

    if selected_id is None:
        return False

    tx_row = df[df["tx_id"] == selected_id].iloc[0]

    with st.form("edit_form"):
        col1, col2 = st.columns(2)
        trade_date = col1.date_input("Trade Date", value=date.fromisoformat(tx_row["trade_date"]))
        tx_type = col2.selectbox(
            "Type", _TX_TYPE_OPTIONS,
            index=_TX_TYPE_OPTIONS.index(tx_row["tx_type"]) if tx_row["tx_type"] in _TX_TYPE_OPTIONS else 0,
        )
        col3, col4 = st.columns(2)
        symbol = col3.text_input("Symbol", value=tx_row["symbol"] or "")
        shares = col4.number_input("Shares", value=float(tx_row["shares"] or 0), format="%.4f")
        col5, col6 = st.columns(2)
        price = col5.number_input("Price per Share", value=float(tx_row["price_per_share"] or 0), format="%.4f")
        amount = col6.number_input("Amount", value=float(tx_row["total_amount"] or 0), format="%.2f")
        col7, col8 = st.columns(2)
        fees = col7.number_input("Fees", value=float(tx_row["fees"] or 0), format="%.2f")
        description = col8.text_input("Description", value=tx_row["raw_description"] or "")

        if st.form_submit_button("Save Changes"):
            queries.update_transaction(
                conn, selected_id,
                trade_date=trade_date.isoformat(),
                tx_type=tx_type,
                symbol=symbol or None,
                shares=shares or None,
                price_per_share=price or None,
                total_amount=amount,
                fees=fees,
                raw_description=description,
            )
            conn.commit()
            st.success(f"Transaction {selected_id} updated.")
            return True
    return False


def _add_section(conn):
    """Add a new transaction manually."""
    st.subheader("Add Transaction")
    account_ids = [r["account_id"] for r in conn.execute("SELECT account_id FROM accounts").fetchall()]
    if not account_ids:
        st.warning("No accounts exist. Import data first.")
        return False

    with st.form("add_form"):
        col1, col2, col3 = st.columns(3)
        account_id = col1.selectbox("Account", account_ids)
        trade_date = col2.date_input("Trade Date", value=date.today())
        tx_type = col3.selectbox("Type", _TX_TYPE_OPTIONS)

        col4, col5 = st.columns(2)
        symbol = col4.text_input("Symbol")
        shares = col5.number_input("Shares", value=0.0, format="%.4f")

        col6, col7 = st.columns(2)
        price = col6.number_input("Price per Share", value=0.0, format="%.4f")
        amount = col7.number_input("Amount", value=0.0, format="%.2f")

        col8, col9 = st.columns(2)
        fees = col8.number_input("Fees", value=0.0, format="%.2f")
        description = col9.text_input("Description")

        if st.form_submit_button("Add Transaction"):
            tx_id = queries.insert_transaction(
                conn,
                account_id=account_id,
                trade_date=trade_date,
                tx_type=tx_type,
                total_amount=amount,
                symbol=symbol or None,
                shares=shares or None,
                price_per_share=price or None,
                fees=fees,
                raw_description=description,
            )
            conn.commit()
            st.success(f"Added transaction {tx_id}.")
            return True
    return False


def _delete_section(conn, df):
    """Delete a transaction with confirmation."""
    st.subheader("Delete Transaction")
    tx_ids = df["tx_id"].tolist()
    del_id = st.selectbox("Select Transaction ID to Delete", tx_ids, key="del_select")

    if del_id is not None:
        tx_row = df[df["tx_id"] == del_id].iloc[0]
        st.caption(
            f"{tx_row['trade_date']} | {tx_row['tx_type']} | "
            f"{tx_row['symbol'] or '—'} | {tx_row['shares'] or '—'} shares | "
            f"${tx_row['total_amount']:.2f}"
        )
        if st.button("Delete", type="primary"):
            queries.delete_transaction(conn, del_id)
            conn.commit()
            st.success(f"Transaction {del_id} deleted.")
            return True
    return False


def _rebuild_section(conn):
    """Rebuild lots and snapshots after edits."""
    account_ids = [r["account_id"] for r in conn.execute("SELECT account_id FROM accounts").fetchall()]
    portfolio_row = conn.execute("SELECT portfolio_id FROM portfolios LIMIT 1").fetchone()
    if not account_ids or not portfolio_row:
        return

    if st.button("Rebuild Portfolio", type="primary"):
        with st.spinner("Rebuilding lots..."):
            for acct_id in account_ids:
                rebuild_lots(conn, acct_id)

        portfolio_id = portfolio_row["portfolio_id"]
        with st.spinner("Rebuilding snapshots..."):
            build_daily_snapshots(conn, portfolio_id)

        conn.commit()
        st.success("Portfolio rebuilt successfully.")
        st.rerun()


def main():
    st.set_page_config(page_title="Transactions", page_icon=":material/receipt_long:", layout="wide")
    st.title("Transactions")

    conn = _get_conn()

    df = _render_transaction_table(conn)
    if df is None or df.empty:
        return

    st.divider()

    changed = False
    tab_edit, tab_add, tab_delete = st.tabs(["Edit", "Add", "Delete"])

    with tab_edit:
        if _edit_section(conn, df):
            changed = True

    with tab_add:
        if _add_section(conn):
            changed = True

    with tab_delete:
        if _delete_section(conn, df):
            changed = True

    st.divider()

    if changed:
        st.warning("Transactions were modified. Rebuild the portfolio to update lots and snapshots.")

    _rebuild_section(conn)

from __future__ import annotations

import streamlit as st

from market_dashboard.database.connection import get_app_connection
from market_dashboard.portfolio import queries
from market_dashboard.portfolio.page import _rebuild_aggregate_snapshots


def _list_portfolios(conn):
    """Display a table of all portfolios."""
    all_portfolios = queries.get_all_portfolios(conn)
    if not all_portfolios:
        st.info("No portfolios yet. Import data on the Portfolio page.")
        return

    rows = []
    for p in all_portfolios:
        ptype = "Aggregate" if p["is_aggregate"] else "Individual"
        if p["is_aggregate"]:
            members = queries.get_aggregate_members(conn, p["portfolio_id"])
            detail = ", ".join(m["name"] for m in members)
        else:
            accts = queries.get_portfolio_accounts(conn, p["portfolio_id"])
            detail = ", ".join(a["account_id"] for a in accts)
        rows.append({
            "Name": p["name"],
            "Type": ptype,
            "Details": detail,
            "Created": p["created_at"],
        })
    st.dataframe(rows, width="stretch", hide_index=True)


def _create_aggregate_section(conn):
    """UI to create an aggregate portfolio."""
    st.subheader("Create Aggregate Portfolio")
    all_portfolios = queries.get_all_portfolios(conn)
    individual = [p for p in all_portfolios if not p["is_aggregate"]]

    if len(individual) < 2:
        st.caption("Need at least 2 individual portfolios to create an aggregate.")
        return

    with st.form("create_aggregate"):
        name = st.text_input("Aggregate name", placeholder="e.g. Combined")
        options = {p["name"]: p["portfolio_id"] for p in individual}
        selected = st.multiselect("Member portfolios", list(options.keys()))

        if st.form_submit_button("Create"):
            if not name:
                st.error("Name is required.")
                return
            if len(selected) < 2:
                st.error("Select at least 2 member portfolios.")
                return
            if queries.get_portfolio_by_name(conn, name):
                st.error(f"Portfolio '{name}' already exists.")
                return

            agg_id = queries.insert_portfolio(conn, name, is_aggregate=True)
            for member_name in selected:
                member_id = options[member_name]
                queries.add_aggregate_member(conn, agg_id, member_id)
                # Link member's accounts to the aggregate
                for acct in queries.get_portfolio_accounts(conn, member_id):
                    queries.add_account_to_portfolio(conn, agg_id, acct["account_id"])

            with st.spinner("Building snapshots..."):
                _rebuild_aggregate_snapshots(conn, agg_id)
            conn.commit()
            st.success(f"Created aggregate portfolio '{name}'.")
            st.rerun()


def _delete_section(conn):
    """UI to delete a portfolio."""
    st.subheader("Delete Portfolio")
    all_portfolios = queries.get_all_portfolios(conn)
    if not all_portfolios:
        return

    options = {p["name"]: p for p in all_portfolios}
    selected_name = st.selectbox("Select portfolio to delete", list(options.keys()))
    if not selected_name:
        return

    p = options[selected_name]
    if p["is_aggregate"]:
        st.caption("Deleting an aggregate only removes the grouping. Member portfolios are unaffected.")
    else:
        st.caption("Deleting an individual portfolio also removes its account data (transactions, lots, snapshots).")

    if st.button("Delete", type="primary"):
        pid = p["portfolio_id"]
        if not p["is_aggregate"]:
            # Wipe account data
            accts = queries.get_portfolio_accounts(conn, pid)
            for acct in accts:
                aid = acct["account_id"]
                conn.execute(
                    "DELETE FROM lot_disposals WHERE lot_id IN "
                    "(SELECT lot_id FROM lots WHERE account_id = ?)", (aid,)
                )
                conn.execute("DELETE FROM lots WHERE account_id = ?", (aid,))
                conn.execute("DELETE FROM transactions WHERE account_id = ?", (aid,))
                conn.execute("DELETE FROM uploaded_csv WHERE account_id = ?", (aid,))
                conn.execute("DELETE FROM accounts WHERE account_id = ?", (aid,))

        queries.delete_portfolio(conn, pid)

        # Rebuild any aggregates that referenced this portfolio
        # (delete_portfolio already removed aggregate_members rows)

        conn.commit()
        st.success(f"Deleted portfolio '{selected_name}'.")
        st.rerun()


def main():
    st.set_page_config(page_title="Portfolios", page_icon=":material/folder:", layout="wide")
    st.title("Portfolios")

    conn = get_app_connection()

    _list_portfolios(conn)
    st.divider()
    _create_aggregate_section(conn)
    st.divider()
    _delete_section(conn)

from __future__ import annotations

import io
import logging
from collections import defaultdict
from dataclasses import replace
from datetime import date, datetime, timedelta
from pathlib import Path
import pandas as pd

from market_dashboard.config import ISIN_MAP
from market_dashboard.portfolio.models import Transaction, TxType

logger = logging.getLogger(__name__)

# Map Vanguard's transaction type strings to our TxType enum
_VANGUARD_TX_MAP = {
    "Buy": TxType.BUY,
    "Sell": TxType.SELL,
    "Dividend": TxType.DIVIDEND,
    "Reinvestment": TxType.DRIP,
    "Capital gain (LT)": TxType.DIVIDEND,
    "Capital gain (ST)": TxType.DIVIDEND,
    "Fee": TxType.FEE,
    "Transfer (incoming)": TxType.TRANSFER_IN,
    "Transfer (outgoing)": TxType.TRANSFER_OUT,
    "Transfer (Outgoing)": TxType.TRANSFER_OUT,
    "Conversion (incoming)": TxType.TRANSFER_IN,
    "Conversion (outgoing)": TxType.TRANSFER_OUT,
    "Corp Action (Redemption)": TxType.SELL,
    "Corp Action (Spinoff)": TxType.TRANSFER_IN,
    "Merger (incoming)": TxType.TRANSFER_IN,
    "Merger (outgoing)": TxType.TRANSFER_OUT,
    "Stock split": TxType.SPLIT,
    "Sweep in": TxType.SWEEP_IN,
    "Sweep out": TxType.SWEEP_OUT,
    "Rollover (incoming)": TxType.TRANSFER_IN,
    "Rollover (outgoing)": TxType.TRANSFER_OUT,
    "Rollover (Incoming)": TxType.TRANSFER_IN,
    "Rollover (Outgoing)": TxType.TRANSFER_OUT,
    "Distribution": TxType.DIVIDEND,
    "Interest": TxType.DIVIDEND,
    "Funds Received": TxType.TRANSFER_IN,
    "Withdrawal": TxType.TRANSFER_OUT,
}


def _parse_date(val: str) -> date:
    return datetime.strptime(val.strip(), "%m/%d/%Y").date()


def _col(row, *names: str) -> str:
    """Read the first matching column name from a row, return stripped string."""
    for name in names:
        val = row.get(name)
        if val is not None:
            s = str(val).strip()
            if s and s != "nan":
                return s
    return ""


def _money(val: str) -> float:
    """Parse a dollar string like '$1,234.56' or '-$500.00' into a float."""
    val = val.replace("$", "").replace(",", "")
    return float(val) if val else 0.0


def parse_vanguard_csv(
    file_path: Path | str | io.StringIO,
    account_id: str,
) -> tuple[list[Transaction], set[str]]:
    """Parse a Vanguard brokerage CSV into Transaction objects.

    Handles both the official Vanguard export format and the simplified format
    from the PDF parser.

    Returns (transactions, skipped_types) where skipped_types contains any
    unrecognized transaction type strings that were encountered and skipped.
    """
    df = pd.read_csv(file_path, dtype=str)
    df.columns = df.columns.str.strip()

    transactions: list[Transaction] = []
    skipped_types: set[str] = set()
    source = str(file_path) if not isinstance(file_path, io.StringIO) else "<stream>"

    for _, row in df.iterrows():
        raw_type = _col(row, "Transaction Type")

        trade_date = _parse_date(_col(row, "Trade Date"))
        settle_str = _col(row, "Settlement Date")
        settlement_date = _parse_date(settle_str) if settle_str else None

        symbol = _col(row, "Symbol") or None

        shares_str = _col(row, "Shares")
        shares = float(shares_str) if shares_str else None

        price_str = _col(row, "Share Price")
        price_per_share = _money(price_str) if price_str else None

        amount_str = _col(row, "Net Amount")
        total_amount = _money(amount_str) if amount_str else 0.0

        # Fees: "Commissions and Fees" (single column) or "Commission"+"Fees" (two columns)
        combined_fees_str = _col(row, "Commissions and Fees")
        if combined_fees_str:
            fees = _money(combined_fees_str)
        else:
            commission = _money(_col(row, "Commission"))
            fees_val = _money(_col(row, "Fees"))
            fees = commission + fees_val

        raw_description = _col(row, "Transaction Description", "Investment Name")
        if not raw_description:
            raw_description = raw_type

        # Handle Exchange / Corp Action (Exchange) by share sign
        if raw_type in ("Exchange", "Corp Action (Exchange)"):
            if shares is not None and shares < 0:
                tx_type = TxType.SELL
                shares = abs(shares)
            else:
                tx_type = TxType.TRANSFER_IN
        else:
            tx_type = _VANGUARD_TX_MAP.get(raw_type)
            if tx_type is None:
                if raw_type:
                    skipped_types.add(raw_type)
                    logger.warning("Skipping unrecognized Vanguard tx type: %s", raw_type)
                continue

        # Negative-amount distribution/interest with no symbol = cash leaving
        # (e.g. taxable distributions paid out, not reinvested)
        if tx_type == TxType.DIVIDEND and total_amount < 0 and not symbol:
            tx_type = TxType.TRANSFER_OUT
            total_amount = abs(total_amount)

        transactions.append(Transaction(
            tx_id=None,
            account_id=account_id,
            trade_date=trade_date,
            settlement_date=settlement_date,
            tx_type=tx_type,
            symbol=symbol,
            shares=abs(shares) if shares is not None else None,
            price_per_share=price_per_share,
            total_amount=total_amount,
            fees=fees,
            split_ratio=None,
            raw_description=raw_description,
            source_file=source,
        ))

    transactions = redate_settlement_sweeps(transactions)
    return transactions, skipped_types


def redate_settlement_sweeps(transactions: list[Transaction]) -> list[Transaction]:
    """Re-date BUY/SELL settlement sweeps back to the trade date.

    Vanguard records BUY/SELL on trade date but the offsetting VMFXX
    SWEEP_OUT/SWEEP_IN settles T+1 (sometimes T+2). Walking snapshots by
    trade date causes 1-day spikes: positions go up while VMFXX hasn't
    been debited yet. Re-dating settlement sweeps to the matching trade
    date makes positions and cash move together.

    Match logic: per (account, date), compute net cash = SELL - BUY total.
    For each SWEEP_OUT (cash leaving VMFXX) of $X, find a prior date within
    5 days where net cash = -X (buys exceeded sells by X). For each SWEEP_IN
    (by abs amount), find a prior date where net cash = +X. Closest prior
    date wins. Unmatched sweeps (recurring distributions, real deposits) are
    left alone. Idempotent: re-running re-matches to the same dates.
    """
    net_cash: dict[tuple[str, date], float] = defaultdict(float)
    for tx in transactions:
        if tx.tx_type == TxType.BUY:
            net_cash[(tx.account_id, tx.trade_date)] -= abs(tx.total_amount or 0.0)
        elif tx.tx_type == TxType.SELL:
            net_cash[(tx.account_id, tx.trade_date)] += abs(tx.total_amount or 0.0)

    out: list[Transaction] = []
    for tx in transactions:
        new_date: date | None = None
        if tx.tx_type in (TxType.SWEEP_OUT, TxType.SWEEP_IN):
            amt = abs(tx.total_amount or 0.0)
            if amt > 0:
                target = -amt if tx.tx_type == TxType.SWEEP_OUT else amt
                for offset in range(1, 6):
                    cand = tx.trade_date - timedelta(days=offset)
                    if abs(net_cash.get((tx.account_id, cand), 0.0) - target) < 0.01:
                        new_date = cand
                        break
        if new_date is not None and new_date != tx.trade_date:
            out.append(replace(tx, trade_date=new_date))
        else:
            out.append(tx)
    return out


_AIL_TX_MAP = {"Buy": TxType.BUY, "Sell": TxType.SELL}


def parse_ail_xlsx(
    file_path: Path | str | io.BytesIO,
    account_id: str,
) -> list[Transaction]:
    """Parse an AIL transactions xlsx into Transaction objects.

    Resolves ISINs to tickers via ISIN_MAP. Computes total_amount and fees (1%).
    """
    df = pd.read_excel(file_path, sheet_name="transactions", engine="openpyxl")
    source = str(file_path) if not isinstance(file_path, io.BytesIO) else "<xlsx>"

    # Deduplicate: same trade appearing in multiple source files (e.g. xlsx + PDF)
    # has identical (ISIN, Value Date, Quantity, Amount) but different trade dates.
    # Keep the first occurrence (earliest trade date).
    df = df.sort_values("Date")
    dedup_cols = ["ISIN", "Value Date", "Quantity", "Amount"]
    before = len(df)
    df = df.drop_duplicates(subset=dedup_cols, keep="first")
    dropped = before - len(df)
    if dropped:
        logger.info("AIL dedup: dropped %d duplicate rows (same ISIN/ValueDate/Qty/Amt)", dropped)

    transactions: list[Transaction] = []
    unmapped: set[str] = set()

    for _, row in df.iterrows():
        isin = str(row.get("ISIN", "")).strip()
        if not isin:
            continue

        ts = ISIN_MAP.get(isin)
        if ts is None:
            unmapped.add(isin)
            continue
        symbol = ts.ticker

        trade_date = pd.to_datetime(row["Date"]).date()

        tx_type_str = str(row.get("Type", "")).strip()
        tx_type = _AIL_TX_MAP.get(tx_type_str)
        if tx_type is None:
            logger.warning("Unknown tx type '%s' for ISIN %s — skipping", tx_type_str, isin)
            continue

        quantity = float(row["Quantity"])
        price = float(row["Price"])
        total_amount = quantity * price
        fees = abs(total_amount) * 0.01

        transactions.append(Transaction(
            tx_id=None,
            account_id=account_id,
            trade_date=trade_date,
            settlement_date=None,
            tx_type=tx_type,
            symbol=symbol,
            shares=abs(quantity),
            price_per_share=price,
            total_amount=total_amount,
            fees=fees,
            split_ratio=None,
            raw_description=f"{tx_type_str} {isin}",
            source_file=source,
        ))

    if unmapped:
        logger.warning("Unmapped ISINs skipped: %s", ", ".join(sorted(unmapped)))

    return transactions

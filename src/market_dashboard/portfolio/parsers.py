from __future__ import annotations

import io
import logging
from datetime import date, datetime
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
) -> list[Transaction]:
    """Parse a Vanguard brokerage CSV into Transaction objects.

    Handles both the official Vanguard export format and the simplified format
    from the PDF parser.
    """
    df = pd.read_csv(file_path, dtype=str)
    df.columns = df.columns.str.strip()

    transactions: list[Transaction] = []
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
                continue  # skip unrecognized types

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

    return transactions


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

"""Parse Vanguard 'Custom Activity Report' PDF into Vanguard-style CSV rows."""
from __future__ import annotations

import re
from pathlib import Path

import pdfplumber

# Date at start of a transaction line
_LINE_RE = re.compile(r"^(\d{1,2}/\d{1,2}/\d{4})\s+(\d{1,2}/\d{1,2}/\d{4})\s+")

# Base transaction types (without parenthetical suffixes)
_BASE_TX_TYPES = [
    "Capital gain (LT)",
    "Capital gain (ST)",
    "Funds Received",
    "Stock split",
    "Partnership",
    "Reinvestment",
    "Distribution",
    "Sweep in",
    "Sweep out",
    "Withdrawal",
    "Dividend",
    "Interest",
    "Journal",
    "Sell",
    "Buy",
    "Fee",
]

# Types that can have a parenthetical suffix on a continuation line
_SUFFIXED_BASES = {"Transfer", "Corp Action", "Conversion", "Merger"}

# Money pattern: optional minus, $, digits with commas, decimal
_MONEY_RE = re.compile(r"-?\$[\d,]+\.\d{2,4}")
# Quantity: optional minus, digits with commas, dot, 4+ digits
_QTY_RE = re.compile(r"-?[\d,]+\.\d{4}")
# Parenthetical suffix like (incoming), (outgoing), (Redemption), (Exchange)
_PAREN_RE = re.compile(r"\([A-Za-z][A-Za-z\s]*\)")

# Account types
_ACCT_TYPES = {"CASH", "MARGIN"}

# Unicode dashes used as empty-field placeholders
_DASHES = {"\u2013", "\u2014", "\u2015", "\u2212"}


def _extract_raw_rows(pdf_path: Path | str) -> list[dict]:
    """Extract raw transaction rows from PDF, joining continuation lines."""
    pdf = pdfplumber.open(str(pdf_path))
    rows: list[dict] = []
    current: dict | None = None

    for page in pdf.pages:
        text = page.extract_text()
        if not text:
            continue
        for line in text.split("\n"):
            line = line.strip()
            if not line:
                continue
            if (
                line.startswith("Custom report")
                or line.startswith("This report")
                or line.startswith("Settlement")
                or line.startswith("date")
                or re.match(r"^Page \d+", line)
            ):
                continue
            if re.search(r"\d{8}\*", line):
                continue

            m = _LINE_RE.match(line)
            if m:
                if current:
                    rows.append(current)
                current = {
                    "settle_date": m.group(1),
                    "trade_date": m.group(2),
                    "rest": line[m.end() :].strip(),
                    "cont": [],
                }
            elif current:
                current["cont"].append(line)

    if current:
        rows.append(current)
    pdf.close()
    return rows


def _detect_no_symbol(rest: str) -> bool:
    """Check if the line starts with a dash placeholder (meaning no ticker symbol)."""
    if not rest:
        return False
    return rest[0] in _DASHES or rest.startswith("--")


def _find_tx_type(full: str, cont_lines: list[str]) -> tuple[str, str, int]:
    """Find the transaction type in the text.

    Returns (display_type, base_word, base_position).
    base_word is the portion actually present on the main line (e.g. "Transfer").
    display_type includes the suffix (e.g. "Transfer (incoming)").
    base_position is the index of base_word in full.
    """
    best_display = ""
    best_base = ""
    best_pos = len(full)

    for tt in _BASE_TX_TYPES:
        idx = full.find(tt)
        if idx != -1 and idx < best_pos:
            best_display = tt
            best_base = tt
            best_pos = idx

    for base in _SUFFIXED_BASES:
        idx = full.find(base)
        if idx == -1 or idx >= best_pos:
            continue
        # Look for parenthetical suffix anywhere after the base in the full text
        after = full[idx + len(base) :]
        suffix_match = _PAREN_RE.search(after)
        if suffix_match:
            best_display = base + " " + suffix_match.group(0)
        else:
            # Check continuation lines
            for cont in cont_lines:
                sm = _PAREN_RE.search(cont.strip())
                if sm:
                    best_display = base + " " + sm.group(0)
                    break
            else:
                best_display = base
        best_base = base
        best_pos = idx

    return best_display, best_base, best_pos


def _clean_dashes(text: str) -> str:
    """Replace unicode dash placeholders with spaces and normalize whitespace."""
    for d in _DASHES:
        text = text.replace(d, " ")
    return re.sub(r"\s+", " ", text).strip()


def _parse_row(raw: dict) -> dict:
    """Parse a raw row dict into structured CSV fields."""
    rest_original = raw["rest"]
    cont_lines = raw["cont"]

    no_symbol = _detect_no_symbol(rest_original)

    full = rest_original
    if cont_lines:
        full += " " + " ".join(cont_lines)

    # Find tx type on original text (before dash stripping)
    display_type, base_word, base_pos = _find_tx_type(full, cont_lines)

    # Split into before/after the base word position
    before_tx_raw = full[:base_pos] if base_word else full
    after_tx_raw = full[base_pos + len(base_word) :] if base_word else ""

    # Clean dashes
    before_tx = _clean_dashes(before_tx_raw)
    after_tx = _clean_dashes(after_tx_raw)

    # Strip parenthetical suffix from after_tx (it's part of the type, not data)
    after_tx = _PAREN_RE.sub(" ", after_tx)
    after_tx = re.sub(r"\s+", " ", after_tx).strip()

    # Extract symbol + name from before_tx
    symbol = ""
    name = before_tx
    if not no_symbol:
        parts = before_tx.split(None, 1)
        if parts:
            candidate = parts[0]
            if re.match(r"^[A-Z][A-Z0-9.]{0,5}$", candidate) and candidate not in _ACCT_TYPES:
                symbol = candidate
                name = parts[1] if len(parts) > 1 else ""

    # Strip account type from after_tx
    after_tx_stripped = after_tx
    for at in _ACCT_TYPES:
        if after_tx_stripped.startswith(at):
            after_tx_stripped = after_tx_stripped[len(at) :].strip()
            break

    # Also strip trailing continuation text (name fragments, CUSIP info, dates)
    # by extracting only the numeric/money fields
    money_vals = _MONEY_RE.findall(after_tx_stripped)

    temp = after_tx_stripped
    for mv in money_vals:
        temp = temp.replace(mv, "", 1)
    qty_match = _QTY_RE.search(temp)
    quantity = qty_match.group(0) if qty_match else ""

    has_free = "Free" in after_tx_stripped

    amount = money_vals[-1] if money_vals else ""
    price = ""
    fees = ""

    if has_free:
        fees = "$0.00"
        if len(money_vals) >= 2:
            price = money_vals[0]
    else:
        if len(money_vals) >= 3:
            price = money_vals[0]
            fees = money_vals[1]
        elif len(money_vals) == 2:
            if quantity:
                price = money_vals[0]
            else:
                fees = money_vals[0]

    shares = quantity.replace(",", "") if quantity else ""
    share_price = price.replace("$", "").replace(",", "") if price else ""
    fees_clean = fees.replace("$", "").replace(",", "") if fees else ""
    net_amount = amount.replace("$", "").replace(",", "") if amount else ""

    return {
        "Trade Date": raw["trade_date"],
        "Settlement Date": raw["settle_date"],
        "Transaction Type": display_type,
        "Transaction Description": name.strip(),
        "Symbol": symbol,
        "Shares": shares,
        "Share Price": share_price,
        "Commission": "",
        "Fees": fees_clean,
        "Net Amount": net_amount,
    }


def pdf_to_csv_rows(pdf_path: Path | str) -> list[dict]:
    """Parse a Vanguard activity report PDF into Vanguard-CSV-shaped dicts."""
    raw_rows = _extract_raw_rows(pdf_path)
    return [_parse_row(r) for r in raw_rows]



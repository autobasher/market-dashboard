"""Tests for pdf_parser.py internal helpers.

Tests the in-memory parsing logic (type detection, row parsing, dash handling)
without requiring actual PDF files.
"""
from market_dashboard.portfolio.pdf_parser import (
    _clean_dashes,
    _detect_no_symbol,
    _find_tx_type,
    _parse_row,
)


# --- _detect_no_symbol ---

def test_detect_no_symbol_unicode_dash():
    assert _detect_no_symbol("\u2013 some text") is True
    assert _detect_no_symbol("\u2014 some text") is True


def test_detect_no_symbol_ascii_dash():
    assert _detect_no_symbol("-- some text") is True


def test_detect_no_symbol_with_ticker():
    assert _detect_no_symbol("VTI Vanguard Total Stock") is False


def test_detect_no_symbol_empty():
    assert _detect_no_symbol("") is False


# --- _clean_dashes ---

def test_clean_dashes_unicode():
    assert _clean_dashes("foo \u2013 bar") == "foo bar"
    assert _clean_dashes("\u2014\u2014") == ""


def test_clean_dashes_normalizes_whitespace():
    assert _clean_dashes("foo   bar  baz") == "foo bar baz"


# --- _find_tx_type ---

def test_find_tx_type_simple():
    display, base, pos = _find_tx_type("VTI Vanguard Buy CASH 10.0000 $220.50", [])
    assert display == "Buy"
    assert base == "Buy"


def test_find_tx_type_sell():
    display, base, pos = _find_tx_type("VTI Vanguard Sell CASH", [])
    assert display == "Sell"
    assert base == "Sell"


def test_find_tx_type_dividend():
    display, base, pos = _find_tx_type("VTI Vanguard Dividend CASH $15.50", [])
    assert display == "Dividend"


def test_find_tx_type_transfer_with_suffix():
    display, base, pos = _find_tx_type("VTI Vanguard Transfer (incoming) CASH", [])
    assert display == "Transfer (incoming)"
    assert base == "Transfer"


def test_find_tx_type_transfer_suffix_on_continuation():
    display, base, pos = _find_tx_type("VTI Vanguard Transfer CASH", ["(outgoing)"])
    assert display == "Transfer (outgoing)"
    assert base == "Transfer"


def test_find_tx_type_corp_action():
    display, base, pos = _find_tx_type("OUST Corp Action (Exchange) CASH", [])
    assert display == "Corp Action (Exchange)"
    assert base == "Corp Action"


def test_find_tx_type_capital_gain():
    display, base, pos = _find_tx_type("VTI Capital gain (LT) CASH $50.00", [])
    assert display == "Capital gain (LT)"


def test_find_tx_type_sweep():
    display, base, pos = _find_tx_type("\u2013 Sweep in CASH $500.00", [])
    assert display == "Sweep in"


def test_find_tx_type_no_match():
    display, base, pos = _find_tx_type("Some random text with no type", [])
    assert display == ""
    assert base == ""


# --- _parse_row ---

def _make_raw(rest, cont=None, trade_date="01/15/2024", settle_date="01/17/2024"):
    return {
        "trade_date": trade_date,
        "settle_date": settle_date,
        "rest": rest,
        "cont": cont or [],
    }


def test_parse_row_buy():
    raw = _make_raw("VTI VANGUARD TOTAL STOCK Buy CASH 10.0000 $220.50 $0.00 -$2,205.00")
    result = _parse_row(raw)
    assert result["Symbol"] == "VTI"
    assert result["Transaction Type"] == "Buy"
    assert result["Shares"] == "10.0000"
    assert result["Share Price"] == "220.50"
    assert result["Net Amount"] == "-2205.00"


def test_parse_row_dividend_no_symbol():
    raw = _make_raw("\u2013 Dividend CASH $15.50")
    result = _parse_row(raw)
    assert result["Symbol"] == ""
    assert result["Transaction Type"] == "Dividend"
    assert result["Net Amount"] == "15.50"


def test_parse_row_transfer_incoming():
    raw = _make_raw("VTI VANGUARD TOTAL STOCK Transfer (incoming) CASH 5.0000 $230.00 $0.00 $0.00")
    result = _parse_row(raw)
    assert result["Transaction Type"] == "Transfer (incoming)"
    assert result["Symbol"] == "VTI"


def test_parse_row_dates():
    raw = _make_raw("VTI Buy CASH $100.00", trade_date="03/15/2024", settle_date="03/17/2024")
    result = _parse_row(raw)
    assert result["Trade Date"] == "03/15/2024"
    assert result["Settlement Date"] == "03/17/2024"


def test_parse_row_fee_no_shares():
    raw = _make_raw("\u2013 Fee CASH $0.00 -$25.00")
    result = _parse_row(raw)
    assert result["Transaction Type"] == "Fee"
    assert result["Symbol"] == ""
    assert result["Shares"] == ""
    assert result["Net Amount"] == "-25.00"


def test_parse_row_with_commas_in_amount():
    raw = _make_raw("VTI VANGUARD Buy CASH 100.0000 $220.50 $0.00 -$22,050.00")
    result = _parse_row(raw)
    assert result["Net Amount"] == "-22050.00"


def test_parse_row_free_trade():
    raw = _make_raw("VTI VANGUARD Buy CASH 10.0000 $220.50 Free -$2,205.00")
    result = _parse_row(raw)
    assert result["Fees"] == "0.00"

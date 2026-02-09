import io
from datetime import date

from market_dashboard.portfolio.models import TxType
from market_dashboard.portfolio.parsers import parse_vanguard_csv

SAMPLE_CSV = """\
Trade Date,Settlement Date,Transaction Type,Transaction Description,Symbol,Shares,Share Price,Commission,Fees,Net Amount
01/15/2024,01/17/2024,Buy,Buy,VTI,10.0,$220.50,$0.00,$0.00,-$2205.00
02/01/2024,02/03/2024,Sell,Sell,VTI,-5.0,$225.00,$0.00,$0.00,$1125.00
03/01/2024,,Dividend,Dividend,VTI,,,$0.00,$0.00,$15.50
04/01/2024,04/03/2024,Reinvestment,Reinvestment,VTI,0.07,$221.43,$0.00,$0.00,-$15.50
05/01/2024,,Sweep in,Sweep in,,,,$0.00,$0.00,$500.00
06/01/2024,06/03/2024,Exchange,Exchange,VEA,-10.0,$48.00,$0.00,$0.00,$480.00
06/01/2024,06/03/2024,Exchange,Exchange,VWO,20.0,$42.00,$0.00,$0.00,-$840.00
07/01/2024,,Fee,Advisory Fee,,,,$0.00,$0.00,-$25.00
08/01/2024,08/03/2024,Transfer (incoming),Transfer,VTI,5.0,$230.00,$0.00,$0.00,$0.00
"""


def test_parse_vanguard_basic():
    txs = parse_vanguard_csv(io.StringIO(SAMPLE_CSV), "acct-1")
    # Sweep row should be skipped, Exchange produces 2 rows -> 8 total
    assert len(txs) == 8


def test_parse_vanguard_tx_types():
    txs = parse_vanguard_csv(io.StringIO(SAMPLE_CSV), "acct-1")
    types = [t.tx_type for t in txs]
    assert types == [
        TxType.BUY,
        TxType.SELL,
        TxType.DIVIDEND,
        TxType.DRIP,
        TxType.SELL,       # Exchange sell VEA
        TxType.BUY,        # Exchange buy VWO
        TxType.FEE,
        TxType.TRANSFER_IN,
    ]


def test_parse_vanguard_dates():
    txs = parse_vanguard_csv(io.StringIO(SAMPLE_CSV), "acct-1")
    assert txs[0].trade_date == date(2024, 1, 15)
    assert txs[0].settlement_date == date(2024, 1, 17)
    # Dividend has no settlement date
    assert txs[2].settlement_date is None


def test_parse_vanguard_amounts():
    txs = parse_vanguard_csv(io.StringIO(SAMPLE_CSV), "acct-1")
    buy = txs[0]
    assert buy.total_amount == -2205.00
    assert buy.shares == 10.0
    assert buy.price_per_share == 220.50
    assert buy.fees == 0.0

    sell = txs[1]
    assert sell.total_amount == 1125.00
    assert sell.shares == 5.0


def test_parse_vanguard_exchange_splitting():
    txs = parse_vanguard_csv(io.StringIO(SAMPLE_CSV), "acct-1")
    exchange_sell = txs[4]
    exchange_buy = txs[5]

    assert exchange_sell.tx_type == TxType.SELL
    assert exchange_sell.symbol == "VEA"
    assert exchange_sell.shares == 10.0

    assert exchange_buy.tx_type == TxType.BUY
    assert exchange_buy.symbol == "VWO"
    assert exchange_buy.shares == 20.0


def test_parse_vanguard_fee():
    txs = parse_vanguard_csv(io.StringIO(SAMPLE_CSV), "acct-1")
    fee = txs[6]
    assert fee.tx_type == TxType.FEE
    assert fee.symbol is None
    assert fee.total_amount == -25.00


def test_parse_vanguard_account_id():
    txs = parse_vanguard_csv(io.StringIO(SAMPLE_CSV), "acct-1")
    assert all(t.account_id == "acct-1" for t in txs)


def test_parse_vanguard_dedup_on_reimport():
    """Parsing the same file twice should produce identical transactions."""
    txs1 = parse_vanguard_csv(io.StringIO(SAMPLE_CSV), "acct-1")
    txs2 = parse_vanguard_csv(io.StringIO(SAMPLE_CSV), "acct-1")
    assert len(txs1) == len(txs2)
    for a, b in zip(txs1, txs2):
        assert a.trade_date == b.trade_date
        assert a.tx_type == b.tx_type
        assert a.total_amount == b.total_amount


def test_parse_vanguard_drip():
    txs = parse_vanguard_csv(io.StringIO(SAMPLE_CSV), "acct-1")
    drip = txs[3]
    assert drip.tx_type == TxType.DRIP
    assert drip.shares == 0.07
    assert drip.price_per_share == 221.43

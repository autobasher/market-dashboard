from __future__ import annotations

import enum
from dataclasses import dataclass
from datetime import date


class TxType(enum.Enum):
    BUY = "BUY"
    SELL = "SELL"
    DIVIDEND = "DIVIDEND"
    SPLIT = "SPLIT"
    TRANSFER_IN = "TRANSFER_IN"
    TRANSFER_OUT = "TRANSFER_OUT"
    FEE = "FEE"
    DRIP = "DRIP"
    SWEEP_IN = "SWEEP_IN"
    SWEEP_OUT = "SWEEP_OUT"


@dataclass(frozen=True)
class Transaction:
    tx_id: int | None
    account_id: str
    trade_date: date
    settlement_date: date | None
    tx_type: TxType
    symbol: str | None
    shares: float | None
    price_per_share: float | None
    total_amount: float
    fees: float
    split_ratio: float | None
    raw_description: str
    source_file: str


@dataclass(frozen=True)
class Lot:
    lot_id: int | None
    account_id: str
    symbol: str
    acquired_date: date
    shares_acquired: float
    shares_remaining: float
    cost_basis_per_share: float
    total_cost_basis: float
    source_tx_id: int | None


@dataclass(frozen=True)
class LotDisposal:
    disposal_id: int | None
    sell_tx_id: int
    lot_id: int
    shares_disposed: float
    cost_basis: float
    proceeds: float
    realized_gain: float

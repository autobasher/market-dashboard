from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
_DEFAULT_DB_PATH = _PROJECT_ROOT / "data" / "market_dashboard.db"


@dataclass(frozen=True)
class DashboardLine:
    label: str
    section: str  # "equity" | "alternatives" | "fixed_income" | "themes"
    tickers: tuple[str, ...]
    weights: tuple[float, ...]


# -- Left column: Equity -------------------------------------------------------

EQUITY_LINES: tuple[DashboardLine, ...] = (
    DashboardLine("Blended",  "equity", ("VT",), (1.0,)),
    DashboardLine(
        "Value", "equity",
        ("DFLV", "DFIV", "AVUV", "AVES", "AVDV", "QVAL", "IVAL"),
        (0.35, 0.16, 0.16, 0.11, 0.11, 0.06, 0.05),
    ),
    DashboardLine("Momentum", "equity", ("QMOM", "IMOM", "GMOM"), (0.4, 0.4, 0.2)),
)

# -- Left column: Alternatives -------------------------------------------------

ALTERNATIVES_LINES: tuple[DashboardLine, ...] = (
    DashboardLine(
        "Trend", "alternatives",
        ("MFUT", "DBMF", "KMLM", "AHLT", "ISMF"),
        (0.2, 0.2, 0.2, 0.2, 0.2),
    ),
    DashboardLine("Real Estate", "alternatives", ("DFGR",), (1.0,)),
    DashboardLine("Uranium",     "alternatives", ("U-UN.TO",), (1.0,)),
    DashboardLine("Gold",        "alternatives", ("GLD",), (1.0,)),
    DashboardLine("Bitcoin",     "alternatives", ("IBIT",), (1.0,)),
)

# -- Right column: Fixed Income -------------------------------------------------

FIXED_INCOME_LINES: tuple[DashboardLine, ...] = (
    DashboardLine("Agg",    "fixed_income", ("BNDW",), (1.0,)),
    DashboardLine("Linkers", "fixed_income", ("SCHP", "WIP"), (0.5, 0.5)),
)

# -- Right column: Themes -------------------------------------------------------

THEMES_LINES: tuple[DashboardLine, ...] = (
    DashboardLine("Ouster Inc",      "themes", ("OUST",), (1.0,)),
    DashboardLine("Rivian Auto",     "themes", ("RIVN",), (1.0,)),
    DashboardLine("Japan Value",     "themes", ("EWJV",), (1.0,)),
    DashboardLine("Freedom EM",      "themes", ("FRDM",), (1.0,)),
    DashboardLine("Oil Services",    "themes", ("OIH",), (1.0,)),
    DashboardLine(
        "Uranium Equities", "themes",
        ("URNM", "URA", "NUKZ", "NLR"),
        (0.25, 0.25, 0.25, 0.25),
    ),
)

# -- All lines and symbols ------------------------------------------------------

DASHBOARD_LINES: tuple[DashboardLine, ...] = (
    *EQUITY_LINES,
    *ALTERNATIVES_LINES,
    *FIXED_INCOME_LINES,
    *THEMES_LINES,
)

ALL_SYMBOLS: tuple[str, ...] = tuple(sorted({
    sym for line in DASHBOARD_LINES for sym in line.tickers
}))

# Section → column mapping
LEFT_SECTIONS = ("equity", "alternatives")
RIGHT_SECTIONS = ("fixed_income", "themes")

SECTION_LABELS = {
    "equity": "Equity",
    "alternatives": "Alternatives",
    "fixed_income": "Fixed Income",
    "themes": "Themes",
}


@dataclass(frozen=True)
class Settings:
    db_path: Path = field(default_factory=lambda: Path(os.environ.get(
        "MARKET_DB_PATH", str(_DEFAULT_DB_PATH)
    )))
    dashboard_lines: tuple[DashboardLine, ...] = DASHBOARD_LINES
    all_symbols: tuple[str, ...] = ALL_SYMBOLS

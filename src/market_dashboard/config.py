from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import NamedTuple

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


# -- Portfolio display constants -----------------------------------------------

DISPLAY_GROUPS: dict[str, dict] = {
    "US Multifactor": {"symbols": ["AVLV", "AVUV", "DFAT", "QVAL", "DFUV"], "class": "Equity"},
    "Non-US Developed Multifactor": {"symbols": ["DFIV", "AVDV", "DISV", "EWJV", "IVAL", "AVIV", "IMOM"], "class": "Equity"},
    "Emerging Markets Multifactor": {"symbols": ["DFEV", "AVES", "GVAL", "GMOM", "FRDM"], "class": "Equity"},
    "Commodity Equities": {"symbols": ["OIH", "URNM", "URA"], "class": "Equity"},
    "Municipal Bonds": {"symbols": ["VTEB", "MUNY"], "class": "Fixed Income"},
    "Trend Following": {"symbols": ["DBMF", "QMHNX", "TFPN", "AQMNX", "RSBT", "AHLT"], "class": "Alternatives"},
    "Global Macro": {"symbols": ["HFGM"], "class": "Alternatives"},
    "US Treasuries": {"symbols": ["VGIT", "BOXA"], "class": "Fixed Income"},
    "US TIPS": {"symbols": ["SCHP"], "class": "Fixed Income"},
    "Non-US Bonds": {"symbols": ["DFGX"], "class": "Fixed Income"},
    "Cash": {"symbols": ["VYFXX", "VMMXX", "VMFXX", "BOXX", "VTIP"], "class": "Cash"},
}

SYMBOL_LABELS: dict[str, str] = {
    "OUST": "Ouster Inc.",
    "RIVN": "Rivian Automotive",
    "QSPNX": "Long-Short Factors",
    "DFGR": "Global Real Estate",
}

GROUPED_SYMS: set[str] = {sym for info in DISPLAY_GROUPS.values() for sym in info["symbols"]}

CLASS_ORDER: list[str] = ["Equity", "Fixed Income", "Alternatives", "Cash"]

CLASS_BASE_COLORS: dict[str, tuple[int, int, int]] = {
    "Equity": (30, 100, 220),
    "Fixed Income": (210, 50, 50),
    "Alternatives": (210, 180, 30),
    "Cash": (40, 170, 70),
}

# -- ISIN → ticker mapping (AIL portfolio) -----------------------------------

class TickerSource(NamedTuple):
    ticker: str
    source: str  # "yahoo" or "eodhd"
    name: str = ""


ISIN_MAP: dict[str, TickerSource] = {
    # Yahoo Finance — stocks (2)
    "US68989M1036": TickerSource("OUST", "yahoo", "Ouster Inc"),
    "US76954A1034": TickerSource("RIVN", "yahoo", "Rivian Automotive Inc"),
    # Yahoo Finance — trusts & ETFs (8)
    "CA85210A1049": TickerSource("U-UN.TO", "yahoo", "Sprott Physical Uranium Trust"),
    "IE00B1FZSC47": TickerSource("IDTP.L", "yahoo", "iShares USD TIPS UCITS ETF (Acc)"),
    "IE00BZ0G8977": TickerSource("TIPS.L", "yahoo", "SPDR Bloomberg US TIPS UCITS ETF (Dist)"),
    "IE00BZ163L38": TickerSource("VDET.L", "yahoo", "Vanguard USD EM Government Bond UCITS ETF (Dist)"),
    "IE00BJRCLL96": TickerSource("JPGL.L", "yahoo", "JPMorgan Global Equity Multi-Factor UCITS ETF (Acc)"),
    "IE00BMGNVD65": TickerSource("AGUG.AS", "yahoo", "iShares Core Global Aggregate Bond UCITS ETF USD Hedged (Dist)"),
    "IE0003R87OG3": TickerSource("AVGS.L", "yahoo", "Avantis Global Small Cap Value UCITS ETF (Acc)"),
    "IE00B3B8PX14": TickerSource("IGIL.L", "yahoo", "iShares Global Inflation Linked Govt Bond UCITS ETF (Acc)"),
    # EODHD — Dimensional mutual funds (4)
    "IE00B0HCGS80": TickerSource("IE00B0HCGS80.EUFUND", "eodhd", "Dimensional Emerging Markets Value Fund"),
    "IE00B2PC0609": TickerSource("IE00B2PC0609.EUFUND", "eodhd", "Dimensional Global Targeted Value Fund"),
    "IE00B3V7VL84": TickerSource("IE00B3V7VL84.EUFUND", "eodhd", "Dimensional World Equity Fund"),
    "IE00BG85LS38": TickerSource("IE00BG85LS38.EUFUND", "eodhd", "Dimensional Global Core Fixed Income Fund"),
    # EODHD — AQR mutual funds (2)
    "LU1103257975": TickerSource("LU1103257975.EUFUND", "eodhd", "AQR Managed Futures UCITS Fund"),
    "LU1662505954": TickerSource("LU1662505954.EUFUND", "eodhd", "AQR Style Premia UCITS Fund"),
}

EODHD_TICKERS: frozenset[str] = frozenset(
    ts.ticker for ts in ISIN_MAP.values() if ts.source == "eodhd"
)

# Money market funds: always valued at $1/share NAV. yfinance returns
# bogus "close" prices ($26-$68) for these, corrupting valuations.
MONEY_MARKET_TICKERS: frozenset[str] = frozenset(
    {"VMFXX", "VMMXX", "VMRXX", "VYFXX"}
)


def _load_env_file() -> dict[str, str]:
    """Read key=value pairs from .env at project root."""
    env_path = _PROJECT_ROOT / ".env"
    if not env_path.exists():
        return {}
    result = {}
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        result[key.strip()] = value.strip()
    return result


def get_eodhd_api_key() -> str | None:
    """Return EODHD API key from environment or .env file."""
    key = os.environ.get("EODHD_API_KEY")
    if key:
        return key
    return _load_env_file().get("EODHD_API_KEY")


@dataclass(frozen=True)
class Settings:
    db_path: Path = field(default_factory=lambda: Path(os.environ.get(
        "MARKET_DB_PATH", str(_DEFAULT_DB_PATH)
    )))
    dashboard_lines: tuple[DashboardLine, ...] = DASHBOARD_LINES
    all_symbols: tuple[str, ...] = ALL_SYMBOLS

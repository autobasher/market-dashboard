from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone, date

import streamlit as st

from market_dashboard.config import (
    DASHBOARD_LINES,
    DashboardLine,
    SECTION_LABELS,
    Settings,
)
from market_dashboard.database.connection import get_connection
from market_dashboard.database import queries
from market_dashboard.poller import QuotePoller
from market_dashboard.dashboard.history_fetcher import HistoricalPriceFetcher
from market_dashboard.portfolio.schema import initialize_portfolio_schema

logger = logging.getLogger(__name__)

_settings = Settings()


def _get_conn():
    conn = get_connection(_settings.db_path)
    queries.initialize(conn)
    initialize_portfolio_schema(conn)
    return conn


@st.cache_resource
def _start_poller():
    poller = QuotePoller(_settings, interval=60)
    poller.start()
    return poller


@st.cache_resource
def _start_history_fetcher():
    fetcher = HistoricalPriceFetcher(_settings)
    fetcher.start()
    return fetcher


# ---------------------------------------------------------------------------
# Formatting helpers (from factor-dashboard)
# ---------------------------------------------------------------------------

def _cell_bg(val: float | None, clamp: float = 5.0) -> str:
    if val is None:
        return "background:rgba(255,255,255,.05);color:rgba(255,255,255,.3)"
    clamped = max(-clamp, min(clamp, val))
    alpha = 0.25 + (abs(clamped) / clamp) * 0.55
    if val > 0:
        return f"background:rgba(34,120,60,{alpha:.2f});color:#e0e0e0"
    if val < 0:
        return f"background:rgba(180,55,55,{alpha:.2f});color:#e0e0e0"
    return "background:rgba(255,255,255,.05);color:rgba(255,255,255,.5)"


def _fmt_pct(v: float | None) -> str:
    if v is None:
        return "\u2014"
    sign = "+" if v >= 0 else ""
    return f"{sign}{v:.2f}%"


def _composition_tip(line: DashboardLine) -> str:
    if len(line.tickers) == 1:
        return line.tickers[0]
    return ", ".join(f"{int(w*100)}% {t}" for t, w in zip(line.tickers, line.weights))


# ---------------------------------------------------------------------------
# Weighted change computation
# ---------------------------------------------------------------------------

def _compute_weighted_change(
    line: DashboardLine,
    quote_map: dict[str, dict],
) -> tuple[float | None, str | None]:
    """Return (weighted_pct, freshest_market_time) for a dashboard line."""
    valid: list[tuple[float, float, datetime]] = []
    for ticker, weight in zip(line.tickers, line.weights):
        q = quote_map.get(ticker)
        if not q or q["change_pct"] is None or q["market_time"] is None:
            continue
        try:
            ts = datetime.fromisoformat(q["market_time"])
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
        except (ValueError, TypeError):
            continue
        valid.append((q["change_pct"], weight, ts))

    if not valid:
        return None, None

    # Staleness filter: drop tickers >24h older than freshest
    if len(valid) > 1:
        freshest = max(v[2] for v in valid)
        valid = [v for v in valid if freshest - v[2] <= timedelta(hours=24)]

    if not valid:
        return None, None

    # Re-normalize weights after any dropped tickers
    total_weight = sum(v[1] for v in valid)
    weighted_pct = sum(v[0] * v[1] for v in valid) / total_weight if total_weight else None
    freshest_time = max(v[2] for v in valid).isoformat()
    return weighted_pct, freshest_time


def _compute_weighted_period_change(
    line: DashboardLine,
    current_prices: dict[str, float],
    ref_closes: dict[str, float],
) -> float | None:
    """Weighted (curr/ref - 1)*100 across tickers with available data."""
    valid: list[tuple[float, float]] = []
    for ticker, weight in zip(line.tickers, line.weights):
        curr = current_prices.get(ticker)
        ref = ref_closes.get(ticker)
        if curr is None or ref is None or ref == 0:
            continue
        pct = (curr / ref - 1) * 100
        valid.append((pct, weight))

    if not valid:
        return None

    total_weight = sum(v[1] for v in valid)
    if total_weight == 0:
        return None
    return sum(v[0] * v[1] for v in valid) / total_weight


# ---------------------------------------------------------------------------
# HTML rendering
# ---------------------------------------------------------------------------

_MOBILE_ORDER = ("equity", "fixed_income", "alternatives", "themes")


_PERIOD_CLAMPS = {"pct": 5.0, "pct_1w": 8.0, "pct_1m": 15.0, "pct_3m": 30.0}
_PERIOD_TIPS = {"pct": "Day", "pct_1w": "Week", "pct_1m": "Month", "pct_3m": "3 Months"}
_PERIOD_KEYS = ("pct", "pct_1w", "pct_1m", "pct_3m")


def _render_section(sec_key: str, title: str, rows: list[dict]) -> str:
    html = (
        f'<div class="md-sec" data-sec="{sec_key}">'
        f'<div class="md-banner">{title}</div>'
        f'<table class="md"><tbody>'
    )
    for r in rows:
        html += f'<tr><td class="md-label" title="{r["tip"]}">{r["label"]}</td>'
        for key in _PERIOD_KEYS:
            val = r.get(key)
            clamp = _PERIOD_CLAMPS[key]
            tip = _PERIOD_TIPS[key]
            html += (
                f'<td class="md-pct" style="{_cell_bg(val, clamp)}" '
                f'title="{tip}">{_fmt_pct(val)}</td>'
            )
        html += '</tr>'
    html += "</tbody></table></div>"
    return html


def _render_grid(section_rows: dict[str, list[dict]]) -> str:
    """Build a responsive grid: 2 columns on desktop, stacked on mobile."""
    inner = ""
    for sec in _MOBILE_ORDER:
        inner += _render_section(sec, SECTION_LABELS[sec], section_rows.get(sec, []))
    return f'<div class="md-grid">{inner}</div>'


_CSS = """
<style>
.md-grid {
    display: grid;
    grid-template-columns: 1fr 1fr;
    grid-auto-flow: dense;
    gap: 0 3rem;
    justify-content: center;
    max-width: 1100px;
    margin: 0 auto;
    padding-top: 1.5rem;
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", system-ui, sans-serif;
    color: #d4d4d4;
}
/* Desktop order: left col (equity 1, alternatives 3), right col (fixed_income 2, themes 4) */
.md-sec[data-sec="equity"]       { order: 1; }
.md-sec[data-sec="fixed_income"] { order: 2; }
.md-sec[data-sec="alternatives"] { order: 3; }
.md-sec[data-sec="themes"]       { order: 4; }
.md-sec {
    margin-bottom: 1.5rem;
}
.md-banner {
    background: rgba(70, 100, 140, 0.55);
    text-align: center;
    font-weight: 700;
    font-size: .85rem;
    padding: 6px 0;
    margin-bottom: 2px;
}
.md {
    width: 100%;
    border-collapse: collapse;
    font-variant-numeric: tabular-nums;
    font-size: .85rem;
}
.md tbody td {
    padding: 6px 6px;
    white-space: nowrap;
}
.md tbody td.md-label {
    font-weight: 400;
    padding-right: 1rem;
    cursor: help;
}
.md tbody td.md-pct {
    text-align: right;
    font-weight: 600;
    min-width: 62px;
}
@media (max-width: 600px) {
    .md-grid {
        grid-template-columns: 1fr;
        gap: 0;
        padding-top: 1rem;
    }
    .md-sec { margin-bottom: 1rem; }
    .md-banner { font-size: .95rem; }
    .md { font-size: .85rem; }
    .md tbody td { padding: 8px 6px; }
}
</style>
"""


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    _start_poller()
    _start_history_fetcher()

    st.set_page_config(
        page_title="Live Market",
        page_icon=":material/monitoring:",
        layout="wide",
    )

    st.markdown(
        "<style>"
        "#MainMenu {visibility:hidden} footer {visibility:hidden} "
        ".block-container {padding-top:.5rem;padding-bottom:0} "
        ".stApp {background:#1e1e1e}"
        "</style>",
        unsafe_allow_html=True,
    )

    st.markdown(_CSS, unsafe_allow_html=True)

    @st.fragment(run_every=60)
    def _live_display():
        conn = _get_conn()
        rows = queries.get_all_quotes(conn)

        quote_map: dict[str, dict] = {}
        current_prices: dict[str, float] = {}
        for r in rows:
            quote_map[r["symbol"]] = {
                "change_pct": r["change_pct"],
                "market_time": r["market_time"],
            }
            if r["price"] is not None:
                current_prices[r["symbol"]] = r["price"]

        # Reference dates for period columns
        today = date.today()
        all_syms = list({s for line in DASHBOARD_LINES for s in line.tickers})
        ref_1w = queries.get_reference_closes(conn, all_syms, (today - timedelta(days=7)).isoformat())
        ref_1m = queries.get_reference_closes(conn, all_syms, (today - timedelta(days=30)).isoformat())
        ref_3m = queries.get_reference_closes(conn, all_syms, (today - timedelta(days=91)).isoformat())

        # Build section → list of rendered rows
        section_rows: dict[str, list[dict]] = {}
        for line in DASHBOARD_LINES:
            pct, _ = _compute_weighted_change(line, quote_map)
            pct_1w = _compute_weighted_period_change(line, current_prices, ref_1w)
            pct_1m = _compute_weighted_period_change(line, current_prices, ref_1m)
            pct_3m = _compute_weighted_period_change(line, current_prices, ref_3m)
            section_rows.setdefault(line.section, []).append({
                "label": line.label,
                "pct": pct,
                "pct_1w": pct_1w,
                "pct_1m": pct_1m,
                "pct_3m": pct_3m,
                "tip": _composition_tip(line),
            })

        st.markdown(_render_grid(section_rows), unsafe_allow_html=True)

    _live_display()


if __name__ == "__main__":
    main()

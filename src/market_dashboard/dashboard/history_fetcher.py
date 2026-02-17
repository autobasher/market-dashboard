from __future__ import annotations

import logging
import threading
from datetime import datetime, timedelta, timezone

import yfinance as yf

from market_dashboard.config import Settings
from market_dashboard.database.connection import get_connection
from market_dashboard.portfolio.schema import initialize_portfolio_schema

logger = logging.getLogger(__name__)

HISTORY_DAYS = 100
FETCH_INTERVAL = 14400  # 4 hours


class HistoricalPriceFetcher:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._thread: threading.Thread | None = None
        self._stop_event = threading.Event()

    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        logger.info("HistoricalPriceFetcher started (interval=%ds)", FETCH_INTERVAL)

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=10)
        logger.info("HistoricalPriceFetcher stopped")

    def _run(self) -> None:
        while not self._stop_event.is_set():
            try:
                self._fetch()
            except Exception:
                logger.exception("HistoricalPriceFetcher cycle failed")
            self._stop_event.wait(timeout=FETCH_INTERVAL)

    def _fetch(self) -> None:
        symbols = list(self._settings.all_symbols)
        logger.info("Fetching %d-day history for %d symbols", HISTORY_DAYS, len(symbols))

        end = datetime.now(tz=timezone.utc).date()
        start = end - timedelta(days=HISTORY_DAYS)

        try:
            df = yf.download(
                symbols,
                start=start.isoformat(),
                end=end.isoformat(),
                progress=False,
                threads=True,
            )
        except Exception:
            logger.exception("yf.download failed")
            return

        if df.empty:
            logger.info("No historical data returned")
            return

        conn = get_connection(self._settings.db_path)
        try:
            initialize_portfolio_schema(conn)
            count = 0

            if len(symbols) == 1:
                # Single symbol: columns are flat (Close, Volume, etc.)
                sym = symbols[0]
                for date_idx, row in df.iterrows():
                    close = row.get("Close")
                    adj_close = row.get("Adj Close", close)
                    volume = row.get("Volume")
                    if close is None or _isnan(close):
                        continue
                    price_date = str(date_idx.date())
                    conn.execute(
                        "INSERT OR REPLACE INTO historical_prices "
                        "(symbol, price_date, close, adj_close, volume) "
                        "VALUES (?, ?, ?, ?, ?)",
                        (sym, price_date, float(close),
                         float(adj_close) if adj_close is not None and not _isnan(adj_close) else float(close),
                         int(volume) if volume is not None and not _isnan(volume) else None),
                    )
                    count += 1
            else:
                # Multi-symbol: MultiIndex columns (metric, symbol)
                for sym in symbols:
                    try:
                        close_col = df[("Close", sym)]
                    except KeyError:
                        logger.debug("No Close data for %s", sym)
                        continue
                    try:
                        adj_col = df[("Adj Close", sym)]
                    except KeyError:
                        adj_col = close_col
                    try:
                        vol_col = df[("Volume", sym)]
                    except KeyError:
                        vol_col = None

                    for date_idx in close_col.index:
                        close = close_col[date_idx]
                        if close is None or _isnan(close):
                            continue
                        adj = adj_col[date_idx] if adj_col is not None else close
                        vol = vol_col[date_idx] if vol_col is not None else None
                        price_date = str(date_idx.date())
                        conn.execute(
                            "INSERT OR REPLACE INTO historical_prices "
                            "(symbol, price_date, close, adj_close, volume) "
                            "VALUES (?, ?, ?, ?, ?)",
                            (sym, price_date, float(close),
                             float(adj) if adj is not None and not _isnan(adj) else float(close),
                             int(vol) if vol is not None and not _isnan(vol) else None),
                        )
                        count += 1

            conn.commit()
            logger.info("Upserted %d historical price rows", count)
        finally:
            conn.close()


def _isnan(v) -> bool:
    try:
        return v != v  # NaN != NaN
    except (TypeError, ValueError):
        return False

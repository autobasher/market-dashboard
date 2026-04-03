from __future__ import annotations

import logging
import threading
from datetime import datetime, timezone

import yfinance as yf

from market_dashboard.config import Settings
from market_dashboard.database.connection import get_connection
from market_dashboard.database import queries

logger = logging.getLogger(__name__)


class QuotePoller:
    def __init__(self, settings: Settings, interval: int = 60) -> None:
        self._settings = settings
        self._interval = interval
        self._thread: threading.Thread | None = None
        self._stop_event = threading.Event()

    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()
        logger.info("QuotePoller started (interval=%ds)", self._interval)

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=10)
        logger.info("QuotePoller stopped")

    def _run(self) -> None:
        while not self._stop_event.is_set():
            try:
                self._poll()
            except Exception:
                logger.exception("QuotePoller poll cycle failed")
            self._stop_event.wait(timeout=self._interval)

    def _poll(self) -> None:
        symbols = list(self._settings.all_symbols)
        logger.info("Polling %d symbols", len(symbols))

        fetched = []
        try:
            tickers_obj = yf.Tickers(" ".join(symbols))
            for sym in symbols:
                try:
                    fi = tickers_obj.tickers[sym].fast_info
                    price = fi.get("lastPrice") or fi.get("last_price")
                    if price is None or price <= 0:
                        logger.debug("No price for %s", sym)
                        continue
                    prev = fi.get("previousClose") or fi.get("previous_close")
                    change_pct = ((price - prev) / prev * 100) if prev else None
                    mt_str = datetime.now(tz=timezone.utc).isoformat()
                    fetched.append((sym, float(price), change_pct, mt_str))
                except Exception:
                    logger.debug("Failed to fetch %s", sym, exc_info=True)
        except Exception:
            logger.exception("Batch ticker fetch failed")

        if not fetched:
            logger.info("No quotes retrieved")
            return

        conn = get_connection(self._settings.db_path)
        try:
            queries.initialize(conn)
            for sym, price, change_pct, mt_str in fetched:
                queries.upsert_quote(conn, sym, price, change_pct, mt_str)
            conn.commit()
            logger.info("Upserted %d quotes", len(fetched))
        finally:
            conn.close()

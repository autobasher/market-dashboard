from unittest.mock import MagicMock, patch

from market_dashboard.database import queries


def test_upsert_and_read(in_memory_db):
    queries.upsert_quote(in_memory_db, "VT", 105.5, 1.23, "2026-02-06T15:00:00+00:00")
    in_memory_db.commit()

    rows = queries.get_all_quotes(in_memory_db)
    assert len(rows) == 1
    assert rows[0]["symbol"] == "VT"
    assert rows[0]["price"] == 105.5
    assert rows[0]["change_pct"] == 1.23


def test_upsert_replaces(in_memory_db):
    queries.upsert_quote(in_memory_db, "VT", 100.0, 0.5, "2026-02-06T14:00:00+00:00")
    queries.upsert_quote(in_memory_db, "VT", 105.0, 1.5, "2026-02-06T15:00:00+00:00")
    in_memory_db.commit()

    rows = queries.get_all_quotes(in_memory_db)
    assert len(rows) == 1
    assert rows[0]["price"] == 105.0
    assert rows[0]["change_pct"] == 1.5


class _NoCloseConn:
    """Wraps a real connection but makes close() a no-op."""

    def __init__(self, real):
        self._real = real

    def close(self):
        pass

    def __getattr__(self, name):
        return getattr(self._real, name)


@patch("market_dashboard.poller.yf")
def test_poll_writes_to_db(mock_yf, in_memory_db):
    from market_dashboard.config import Settings
    from market_dashboard.poller import QuotePoller

    mock_ticker = MagicMock()
    mock_ticker.info = {
        "regularMarketPrice": 100.0,
        "regularMarketChangePercent": 1.5,
        "regularMarketTime": 1738857600,
    }
    mock_yf.Ticker.return_value = mock_ticker

    settings = Settings(db_path=":memory:")
    wrapper = _NoCloseConn(in_memory_db)

    with patch("market_dashboard.poller.get_connection", return_value=wrapper):
        poller = QuotePoller(settings, interval=60)
        poller._poll()

    rows = queries.get_all_quotes(in_memory_db)
    assert len(rows) > 0
    assert all(r["price"] == 100.0 for r in rows)

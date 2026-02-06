import math

from market_dashboard.config import ALL_SYMBOLS, DASHBOARD_LINES


def test_weights_sum_to_one():
    for line in DASHBOARD_LINES:
        assert math.isclose(sum(line.weights), 1.0, abs_tol=1e-9), (
            f"{line.label}: weights sum to {sum(line.weights)}"
        )


def test_tickers_weights_same_length():
    for line in DASHBOARD_LINES:
        assert len(line.tickers) == len(line.weights), (
            f"{line.label}: {len(line.tickers)} tickers vs {len(line.weights)} weights"
        )


def test_all_symbols_unique():
    assert len(ALL_SYMBOLS) == len(set(ALL_SYMBOLS))


def test_all_symbols_covers_all_tickers():
    all_from_lines = {sym for line in DASHBOARD_LINES for sym in line.tickers}
    assert all_from_lines == set(ALL_SYMBOLS)


def test_expected_symbol_count():
    assert len(ALL_SYMBOLS) == 32


def test_dashboard_line_count():
    assert len(DASHBOARD_LINES) == 16

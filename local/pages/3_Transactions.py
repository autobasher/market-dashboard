"""Transactions page entry point for Streamlit multipage navigation."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from market_dashboard.portfolio.tx_page import main

main()

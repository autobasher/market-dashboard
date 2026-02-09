"""Entry point for Streamlit Community Cloud deployment."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "src"))

from market_dashboard.dashboard.app import main

main()

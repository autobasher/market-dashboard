"""Entry point for local deployment with portfolio pages."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from market_dashboard.dashboard.app import main

main()

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
for _pkg in ("core", "company", "streaming", "backtest"):
    _d = _ROOT / _pkg
    if str(_d) not in sys.path:
        sys.path.insert(0, str(_d))

import pytest
from unittest.mock import MagicMock


@pytest.fixture(autouse=True)
def mock_app_state():
    # If appState tries to connect to a DB on import, we intercept it here
    if "appState" not in sys.modules:
        mock_state = MagicMock()
        # Mock the engines dictionary so app_state.engines["overview"] doesn't crash
        mock_state.engines = MagicMock()
        sys.modules["appState"] = MagicMock(app_state=mock_state)

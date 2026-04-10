import pytest
from stream_func import parse_option, is_market_closed
from unittest.mock import patch
import datetime


@pytest.mark.skip(reason="just testing the import")
def test_example():
    assert True

def test_parse_option_valid():
    result = parse_option("AAPL_251219C00150000")
    assert result["ticker"] == "AAPL"
    assert result["price"] == 150.0
    assert result["type"] == "Call"

def test_parse_option_invalid():
    assert parse_option("invalid_string") is None

"""Tests for the small parsing helpers: dollar strings, count strings,
timestamps, and turning one raw trade into our stored shape.

Kalshi sends every price and count as a string (e.g. '0.1200', '10.00'),
never a plain number, so these helpers are the one place that mistake
could sneak in and silently corrupt every downstream number.
"""
from datetime import datetime, timezone

import archiver


def test_parse_dollars_valid_and_invalid_input():
    assert archiver.parse_dollars("0.1200") == 0.12
    assert archiver.parse_dollars("1.0000") == 1.0
    assert archiver.parse_dollars(None) is None
    assert archiver.parse_dollars("") is None
    assert archiver.parse_dollars("not-a-number") is None


def test_parse_count_reuses_dollar_parsing():
    assert archiver.parse_count("10.00") == 10.0
    assert archiver.parse_count(None) is None


def test_parse_iso_ts_matches_known_epoch_and_rejects_bad_input():
    # 2026-07-19T03:52:14Z is a fixed instant; check it converts to the
    # matching Unix timestamp rather than just "some float came back."
    ts = archiver.parse_iso_ts("2026-07-19T03:52:14Z")
    expected = datetime(2026, 7, 19, 3, 52, 14, tzinfo=timezone.utc).timestamp()
    assert ts == expected
    assert archiver.parse_iso_ts(None) is None
    assert archiver.parse_iso_ts("") is None
    assert archiver.parse_iso_ts("not-a-timestamp") is None


def test_month_key_and_series_ticker_helpers():
    assert archiver.month_key("2026-07-19T03:52:14.217089Z") == "2026-07"
    assert archiver.month_key("2026-01-01T00:00:00Z") == "2026-01"
    assert archiver.series_ticker_from_market_ticker("KXHIGHLAX-26JUL19-T80") == "KXHIGHLAX"
    assert archiver.series_ticker_from_market_ticker("KXBTC15M-26JUL190000-00") == "KXBTC15M"
    # a bare series ticker with no event suffix should come back unchanged
    assert archiver.series_ticker_from_market_ticker("KXHIGHLAX") == "KXHIGHLAX"


def test_normalize_trade_produces_expected_shape():
    raw = {
        "trade_id": "trade-001",
        "ticker": "KXHIGHLAX-26JUL19-T80",
        "count_fp": "20.00",
        "yes_price_dollars": "0.1200",
        "no_price_dollars": "0.8800",
        "taker_outcome_side": "no",
        "taker_side": "no",
        "taker_book_side": "ask",
        "created_time": "2026-07-19T03:52:14.217089Z",
        "is_block_trade": False,
    }
    row = archiver.normalize_trade(raw)
    assert row["trade_id"] == "trade-001"
    assert row["ticker"] == "KXHIGHLAX-26JUL19-T80"
    assert row["count"] == 20.0
    assert row["yes_price"] == 0.12
    assert row["no_price"] == 0.88
    assert row["taker_side"] == "no"
    assert row["taker_book_side"] == "ask"
    assert row["is_block_trade"] is False
    assert row["created_ts"] is not None

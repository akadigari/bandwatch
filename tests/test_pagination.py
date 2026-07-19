"""Tests for page_trades: the cursor-walking loop that pages backward
through GET /markets/trades. No network here. Every test hands page_trades
a fake `getter` function that plays back canned pages from tests/fixtures
instead of calling requests.

These fixtures use tiny, evenly spaced timestamps (one trade per minute)
so the "did we stop at the right page" checks are exact instead of
depending on real, messy live data.
"""
import archiver


def two_page_getter(fixture):
    """Page 1 (cursor='cursor-page-2') then page 2 (cursor='', the end)."""
    page1 = fixture("trades_page_1.json")
    page2 = fixture("trades_page_2.json")

    def getter(url, params=None):
        params = params or {}
        cursor = params.get("cursor")
        if cursor is None:
            return page1
        if cursor == "cursor-page-2":
            return page2
        return {"cursor": "", "trades": []}

    return getter


def test_page_trades_reaches_end_when_cursor_empty(fixture):
    getter = two_page_getter(fixture)
    trades, reached_end, hit_page_cap = archiver.page_trades(
        max_ts=None, stop_at_ts=None, max_pages=10, getter=getter,
    )
    assert [t["trade_id"] for t in trades] == ["trade-006", "trade-005", "trade-004", "trade-003"]
    assert reached_end is True
    assert hit_page_cap is False


def test_page_trades_stops_before_fetching_second_page(fixture):
    getter = two_page_getter(fixture)
    # trade-005's timestamp: stopping here should keep all of page 1
    # (including the trade exactly at the watermark) and never touch
    # page 2 at all.
    watermark = archiver.parse_iso_ts("2026-07-19T03:59:00.000000Z")
    trades, reached_end, hit_page_cap = archiver.page_trades(
        max_ts=None, stop_at_ts=watermark, max_pages=10, getter=getter,
    )
    assert [t["trade_id"] for t in trades] == ["trade-006", "trade-005"]
    assert reached_end is False
    assert hit_page_cap is False


def test_page_trades_keeps_whole_boundary_page_when_crossing_watermark(fixture):
    getter = two_page_getter(fixture)
    # trade-004's timestamp is on page 2. Older trades share no exact
    # timestamp collision in this fixture, but the boundary page (page 2)
    # must still come back in full, not filtered down to "newer than
    # watermark only": that filtering is exactly what could drop a real
    # sibling trade that shares Kalshi's batch-matched timestamp.
    watermark = archiver.parse_iso_ts("2026-07-19T03:58:00.000000Z")
    trades, reached_end, hit_page_cap = archiver.page_trades(
        max_ts=None, stop_at_ts=watermark, max_pages=10, getter=getter,
    )
    assert [t["trade_id"] for t in trades] == ["trade-006", "trade-005", "trade-004", "trade-003"]
    assert reached_end is False
    assert hit_page_cap is False


def test_page_trades_hits_max_pages_cap():
    # A getter that always hands back a fresh cursor and one trade, so
    # the only thing that can stop the loop is the page budget.
    call_count = {"n": 0}

    def endless_getter(url, params=None):
        call_count["n"] += 1
        return {
            "cursor": "keep-going",
            "trades": [{
                "trade_id": f"trade-endless-{call_count['n']}",
                "ticker": "KXHIGHLAX-26JUL19-T80",
                "count_fp": "1.00",
                "yes_price_dollars": "0.5000",
                "no_price_dollars": "0.5000",
                "taker_outcome_side": "yes",
                "taker_book_side": "bid",
                "created_time": "2026-07-19T03:00:00.000000Z",
                "is_block_trade": False,
            }],
        }

    trades, reached_end, hit_page_cap = archiver.page_trades(
        max_ts=None, stop_at_ts=None, max_pages=3, getter=endless_getter,
    )
    assert len(trades) == 3
    assert call_count["n"] == 3
    assert reached_end is False
    assert hit_page_cap is True
